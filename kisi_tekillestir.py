"""
Kişi tekilleştirme (benzersiz kişi sayımı) — salt okunur.

Kural (kullanıcı, 02.10.2026):
  1. E-posta aynıysa  -> AYNI KİŞİ (isimde ı/i gibi harf hatası olsa bile).
  2. E-posta farklıysa isimlere bak:
       - isim farklı                 -> FARKLI kişi
       - isim aynı, telefon farklı   -> FARKLI kişi
       - isim aynı, telefon aynı     -> AYNI kişi (iki e-postası var)
       - isim aynı, telefon eksik    -> KANIT YOK: birleştirilmez, 'belirsiz' sayılır
  3. İsim sırası ve tam dizgi korunur ('Ahmet Mehmet Kaya' != 'Mehmet Ahmet Kaya');
     ad/soyad ayrı kolonlarda ise 'ad soyad' olarak birleştirilir. Tek sözcüklü
     isimler birleştirme kanıtı sayılmaz.

Kullanım:
  python kisi_tekillestir.py            # rapor basar (kişisel veri yazmaz)
  from kisi_tekillestir import benzersiz_kisi_say
"""
import os
import re
import sys
import unicodedata

import psycopg2

IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
TEL_KOLONLARI = ("telefon", "gsm", "telefon2")
R_D_AKTIF = False   # R-d: isimle ilgisiz serbest posta (tahmin); kullanıcı kararıyla kapatılabilir
MAKS_EPOSTA = 5   # aynı (isim+telefon) için en fazla bu kadar e-posta tek kişi sayılır; fazlası toplu/kurumsal kayıttır
YER_TUTUCU = {"nan", "none", "null", "n/a", "na", "bilinmiyor", "yok", "-", ""}


def norm_isim(s):
    """Türkçe harf hatalarına dayanıklı normalizasyon: İ/I/ı/i -> i, ş->s ..."""
    if not s:
        return None
    s = str(s).replace("İ", "i").replace("I", "ı").lower()
    s = s.replace("ı", "i")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if s in YER_TUTUCU or len(s) < 3:
        return None
    return s


def norm_tel(s):
    """Son 9 hane (portal tel_son9 ile aynı mantık). Sahte/boş numaralar elenir."""
    if not s:
        return None
    d = re.sub(r"[^0-9]", "", str(s))
    if len(d) < 9:
        return None
    d = d[-9:]
    if len(set(d)) <= 2:          # 000000000, 111111111, 121212121 ...
        return None
    return d


GENEL_KUTU = ("info", "sales", "satis", "muhasebe", "contact", "iletisim", "admin", "office", "ofis",
              "export", "import", "mail", "destek", "support", "hello", "bilgi", "marketing", "pazarlama",
              "satinalma", "purchase", "procurement", "hr", "genel", "finans", "accounting", "reception",
              "sekreter", "yonetim", "order", "siparis", "firma", "company")


def genel_mi(ham):
    """Genel kutu: kısa kelimeler (hr, mail…) tam eşleşme, uzunlar (info, sales…) önek (infopamtek)."""
    return any(ham == g or (len(g) >= 4 and ham.startswith(g)) for g in GENEL_KUTU)


def eposta_sinifi(mail, isim_norm):
    """E-postanın baş kısmına göre: 'ad' (isimle uyumlu), 'genel' (info@, sales@ ...), 'baska' (başka biri gibi).
    'ad': isimden bir sözcük (>=3 harf) adreste geçer VE kalan harfler isme aittir (baş harf, diğer ad sözcükleri
    ya da küçük yazım farkı). Ör. 'rkaya'/'ikose'/'zeynepserpileryilmaz' = ad; ferhat dutkun için
    'serhatdutkun' = baska (soyad var ama başında başka bir ad). Kullanıcı kuralı (02.10): aynı isim+telefonla
    kayıtlı ama baş kısmı farklı kişiye ait adresler farklı kişidir; kişinin kendi adresi + şirketin genel kutusu
    aynı kişidir."""
    ham = re.sub(r"[^a-z]", "", mail.split("@")[0].lower())
    if not ham:
        return "baska"
    sozcukler = (isim_norm or "").split()
    for t in sorted((x for x in sozcukler if len(x) >= 3), key=len, reverse=True):
        k = ham.find(t)
        if k < 0:
            continue
        kalan = ham[:k] + ham[k + len(t):]
        digerleri = [x for x in sozcukler if x != t]
        # kalan harfler isme ait mi? (baş harf, diğer sözcükler; yazım hatası ilk harfi değiştirmez)
        uyumlu = (len(kalan) <= 2 or kalan == "".join(digerleri)
                  or any((x[0] == kalan[0] and _mesafe(kalan, x, 2) <= 2) or x.startswith(kalan) or kalan in x
                         for x in sozcukler))
        # İlk ad eşleşiyorsa fazladan bir ad (orta ad) kabul edilir; SOYAD eşleşip başında/sonunda başka ad
        # varsa (serhatdutkun / ferhat dutkun, timintugay / güleser timin) o adres başka bir kişiye aittir.
        if uyumlu or t != sozcukler[-1]:
            return "ad"
    if genel_mi(ham):
        return "genel"
    return "baska"



SERBEST_ALAN = {"gmail.com", "hotmail.com", "outlook.com", "yahoo.com", "icloud.com", "yandex.com", "live.com",
                "msn.com", "hotmail.com.tr", "yahoo.com.tr", "yandex.com.tr", "mynet.com", "icloud.com.tr",
                "gmx.com", "protonmail.com", "me.com", "ymail.com"}


def _etiket(alan):
    """Alan adının şirket kısmı: 'maratonsport.com.tr' -> 'maratonsport'."""
    p = alan.split(".")
    if len(p) >= 3 and p[-2] in ("com", "net", "org", "co", "gov", "edu", "biz", "info"):
        return p[-3]
    return p[-2] if len(p) >= 2 else alan

SERBEST_ETIKET = {"gmail", "googlemail", "hotmail", "outlook", "live", "msn", "yahoo", "ymail", "icloud", "me", "mac",
                  "yandex", "mynet", "gmx", "web", "mail", "protonmail", "proton", "aol", "qq", "163", "126", "sina",
                  "inbox", "bk", "list", "rambler", "t-online", "libero", "virgilio", "free", "orange", "wanadoo",
                  "sfr", "laposte", "tiscali", "ttmail", "superonline", "turk", "e-kolay", "hotmail"}


def serbest_mi(alan):
    """Serbest posta sağlayıcısı mı? Uzantıdan bağımsız: hotmail.com / hotmail.com.tr / hotmail.fr / outlook.com.tr ..."""
    return alan in SERBEST_ALAN or _etiket(alan) in SERBEST_ETIKET


def _mesafe(a, b, sinir=2):
    """Levenshtein (erken çıkışlı); fark `sinir`ten büyükse sinir+1 döner."""
    if abs(len(a) - len(b)) > sinir:
        return sinir + 1
    onceki = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        simdiki = [i]
        for j, cb in enumerate(b, 1):
            simdiki.append(min(onceki[j] + 1, simdiki[j - 1] + 1, onceki[j - 1] + (ca != cb)))
        onceki = simdiki
    return onceki[-1]


def _alan_benzer(a1, a2):
    """Aynı alan adı ya da yazım hatası kadar yakın (maratonsport / marotonsport)."""
    if a1 == a2:
        return True
    e1, e2 = _etiket(a1), _etiket(a2)
    if e1 == e2 or (len(e1) >= 6 and len(e2) >= 6 and _mesafe(e1, e2) <= 2):
        return True
    kisa, uzun = sorted((e1, e2), key=len)
    return len(kisa) >= 5 and uzun.startswith(kisa)       # alt şirket: flokser -> flokserkimya


def _yerel(mail):
    return re.sub(r"[^a-z]", "", mail.split("@")[0].lower())


def kanit_var(m1, m2, isim):
    """İki küme arasında 'aynı kişi' kanıtı (telefon çelişkisi dışarıda denetlenir). Kural kodu veya None."""
    a1, a2 = m1.split("@")[-1], m2.split("@")[-1]
    f1, f2 = serbest_mi(a1), serbest_mi(a2)
    c1, c2 = eposta_sinifi(m1, isim), eposta_sinifi(m2, isim)
    y1, y2 = _yerel(m1), _yerel(m2)
    # R-a: aynı alan adı, adresler isimle uyumlu / genel kutu (kişinin adresi + şirketin genel kutusu)
    if (a1 == a2 or (not f1 and not f2 and _alan_benzer(a1, a2))) and {c1, c2} <= {"ad", "genel"} and not (f1 and c1 == c2):
        return "R-a"
    # R-b: aynı yerel kısım + aynı/yakın alan adı (yazım hatası) ya da biri serbest posta
    if len(y1) >= 4 and y1 == y2 and (_alan_benzer(a1, a2) or f1 or f2):
        return "R-b"
    # R-c: kurumsal adres isimle uyumlu + diğeri kişinin isimle uyumlu serbest postası (osman.yilmaz@x + osman144y@gmail)
    if (f1 != f2) and ((c1 == "ad" and c2 == "ad")):
        return "R-c"
    # R-d: kurumsal adres isimle uyumlu + diğeri isimle İLGİSİZ serbest posta (abdullah@x + asmen32@gmail) — tahmin
    if R_D_AKTIF and (f1 != f2) and (c1 == "ad" and not f1 or c2 == "ad" and not f2):
        return "R-d"
    return None


class _UF:
    def __init__(self):
        self.p = {}

    def find(self, x):
        self.p.setdefault(x, x)
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def _kolonlar(cur, tablo):
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name=%s", (tablo,))
    return {r[0] for r in cur.fetchall()}


def _isim_ifadesi(cols):
    if {"ad", "soyad"} <= cols:
        return "concat_ws(' ', ad, soyad)"
    if {"isim", "soyisim"} <= cols:
        return "concat_ws(' ', isim, soyisim)"
    if "ad_soyad" in cols:
        return "ad_soyad"
    if "isim" in cols:
        return "isim"
    return "NULL"


# Kampanya listesi dışında kalan ama kişi boyutuna girmesi gereken kaynaklar (02.10 kapsam genişletme).
# isim/firma: SQL ifadesi · tel: telefon kolonları · filtre: dışlanacak satırlar (demo/sahte).
EK_KAYNAK = {
    "paintistanbul_2026_fuar_ziyaretci": dict(isim="COALESCE(NULLIF(trim(ad_soyad), ''), concat_ws(' ', ad, soyad))", firma="firma", tel=["telefon"]),
    "crm_cari": dict(isim="NULL", firma="firma", tel=[]),
    "aysaf_email_kampanya": dict(isim="NULL", firma="NULL", tel=[]),
    "paintistanbul_davetiye_kargo": dict(isim="isim", firma="firma_adi", tel=["telefon", "gsm"]),
    "osb_telemarketing_master": dict(isim="isim", firma="firma", tel=["telefon"]),
    "powderdosing_lead": dict(isim="yetkili_kisi", firma="firma_adi", tel=[]),
    "poliyuretan_yiz_aysad": dict(isim="NULL", firma="firma_adi", tel=["telefon"],
                                  filtre="NOT (kaynak = 'OSB_PAZARI' AND telefon = '+90 312 000 00 00')"),
    "paintistanbul_yiz": dict(isim="isim", firma="sirket", tel=["telefon", "telefon2"]),
    "aysaf_yik": dict(isim="ilgili_kisi", firma="firma_adi", tel=["telefon"]),
    "kartvizitler": dict(isim="isim", firma="sirket", tel=["telefon", "telefon2"]),
}
FIRMA_ADAYLARI = ("firma", "sirket", "firma_adi")


def tablo_alanlari(cols, tablo):
    """Tablonun isim/firma ifadesi, telefon kolonları ve (varsa) dışlama filtresi."""
    if tablo in EK_KAYNAK:
        return dict(EK_KAYNAK[tablo])
    return dict(isim=_isim_ifadesi(cols), firma=next((c for c in FIRMA_ADAYLARI if c in cols), "NULL"),
                tel=[c for c in TEL_KOLONLARI if c in cols])


def kampanya_tablolari(conn):
    with conn.cursor() as cur:
        cur.execute("""SELECT DISTINCT kaynak_tablo FROM pipeline_fuar_meta
                       WHERE aktif = true AND kaynak_tipi = 'Kampanya' AND kaynak_tablo IS NOT NULL""")
        return sorted(t for (t,) in cur.fetchall() if IDENT.match(t))


def benzersiz_kisi_say(conn, tablolar=None, maks_eposta=MAKS_EPOSTA):
    """Dönüş: dict(email_tekil, kisi, birlesen, belirsiz, ...). Veritabanına YAZMAZ."""
    tablolar = tablolar or kampanya_tablolari(conn)
    uf = _UF()
    # (isim, tel) -> e-postalar ; isim -> e-posta kümesi ; telefonu olan e-postalar
    isim_tel = {}
    isim_mail = {}
    mail_tel = set()
    mail_tels = {}
    satir = 0
    with conn.cursor() as cur:
        for t in tablolar:
            al = tablo_alanlari(_kolonlar(cur, t), t)
            sel = ["lower(trim(email))", al["isim"]] + (al["tel"] or ["NULL"])
            kosul = "email IS NOT NULL AND trim(email) <> ''" + (f" AND {al['filtre']}" if al.get("filtre") else "")
            cur.execute(f"SELECT {', '.join(sel)} FROM {t} WHERE {kosul}")
            for row in cur.fetchall():
                satir += 1
                em, isim = row[0], norm_isim(row[1])
                uf.find(em)                                   # kural 1: aynı e-posta = aynı düğüm
                tels = {x for x in (norm_tel(v) for v in row[2:]) if x}
                if tels:
                    mail_tel.add(em)
                    mail_tels.setdefault(em, set()).update(tels)
                if isim and len(isim.split()) >= 2:
                    isim_mail.setdefault(isim, set()).add(em)
                    for tl in tels:
                        isim_tel.setdefault((isim, tl), set()).add(em)
    email_tekil = len(uf.p)
    toplu_grup = toplu_mail = 0
    uyumsuz_grup = ayri_mail = 0
    kenarlar = []
    # Kullanıcı ilkesi (02.10): farklı kurumsal alan adı = farklı kişi. Bir bileşen, birbiriyle ilgisiz iki
    # kurumsal alan adı içeremez (serbest posta iki kurumsal kişiyi köprü gibi bağlayamaz).
    dom = {}
    for e in list(uf.p):
        d = e.split("@")[-1]
        dom[uf.find(e)] = set() if serbest_mi(d) else {d}
    engel = {"telefon+isim": 0, "ikinci_gecis": 0}

    def birles(a, b):
        ra, rb = uf.find(a), uf.find(b)
        if ra == rb:
            return True
        da, db = dom.get(ra, set()), dom.get(rb, set())
        if any(not _alan_benzer(x, y) for x in da for y in db):
            return False
        uf.union(ra, rb)
        dom[uf.find(ra)] = da | db
        return True
    for (isim, _tel), mails in isim_tel.items():             # kural 2: aynı isim + aynı telefon
        if len(mails) > maks_eposta:                          # bir kişinin bu kadar e-postası olmaz: toplu kayıt
            toplu_grup += 1
            toplu_mail += len(mails)
            continue
        sinif = {m: eposta_sinifi(m, isim) for m in mails}
        ad = [m for m in mails if sinif[m] == "ad"]
        genel = [m for m in mails if sinif[m] == "genel"]
        baska = [m for m in mails if sinif[m] == "baska"]
        if baska and len(mails) > 1:                          # başka biri gibi görünen adres ayrı kişidir
            uyumsuz_grup += 1
            ayri_mail += len(baska)
        if ad:                                                # kişinin kendi adresleri + genel kutular tek kişi
            for m in ad[1:] + genel:
                if birles(ad[0], m):
                    kenarlar.append((ad[0], m, "telefon+isim"))
                else:
                    engel["telefon+isim"] += 1
    kisi_birinci = len({uf.find(x) for x in list(uf.p)})
    # İkinci geçiş: telefon kanıtı olmayan, aynı isimli kümeler (kural kodları kanit_var'da)
    kural_say = {"R-a": 0, "R-b": 0, "R-c": 0, "R-d": 0}
    ornekler = []
    for isim, mails in isim_mail.items():
        kum = {}
        for m in mails:
            kum.setdefault(uf.find(m), []).append(m)
        if len(kum) < 2 or len(kum) > maks_eposta:
            continue
        keys = list(kum)
        for i in range(len(keys)):
            for j in range(i + 1, len(keys)):
                ki, kj = keys[i], keys[j]
                if uf.find(ki) == uf.find(kj):
                    continue
                ti = set().union(*[mail_tels.get(m, set()) for m in kum[ki]])
                tj = set().union(*[mail_tels.get(m, set()) for m in kum[kj]])
                if ti and tj and not (ti & tj):               # iki tarafta da telefon var ve farklı -> farklı kişi
                    continue
                kural = None
                ea = eb = None
                for a in kum[ki]:
                    for b in kum[kj]:
                        kural = kanit_var(a, b, isim)
                        if kural:
                            ea, eb = a, b
                            break
                    if kural:
                        break
                if kural and not birles(ki, kj):
                    engel["ikinci_gecis"] += 1
                elif kural:
                    kenarlar.append((ea, eb, kural))
                    kural_say[kural] += 1
                    if len(ornekler) < 5000:
                        ornekler.append((kural, isim, sorted(kum[ki]), sorted(kum[kj])))
    kisi = len({uf.find(x) for x in list(uf.p)})
    # belirsiz: aynı isim, farklı kişi kümeleri, kümelerden en az biri telefonsuz -> kanıt yok
    belirsiz = 0
    for isim, mails in isim_mail.items():
        kumeler = {}
        for m in mails:
            kumeler.setdefault(uf.find(m), []).append(m)
        if len(kumeler) > 1:
            belirsiz += sum(1 for ms in kumeler.values() if not any(m in mail_tel for m in ms))
    boyut = {}
    for x in list(uf.p):
        boyut.setdefault(uf.find(x), []).append(x)
    ayni_alan = sum(1 for ms in boyut.values() if len(ms) > 1 and len({m.split("@")[-1] for m in ms}) == 1)
    return {
        "kume_boyut_2": sum(1 for ms in boyut.values() if len(ms) == 2),
        "kume_boyut_3_ustu": sum(1 for ms in boyut.values() if len(ms) >= 3),
        "en_buyuk_kume": max(len(ms) for ms in boyut.values()),
        "ayni_alan_adi": ayni_alan,
        "toplu_grup": toplu_grup, "toplu_mail": toplu_mail,
        "uyumsuz_grup": uyumsuz_grup, "ayri_mail": ayri_mail,
        "birinci_gecis_kisi": kisi_birinci, "kural_say": kural_say, "ornekler": ornekler, "uf": uf, "kenarlar": kenarlar, "engel": engel,
        "tablo": len(tablolar), "satir": satir,
        "email_tekil": email_tekil, "kisi": kisi,
        "birlesen": email_tekil - kisi, "belirsiz": belirsiz,
    }


def main():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"), port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "cng_expo"), user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("PGPASSWORD") or os.getenv("DB_PASSWORD"), client_encoding="UTF8")
    conn.set_session(readonly=True)
    r = benzersiz_kisi_say(conn)
    def n(x):
        return f"{x:,}".replace(",", ".")
    print(f"Tablo: {r['tablo']}  Satır: {n(r['satir'])}")
    print(f"E-posta bazlı tekil (eski sayım): {n(r['email_tekil'])}")
    print(f"Kişi bazlı tekil (yeni kural):    {n(r['kisi'])}")
    print(f"Birleşen (2. e-posta çıkarıldı):  {n(r['birlesen'])}")
    print(f"Belirsiz (aynı isim, telefon kanıtı yok, birleştirilmedi): {n(r['belirsiz'])}")
    print(f"Toplu/kurumsal kayıt (>{MAKS_EPOSTA} e-posta, birleştirilmedi): {n(r['toplu_grup'])} grup, {n(r['toplu_mail'])} e-posta")
    print(f"Adres isimle uyumsuz (başka kişi gibi, ayrı bırakıldı): {n(r['uyumsuz_grup'])} grup, {n(r['ayri_mail'])} e-posta")
    print(f"İlgisiz kurumsal alan adı nedeniyle engellenen birleşme: {r['engel']}")
    print(f"Telefon kuralıyla kişi: {n(r['birinci_gecis_kisi'])} | ikinci geçiş birleştirmeleri: {r['kural_say']}")
    print(f"Birleşen kümeler: 2 e-postalı {n(r['kume_boyut_2'])}, 3+ e-postalı {n(r['kume_boyut_3_ustu'])}, "
          f"en büyük küme {r['en_buyuk_kume']} e-posta, aynı alan adlı {n(r['ayni_alan_adi'])}")


if __name__ == "__main__":
    main()
