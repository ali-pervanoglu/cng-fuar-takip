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


def eposta_sinifi(mail, isim_norm):
    """E-postanın baş kısmına göre: 'ad' (isimle uyumlu), 'genel' (info@, sales@ ...), 'baska' (başka biri gibi).
    Kullanıcı kuralı (02.10): aynı isim+telefonla kayıtlı ama baş kısmı farklı kişiye ait adresler farklı kişidir
    (toplu/hızlı kayıt); kişinin kendi adresi + şirketin genel kutusu aynı kişidir."""
    yerel = norm_isim(mail.split("@")[0].replace(".", " ").replace("_", " ").replace("-", " "))
    ham = re.sub(r"[^a-z]", "", mail.split("@")[0].lower())
    if not ham:
        return "baska"
    belirtec = [t for t in (isim_norm or "").split() if len(t) >= 3]
    if any(t in ham for t in belirtec):
        return "ad"
    if any(ham == g or ham.startswith(g) for g in GENEL_KUTU):
        return "genel"
    return "baska"


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
    satir = 0
    with conn.cursor() as cur:
        for t in tablolar:
            cols = _kolonlar(cur, t)
            tel_cols = [c for c in TEL_KOLONLARI if c in cols]
            sel = ["lower(trim(email))", _isim_ifadesi(cols)] + (tel_cols or ["NULL"])
            cur.execute(f"SELECT {', '.join(sel)} FROM {t} WHERE email IS NOT NULL AND trim(email) <> ''")
            for row in cur.fetchall():
                satir += 1
                em, isim = row[0], norm_isim(row[1])
                uf.find(em)                                   # kural 1: aynı e-posta = aynı düğüm
                tels = {x for x in (norm_tel(v) for v in row[2:]) if x}
                if tels:
                    mail_tel.add(em)
                if isim and len(isim.split()) >= 2:
                    isim_mail.setdefault(isim, set()).add(em)
                    for tl in tels:
                        isim_tel.setdefault((isim, tl), set()).add(em)
    email_tekil = len(uf.p)
    toplu_grup = toplu_mail = 0
    uyumsuz_grup = ayri_mail = 0
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
                uf.union(ad[0], m)
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
    print(f"Birleşen kümeler: 2 e-postalı {n(r['kume_boyut_2'])}, 3+ e-postalı {n(r['kume_boyut_3_ustu'])}, "
          f"en büyük küme {r['en_buyuk_kume']} e-posta, aynı alan adlı {n(r['ayni_alan_adi'])}")


if __name__ == "__main__":
    main()
