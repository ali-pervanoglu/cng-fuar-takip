"""
kisi_master / kisi_kimlik ilk yükleme (kalıcı kisi_id).

Kural kaynağı: kisi_tekillestir.py (e-posta + isim + telefon + e-posta baş kısmı + ikinci geçiş).
Her kişi bir `kisi_master` satırı, her e-postası bir `kisi_kimlik` satırıdır (tip='email').
Telefonlar kimlik OLARAK YAZILMAZ: UNIQUE(tip, deger) kısıtı var ve bir telefon birden fazla kişiye ait olabilir.

Kullanım (hepsi tablo boşken; dolu tabloda durur):
  python kisi_master_doldur.py --hesapla   # SALT OKUNUR: hesaplar, sayıları basar, DB'ye yazmaz
  python kisi_master_doldur.py             # DRY-RUN: yazar, kontrol eder, ROLLBACK (kalıcı değişiklik YOK)
  python kisi_master_doldur.py --uygula    # YAZAR ve COMMIT eder (kullanıcı çalıştırır)
Parola: PGPASSWORD (ya da DB_PASSWORD) ortam değişkeni.
"""
import argparse
import collections
import os
import re
import sys
import unicodedata

import psycopg2
from psycopg2.extras import execute_values

import kisi_tekillestir as K

FIRMA_KOLONLARI = ("firma", "sirket", "firma_adi")


def firma_norm(s):
    if not s:
        return None
    s = str(s).replace("İ", "i").replace("I", "ı").lower().replace("ı", "i")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s or None


def temiz_ad(s):
    if not s:
        return None
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s if s and s.lower() not in K.YER_TUTUCU else None


def baglan():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"), port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "cng_expo"), user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("PGPASSWORD") or os.getenv("DB_PASSWORD"), client_encoding="UTF8")


def kisileri_hazirla(conn):
    """Dönüş: kişi listesi [(birincil_email, isim, firma, firma_norm, domain, [(email, kaynak_tablo)])]."""
    r = K.benzersiz_kisi_say(conn)
    uf = r["uf"]
    isimler = collections.defaultdict(collections.Counter)       # email -> ham isim sayacı
    firmalar = collections.defaultdict(collections.Counter)
    tablolar = collections.defaultdict(collections.Counter)
    with conn.cursor() as cur:
        for t in K.kampanya_tablolari(conn):
            cols = K._kolonlar(cur, t)
            fc = next((c for c in FIRMA_KOLONLARI if c in cols), None)
            cur.execute(f"SELECT lower(trim(email)), {K._isim_ifadesi(cols)}, {fc or 'NULL'} FROM {t} "
                        f"WHERE email IS NOT NULL AND trim(email) <> ''")
            for em, isim, firma in cur.fetchall():
                tablolar[em][t] += 1
                a, f = temiz_ad(isim), temiz_ad(firma)
                if a:
                    isimler[em][a] += 1
                if f:
                    firmalar[em][f] += 1
    kume = collections.defaultdict(list)
    for em in uf.p:
        kume[uf.find(em)].append(em)

    def puan(em, isim_norm):
        sinif = K.eposta_sinifi(em, isim_norm)
        serbest = em.split("@")[-1] in K.SERBEST_ALAN
        return ({"ad": 2, "genel": 1, "baska": 0}[sinif] * 2 + (0 if serbest else 1), sum(tablolar[em].values()))

    kisiler = []
    for uyeler in kume.values():
        isim_say = collections.Counter()
        firma_say = collections.Counter()
        for em in uyeler:
            isim_say.update(isimler[em])
            firma_say.update(firmalar[em])
        isim = None
        if isim_say:   # en sık; eşitlikte karışık harfli (Ad Soyad) yazım öncelikli, sonra alfabetik
            isim = sorted(isim_say.items(), key=lambda kv: (-kv[1], kv[0].isupper(), kv[0]))[0][0]
        firma = sorted(firma_say.items(), key=lambda kv: (-kv[1], kv[0]))[0][0] if firma_say else None
        inorm = K.norm_isim(isim) if isim else None
        birincil = sorted(uyeler, key=lambda em: (tuple(-x for x in puan(em, inorm)), em))[0]
        kaynak = {em: tablolar[em].most_common(1)[0][0] for em in uyeler}
        kisiler.append((birincil, isim, firma, firma_norm(firma), birincil.split("@")[-1],
                        sorted((em, kaynak[em]) for em in uyeler)))
    kisiler.sort(key=lambda x: x[0])                                # kisi_id sırası tekrarlanabilir
    return kisiler, r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hesapla", action="store_true", help="salt okunur: yalnız hesapla ve raporla")
    ap.add_argument("--uygula", action="store_true", help="COMMIT et (varsayılan: ROLLBACK'li dry-run)")
    args = ap.parse_args()

    conn = baglan()
    if args.hesapla:
        conn.set_session(readonly=True)
    with conn.cursor() as cur:
        cur.execute("SELECT (SELECT count(*) FROM kisi_master), (SELECT count(*) FROM kisi_kimlik), "
                    "(SELECT count(*) FROM temas)")
        km, kk, tm = cur.fetchone()
    print(f"Mevcut: kisi_master={km}, kisi_kimlik={kk}, temas={tm}")
    if km or kk:
        sys.exit("DURDU: tablolar boş değil. Bu betik yalnız ilk yükleme içindir (artımlı yükleme ayrı).")

    kisiler, r = kisileri_hazirla(conn)
    n_kimlik = sum(len(k[5]) for k in kisiler)
    boyut = collections.Counter(min(len(k[5]), 5) for k in kisiler)
    print(f"Kişi (kisi_master satırı): {len(kisiler):,}  | E-posta (kisi_kimlik satırı): {n_kimlik:,}".replace(",", "."))
    print(f"  kisi_tekillestir kişi sayısı: {r['kisi']:,} | e-posta bazlı: {r['email_tekil']:,}".replace(",", "."))
    print(f"  kişi başına e-posta: 1 -> {boyut[1]:,}, 2 -> {boyut[2]:,}, 3 -> {boyut[3]:,}, 4 -> {boyut[4]:,}, 5+ -> {boyut[5]:,}".replace(",", "."))
    print(f"  isimsiz kişi: {sum(1 for k in kisiler if not k[1]):,} | firmasız: {sum(1 for k in kisiler if not k[2]):,}".replace(",", "."))
    assert len(kisiler) == r["kisi"], "kişi sayısı kisi_tekillestir ile uyuşmuyor"
    assert n_kimlik == r["email_tekil"], "e-posta sayısı uyuşmuyor"
    assert len({e for k in kisiler for e, _ in k[5]}) == n_kimlik, "bir e-posta birden fazla kişide"
    if args.hesapla:
        print("Salt okunur hesap tamam; DB'ye yazılmadı.")
        return

    with conn.cursor() as cur:
        master = [(i, k[0], k[1], k[2], k[3], k[4]) for i, k in enumerate(kisiler, 1)]
        execute_values(cur, "INSERT INTO kisi_master (kisi_id, birincil_email, isim, firma, firma_norm, domain) VALUES %s",
                       master, page_size=5000)
        kimlik = [(i, "email", em, kt) for i, k in enumerate(kisiler, 1) for em, kt in k[5]]
        execute_values(cur, "INSERT INTO kisi_kimlik (kisi_id, tip, deger, kaynak_tablo) VALUES %s",
                       kimlik, page_size=5000)
        cur.execute("SELECT (SELECT count(*) FROM kisi_master), (SELECT count(*) FROM kisi_kimlik), "
                    "(SELECT count(DISTINCT deger) FROM kisi_kimlik WHERE tip='email')")
        print("Yazıldı (transaction içinde): kisi_master=%s, kisi_kimlik=%s, tekil e-posta=%s" % cur.fetchone())
        if args.uygula:
            cur.execute("SELECT setval('kisi_master_kisi_id_seq', (SELECT max(kisi_id) FROM kisi_master))")
            conn.commit()
            print("COMMIT edildi.")
        else:
            conn.rollback()
            print("DRY-RUN: ROLLBACK yapıldı, kalıcı değişiklik yok.")


if __name__ == "__main__":
    main()
