"""
kisi_master.kayit_tipi ve kisi_kimlik.baglanma_kurali kolonlarını ekler ve doldurur.

  kisi_master.kayit_tipi        'kisi' | 'genel_kutu'
      genel_kutu = isimsiz kişi VE birincil adresi şirket genel kutusu (info@, sales@, office@ ...).
      İsimli kişiler 'kisi' kalır (info@ ile adını yazarak kayıt olan kişi, kişidir).
  kisi_kimlik.baglanma_kurali   'birincil' | 'telefon+isim' | 'R-a' | 'R-b' | 'R-c'
      birincil      : kişinin birincil e-postası
      telefon+isim  : aynı isim + aynı telefon (+ e-posta baş kısmı kuralı)
      R-a/R-b/R-c   : ikinci geçiş kanıt kuralları (kisi_tekillestir.kanit_var)
      Birincil e-postadan kenar ağacı boyunca, e-postayı kişiye bağlayan kuralın adı yazılır.
  Mevcut kisi_id'ler DEĞİŞMEZ; yalnız yeni kolonlar eklenir ve doldurulur.

Kullanım:
  python kisi_sema_guncelle.py --hesapla   # SALT OKUNUR: kolon eklemez, sayıları hesaplayıp basar
  python kisi_sema_guncelle.py             # DRY-RUN: ALTER + UPDATE çalışır, sonra ROLLBACK
  python kisi_sema_guncelle.py --uygula    # COMMIT eder (kullanıcı çalıştırır)
Parola: PGPASSWORD (ya da DB_PASSWORD).
"""
import argparse
import collections
import re
import sys

from psycopg2.extras import execute_values

import kisi_tekillestir as K
from kisi_master_doldur import baglan

KURALLAR = ("birincil", "telefon+isim", "R-a", "R-b", "R-c", "R-d")


def genel_kutu_mu(email):
    ham = re.sub(r"[^a-z]", "", email.split("@")[0].lower())
    return bool(ham) and K.genel_mi(ham)


def hesapla(conn):
    """Dönüş: (kayit_tipi {kisi_id: tip}, kural {email: kural}, rapor dict). DB'den yalnız okur."""
    r = K.benzersiz_kisi_say(conn)
    uf = r["uf"]
    with conn.cursor() as cur:
        cur.execute("SELECT kisi_id, birincil_email, isim FROM kisi_master")
        master = {k: (e, n) for k, e, n in cur.fetchall()}
        cur.execute("SELECT deger, kisi_id FROM kisi_kimlik WHERE tip = 'email'")
        mail_kisi = dict(cur.fetchall())

    # kenar ağacı: birincil e-postadan BFS ile her e-postayı bağlayan kuralı bul
    komsu = collections.defaultdict(list)
    for a, b, kural in r["kenarlar"]:
        komsu[a].append((b, kural))
        komsu[b].append((a, kural))
    kural = {}
    for kid, (birincil, _isim) in master.items():
        kural[birincil] = "birincil"
        kuyruk = collections.deque([birincil])
        while kuyruk:
            x = kuyruk.popleft()
            for y, kr in komsu[x]:
                if y not in kural:
                    kural[y] = kr
                    kuyruk.append(y)

    # tutarlılık: motorun bugünkü kümeleri = yüklenmiş kisi_id'ler mi?
    kume_kisi = collections.defaultdict(set)
    for em, kid in mail_kisi.items():
        kume_kisi[uf.find(em)].add(kid) if em in uf.p else None
    uyumsuz = sum(1 for s in kume_kisi.values() if len(s) != 1)
    kisi_kume = collections.defaultdict(set)
    for em, kid in mail_kisi.items():
        if em in uf.p:
            kisi_kume[kid].add(uf.find(em))
    uyumsuz += sum(1 for s in kisi_kume.values() if len(s) != 1)
    kayitsiz = [em for em in mail_kisi if em not in kural]

    tip = {}
    for kid, (birincil, isim) in master.items():
        tip[kid] = "genel_kutu" if (not isim and genel_kutu_mu(birincil)) else "kisi"
    rapor = {
        "kisi": len(master), "kimlik": len(mail_kisi),
        "genel_kutu": sum(1 for t in tip.values() if t == "genel_kutu"),
        "kural_dagilim": collections.Counter(kural[e] for e in mail_kisi if e in kural),
        "uyumsuz_kume": uyumsuz, "kuralsiz_email": len(kayitsiz),
    }
    return tip, {e: kural[e] for e in mail_kisi if e in kural}, rapor


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hesapla", action="store_true", help="salt okunur: yalnız hesapla")
    ap.add_argument("--uygula", action="store_true", help="COMMIT et (varsayılan: ROLLBACK'li dry-run)")
    args = ap.parse_args()

    conn = baglan()
    if args.hesapla:
        conn.set_session(readonly=True)
    tip, kural, rap = hesapla(conn)
    n = lambda x: f"{x:,}".replace(",", ".")
    print(f"kisi_master: {n(rap['kisi'])} | kisi_kimlik: {n(rap['kimlik'])}")
    print(f"kayit_tipi  -> genel_kutu: {n(rap['genel_kutu'])} | kisi: {n(rap['kisi'] - rap['genel_kutu'])}")
    print("baglanma_kurali ->", {k: n(v) for k, v in rap["kural_dagilim"].items()})
    print(f"Tutarlılık: motor kümesi ile yüklü kisi_id uyuşmayan: {rap['uyumsuz_kume']} | kuralı bulunamayan e-posta: {rap['kuralsiz_email']}")
    if rap["uyumsuz_kume"] or rap["kuralsiz_email"]:
        sys.exit("DURDU: motor kümeleri yüklü kisi_id'lerle uyuşmuyor (veri sonradan değişmiş olabilir). Yazılmadı.")
    if args.hesapla:
        print("Salt okunur hesap tamam; DB'ye yazılmadı.")
        return

    with conn.cursor() as cur:
        cur.execute("ALTER TABLE kisi_master  ADD COLUMN IF NOT EXISTS kayit_tipi text NOT NULL DEFAULT 'kisi'")
        cur.execute("ALTER TABLE kisi_kimlik  ADD COLUMN IF NOT EXISTS baglanma_kurali text NOT NULL DEFAULT 'birincil'")
        cur.execute("ALTER TABLE kisi_master  DROP CONSTRAINT IF EXISTS kisi_master_kayit_tipi_chk")
        cur.execute("ALTER TABLE kisi_master  ADD CONSTRAINT kisi_master_kayit_tipi_chk CHECK (kayit_tipi IN ('kisi','genel_kutu'))")
        cur.execute("ALTER TABLE kisi_kimlik  DROP CONSTRAINT IF EXISTS kisi_kimlik_baglanma_chk")
        cur.execute("ALTER TABLE kisi_kimlik  ADD CONSTRAINT kisi_kimlik_baglanma_chk CHECK (baglanma_kurali IN %s)", (KURALLAR,))
        gk = [(kid,) for kid, t in tip.items() if t == "genel_kutu"]
        execute_values(cur, "UPDATE kisi_master m SET kayit_tipi = 'genel_kutu', guncelleme_ts = now() "
                            "FROM (VALUES %s) v(kid) WHERE m.kisi_id = v.kid", gk, page_size=5000)
        degisen = [(e, k) for e, k in kural.items() if k != "birincil"]
        execute_values(cur, "UPDATE kisi_kimlik c SET baglanma_kurali = v.k "
                            "FROM (VALUES %s) v(e, k) WHERE c.tip = 'email' AND c.deger = v.e", degisen, page_size=5000)
        cur.execute("SELECT kayit_tipi, count(*) FROM kisi_master GROUP BY 1 ORDER BY 1")
        print("kisi_master.kayit_tipi:", cur.fetchall())
        cur.execute("SELECT baglanma_kurali, count(*) FROM kisi_kimlik GROUP BY 1 ORDER BY 2 DESC")
        print("kisi_kimlik.baglanma_kurali:", cur.fetchall())
        cur.execute("SELECT count(*) FROM kisi_kimlik c JOIN kisi_master m USING (kisi_id) "
                    "WHERE c.deger = m.birincil_email AND c.baglanma_kurali <> 'birincil'")
        print("birincil e-postası 'birincil' olmayan (0 olmalı):", cur.fetchone()[0])
        if args.uygula:
            conn.commit()
            print("COMMIT edildi.")
        else:
            conn.rollback()
            print("DRY-RUN: ROLLBACK yapıldı, kalıcı değişiklik yok.")


if __name__ == "__main__":
    main()
