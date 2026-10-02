"""
Kullanıcının ONAYLADIĞI kişi çiftlerini birleştirir (kisi_id -> küçük id kalır, büyük id silinir).

  Eski kişinin e-postaları kalan kişiye taşınır; eski kişinin 'birincil' e-postası 'elle' kuralıyla işaretlenir.
  Kalan kişinin boş isim/firma alanı eskisinden doldurulur; kayit_tipi 'kisi' olan kazanır.
  temas ve portal_eslesme tablolarındaki kisi_id (varsa) taşınır (yoksa FK silmeyi engeller).

Varsayılan çiftler (kullanıcı onayı 02.10.2026, kisi_cakisma_38_inceleme.xlsx incelemesi):
  155597 + 155631   Zeynep Serpil Eryılmaz (icloud + gmail)
   39795 +  39796   Ekin Tükek (flokser.com.tr + flokserkimya.com.tr, alt şirket)
Başka çift için:  --cift 111,222 --cift 333,444   (varsayılanların yerine geçer)

Kullanım:
  python kisi_birlestir.py --hesapla   # SALT OKUNUR: planı gösterir
  python kisi_birlestir.py             # DRY-RUN (yazar, ROLLBACK)
  python kisi_birlestir.py --uygula    # COMMIT (kullanıcı çalıştırır)
"""
import argparse
import sys

from kisi_master_doldur import baglan

VARSAYILAN = [(155597, 155631), (39795, 39796)]
KURALLAR = ("birincil", "telefon+isim", "R-a", "R-b", "R-c", "R-d", "elle")


def bilgi(cur, kid):
    cur.execute("SELECT kisi_id, birincil_email, isim, firma, kayit_tipi FROM kisi_master WHERE kisi_id = %s", (kid,))
    m = cur.fetchone()
    cur.execute("SELECT deger FROM kisi_kimlik WHERE kisi_id = %s ORDER BY deger", (kid,))
    return m, [r[0] for r in cur.fetchall()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cift", action="append", help="'kalan,silinen' ya da 'a,b' (küçük id kalır)")
    ap.add_argument("--hesapla", action="store_true")
    ap.add_argument("--uygula", action="store_true")
    args = ap.parse_args()
    ciftler = [tuple(sorted(int(x) for x in c.split(","))) for c in args.cift] if args.cift else VARSAYILAN

    conn = baglan()
    if args.hesapla:
        conn.set_session(readonly=True)
    cur = conn.cursor()
    plan = []
    for kalan, silinen in ciftler:
        m1, e1 = bilgi(cur, kalan)
        m2, e2 = bilgi(cur, silinen)
        if not m1 or not m2:
            sys.exit(f"DURDU: kisi_id {kalan if not m1 else silinen} bulunamadı (zaten birleşmiş olabilir). Yazılmadı.")
        print(f"\nKALAN  {m1[0]}: isim={m1[2]!r} firma={m1[3]!r} tip={m1[4]}  e-postalar={e1}")
        print(f"SİLİNEN {m2[0]}: isim={m2[2]!r} firma={m2[3]!r} tip={m2[4]}  e-postalar={e2}")
        plan.append((kalan, silinen))
    if args.hesapla:
        print("\nSalt okunur plan tamam; DB'ye yazılmadı.")
        return

    cur.execute("SELECT count(*) FROM kisi_master"); once_m = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM kisi_kimlik"); once_k = cur.fetchone()[0]
    cur.execute("ALTER TABLE kisi_kimlik DROP CONSTRAINT IF EXISTS kisi_kimlik_baglanma_chk")
    cur.execute("ALTER TABLE kisi_kimlik ADD CONSTRAINT kisi_kimlik_baglanma_chk CHECK (baglanma_kurali IN %s)", (KURALLAR,))
    for kalan, silinen in plan:
        cur.execute("UPDATE temas SET kisi_id = %s WHERE kisi_id = %s", (kalan, silinen))
        cur.execute("UPDATE portal_eslesme SET kisi_id = %s WHERE kisi_id = %s", (kalan, silinen))   # FK: silinen kişiye bağlı portal eşleşmeleri
        cur.execute("UPDATE kisi_kimlik SET kisi_id = %s, "
                    "baglanma_kurali = CASE WHEN baglanma_kurali = 'birincil' THEN 'elle' ELSE baglanma_kurali END "
                    "WHERE kisi_id = %s", (kalan, silinen))
        cur.execute("""UPDATE kisi_master m SET
                         isim = coalesce(m.isim, s.isim), firma = coalesce(m.firma, s.firma),
                         firma_norm = coalesce(m.firma_norm, s.firma_norm),
                         kayit_tipi = CASE WHEN m.kayit_tipi = 'kisi' OR s.kayit_tipi = 'kisi' THEN 'kisi' ELSE m.kayit_tipi END,
                         guncelleme_ts = now()
                       FROM kisi_master s WHERE m.kisi_id = %s AND s.kisi_id = %s""", (kalan, silinen))
        cur.execute("DELETE FROM kisi_master WHERE kisi_id = %s", (silinen,))
    cur.execute("SELECT count(*) FROM kisi_master"); son_m = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM kisi_kimlik"); son_k = cur.fetchone()[0]
    print(f"\nkisi_master {once_m} -> {son_m} | kisi_kimlik {once_k} -> {son_k} (değişmemeli)")
    cur.execute("SELECT count(*) FROM (SELECT kisi_id FROM kisi_kimlik GROUP BY 1 HAVING count(*) FILTER "
                "(WHERE baglanma_kurali = 'birincil') <> 1) x")
    print("tam 1 birincil olmayan kişi (0 olmalı):", cur.fetchone()[0])
    for kalan, _ in plan:
        m, e = bilgi(cur, kalan)
        print(f"SONUÇ {m[0]}: isim={m[2]!r} firma={m[3]!r} tip={m[4]} e-postalar={e}")
    if son_m != once_m - len(plan) or son_k != once_k:
        conn.rollback()
        sys.exit("DURDU: sayılar beklenenle uyuşmuyor, ROLLBACK yapıldı.")
    if args.uygula:
        conn.commit()
        print("COMMIT edildi.")
    else:
        conn.rollback()
        print("DRY-RUN: ROLLBACK yapıldı, kalıcı değişiklik yok.")


if __name__ == "__main__":
    main()
