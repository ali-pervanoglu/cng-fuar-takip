"""
MEV ihtiyaç raporu ve CSV üretici (SALT OKUNUR — veritabanına yazmaz).

Her kampanya kartı (pipeline_fuar_meta.kaynak_tablo + kaynak_filtre) için kitledeki e-postaları
`v_kisi_havuz` e-posta düzeyi MEV durumuyla karşılaştırır:
  KULLANILABILIR : MEV güncel (<=75 gün) ve son karar GÖNDER      -> doğrulatmaya gerek yok
  YENIDEN_MEV    : hiç MEV sonucu yok ya da MEV 75 günden eski    -> MEV'e verilecek
  TARIHSIZ_MEV   : MEV sonucu var ama mev_tarih boş (yaş bilinmiyor)  -> verilmez, tarih doldurulmalı
  ENGELLI        : daha önce GÖNDERME/GEÇERSİZ/RED/İHTİYATLI/Catch-all ya da biçim hatalı -> verilmez
Kural kaynağı: BAGLAM_01 'EMAIL DOĞRULAMA SÜRECİ' (MEV <=75 gün geçerli; MX GÖNDERME ise MEV'e bakılmaz).
CRM 'Müşteri' alan adları kampanyaya alınmadığı için varsayılan olarak dışarıda tutulur.

Kullanım:
  python mev_ihtiyac.py                      # yaklaşan fuarların kartları için rapor
  python mev_ihtiyac.py --hepsi              # geçmiş fuarlar dahil
  python mev_ihtiyac.py --csv paintist_2028_ydz   # o kart için başsız CSV (yalnız YENIDEN_MEV)
Çıktı CSV: Downloads\\mev_girdi_<fuar_kod>_<YYYYMMDD>.csv  (başlık satırı YOK; MEV'e böyle yüklenir)
Parola: PGPASSWORD (ya da DB_PASSWORD).
"""
import argparse
import collections
import datetime
import os
import pathlib
import re
import sys

import psycopg2

IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def baglan():
    c = psycopg2.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"), port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "cng_expo"), user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("PGPASSWORD") or os.getenv("DB_PASSWORD"), client_encoding="UTF8")
    c.set_session(readonly=True)
    return c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hepsi", action="store_true", help="geçmiş fuarlar dahil")
    ap.add_argument("--csv", metavar="FUAR_KOD", help="bu kart için MEV girdi CSV'si üret")
    ap.add_argument("--musteri-dahil", action="store_true", help="CRM Müşteri alan adlarını dışarıda TUTMA")
    args = ap.parse_args()

    conn = baglan()
    cur = conn.cursor()
    cur.execute("""SELECT fuar_kod, fuar_ad, fuar_alt, kaynak_tablo, coalesce(kaynak_filtre, ''),
                          fuar_tarihi_baslangic, mev_durum, email_durum
                   FROM v_pipeline_ozet
                   WHERE kaynak_tipi = 'Kampanya' AND kaynak_tablo IS NOT NULL
                   ORDER BY fuar_tarihi_baslangic NULLS LAST, fuar_ad, fuar_alt""")
    kartlar = cur.fetchall()

    # e-posta düzeyi MEV durumu (v_kisi_havuz'da aynı e-posta tüm satırlarında aynı değerleri taşır)
    # sonuc_var: e-postanın herhangi bir satırında MEV sonucu dolu mu? (tarih boş olsa bile)
    cur.execute("""SELECT email, min(capraz_durum), min(email_mev_durumu),
                          bool_or(coalesce(trim(mev_sonuc), '') <> '')
                   FROM v_kisi_havuz GROUP BY email""")
    durum = {e: (cd, md, sv) for e, cd, md, sv in cur.fetchall()}

    musteri_alan = set()
    if not args.musteri_dahil:
        cur.execute("SELECT DISTINCT lower(trim(domain)) FROM crm_cari WHERE tip ILIKE 'M%teri' AND domain IS NOT NULL")
        musteri_alan = {r[0] for r in cur.fetchall() if r[0]}

    bugun = datetime.date.today()
    n = lambda x: f"{x:,}".replace(",", ".")
    satirlar = []
    csv_epostalar = None
    for kod, ad, alt, tablo, filtre, tarih, mev_d, em_d in kartlar:
        if not IDENT.match(tablo):
            continue
        if not args.hepsi and tarih and tarih < bugun and not (args.csv and kod == args.csv):
            continue
        kosul = "email IS NOT NULL AND trim(email) <> ''" + (f" AND ({filtre})" if filtre else "")
        try:
            cur.execute(f"SELECT DISTINCT lower(trim(email)) FROM {tablo} WHERE {kosul}")
        except psycopg2.Error as e:
            conn.rollback()
            print(f"UYARI: {kod} atlandı ({str(e).strip()[:70]})", file=sys.stderr)
            continue
        epostalar = [r[0] for r in cur.fetchall()]
        sayac = collections.Counter()
        mevde_beklenen = []
        for e in epostalar:
            if e.split("@")[-1] in musteri_alan:
                sayac["MÜŞTERİ (dışlandı)"] += 1
                continue
            cd, md, sonuc_var = durum.get(e, ("YENIDEN_MEV", "MEV_YOK", False))
            if cd == "YENIDEN_MEV" and md == "MEV_YOK" and sonuc_var:
                sayac["TARIHSIZ_MEV"] += 1          # MEV yapılmış (sonuç var) ama tarih yazılmamış: yaş bilinmiyor
                continue
            sayac[cd] += 1
            if cd == "YENIDEN_MEV":
                sayac["  MEV_YOK" if md == "MEV_YOK" else "  MEV_ESKI"] += 1
                mevde_beklenen.append(e)
        gun = (tarih - bugun).days if tarih else None
        satirlar.append((tarih, kod, ad, alt, gun, len(epostalar), sayac, mev_d, em_d))
        if args.csv and kod == args.csv:
            csv_epostalar = sorted(mevde_beklenen)

    if args.csv:
        if csv_epostalar is None:
            sys.exit(f"DURDU: '{args.csv}' kartı bulunamadı (fuar_kod'u v_pipeline_ozet'ten kontrol et).")
        yol = pathlib.Path.home() / "Downloads" / f"mev_girdi_{args.csv}_{bugun:%Y%m%d}.csv"
        yol.write_text("\n".join(csv_epostalar) + ("\n" if csv_epostalar else ""), encoding="utf-8")
        print(f"CSV yazıldı: {yol}  ({n(len(csv_epostalar))} e-posta, BAŞLIK YOK)")
        return

    print(f"{'Fuar kodu':32} {'Gün':>5} {'Kitle':>8} {'Kullanılabilir':>14} {'Tarihsiz MEV':>12} {'MEV bekleyen':>12} {'(yok/eski)':>12} {'Engelli':>8}  Kart (MEV/Email)")
    for tarih, kod, ad, alt, gun, kitle, s, mev_d, em_d in satirlar:
        yok, eski = s.get("  MEV_YOK", 0), s.get("  MEV_ESKI", 0)
        gun_s = "-" if gun is None else str(gun)
        print(f"{kod:32} {gun_s:>5} {n(kitle):>8} {n(s.get('KULLANILABILIR', 0)):>14} {n(s.get('TARIHSIZ_MEV', 0)):>12} "
              f"{n(s.get('YENIDEN_MEV', 0)):>12} {f'{n(yok)}/{n(eski)}':>12} {n(s.get('ENGELLI', 0)):>8}  {mev_d}/{em_d}")
    print("\nTarihsiz MEV = MEV sonucu kayıtlı ama mev_tarih boş: yaş bilinmiyor, MEV'e VERİLMEZ (tarih doldurulursa 75 gün kuralı işler).")
    print("\nCSV için:  python mev_ihtiyac.py --csv <Fuar kodu>   (MEV'e başlıksız yüklenir)")
    if musteri_alan:
        print(f"Not: {n(len(musteri_alan))} CRM Müşteri alan adı dışarıda tutuldu (--musteri-dahil ile dahil edilir).")


if __name__ == "__main__":
    main()
