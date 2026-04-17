#!/usr/bin/env python3
# =============================================================================
#  CNG Expo — Pipeline ETL Script
#  Dosya    : etl_pipeline.py
#  Yazar    : Data & CRM Operations — ali.pervanoglu@cngexpo.com
#  Versiyon : 1.0  (2026-04)
#
#  Görev    :
#    1. PostgreSQL v_pipeline_ozet view'ından tüm fuar + durum verilerini çeker.
#    2. kaynak_tablo tanımlı fuarlar için COUNT(*) ile gerçek satır sayısını günceller.
#    3. Genel KPI toplamlarını hesaplar (toplam kişi, email hazır, SMS hazır, DB kaydı).
#    4. data.json dosyasını üretir.
#    5. index.html içindeki FUARLAR ve KPI değerlerini otomatik günceller.
#    6. pipeline_etl_log tablosuna çalışma kaydı bırakır.
#
#  Kullanım :
#    python etl_pipeline.py              # Normal çalıştır
#    python etl_pipeline.py --dry-run   # DB'yi okur, dosyaları YAZMAZ (test)
#    python etl_pipeline.py --no-count  # COUNT sorgularını atla (hızlı mod)
#
#  Bağımlılıklar:
#    pip install psycopg2-binary python-dotenv
# =============================================================================

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Konfigürasyon
# ---------------------------------------------------------------------------

BASE_DIR   = Path(__file__).parent
OUTPUT_DIR = BASE_DIR          # data.json ve index.html nerede yazılacak
LOG_DIR    = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

# Çıktı dosyaları
DATA_JSON_PATH  = OUTPUT_DIR / "data.json"
INDEX_HTML_PATH = OUTPUT_DIR / "index.html"

# Loglama
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / f"etl_{datetime.now():%Y%m%d}.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("cng_etl")

# ---------------------------------------------------------------------------
# DB Bağlantısı
# ---------------------------------------------------------------------------

def get_conn():
    """
    .env dosyasından veya ortam değişkenlerinden bağlantı bilgilerini okur.

    .env örneği:
        DB_HOST=localhost
        DB_PORT=5432
        DB_NAME=cngexpo
        DB_USER=ali
        DB_PASSWORD=gizli_sifre
    """
    load_dotenv(BASE_DIR / ".env")
    return psycopg2.connect(
        host     = os.getenv("DB_HOST",     "localhost"),
        port     = int(os.getenv("DB_PORT", "5432")),
        dbname   = os.getenv("DB_NAME",     "cngexpo"),
        user     = os.getenv("DB_USER",     "postgres"),
        password = os.getenv("DB_PASSWORD", ""),
        options  = "-c search_path=public",
        client_encoding = "utf8",
        connect_timeout = 10,
    )


# ---------------------------------------------------------------------------
# Adım 1: v_pipeline_ozet'ten okuma
# ---------------------------------------------------------------------------

def fetch_pipeline_rows(conn) -> list[dict]:
    """v_pipeline_ozet view'ından tüm aktif fuar satırlarını çeker."""
    sql = """
        SELECT
            fuar_id, fuar_kod, fuar_ad, fuar_alt, fuar_tarih,
            kaynak_tablo, kaynak_filtre, sira,
            temizlik_durum, db_durum, mx_durum, mev_durum,
            email_durum, sms_durum,
            kayit_sayisi, email_gonder_sayisi, sms_gonder_sayisi,
            email_not, genel_not,
            tamamlanma_pct,
            guncelleme_ts
        FROM v_pipeline_ozet
        ORDER BY sira, fuar_id
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        rows = [dict(r) for r in cur.fetchall()]
    log.info(f"v_pipeline_ozet: {len(rows)} fuar satırı okundu")
    return rows


# ---------------------------------------------------------------------------
# Adım 2: Gerçek COUNT sorgularıyla satır sayılarını güncelle
# ---------------------------------------------------------------------------

def refresh_counts(conn, rows: list[dict]) -> list[dict]:
    """
    Her fuarın kaynak_tablo alanı doluysa COUNT(*) çalıştırır
    ve hem DB'deki pipeline_durum kaydını hem de rows listesini günceller.
    """
    updated = 0
    with conn.cursor() as cur:
        for row in rows:
            tbl    = row.get("kaynak_tablo")
            filtre = row.get("kaynak_filtre")

            if not tbl:
                log.debug(f"  {row['fuar_kod']}: kaynak_tablo yok, atlanıyor")
                continue

            # Güvenli tablo adı kontrolü (SQL injection önlemi)
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', tbl):
                log.warning(f"  {row['fuar_kod']}: Geçersiz tablo adı '{tbl}', atlanıyor")
                continue

            count_sql = f"SELECT COUNT(*) FROM {tbl}"
            if filtre:
                count_sql += f" WHERE {filtre}"

            try:
                cur.execute(count_sql)
                count = cur.fetchone()[0]
                row["kayit_sayisi"] = count

                # pipeline_durum tablosunu da güncelle
                cur.execute(
                    """
                    UPDATE pipeline_durum
                    SET    kayit_sayisi  = %s,
                           guncelleme_ts = NOW(),
                           guncelleyen   = 'etl_count_refresh'
                    WHERE  fuar_id = %s
                    """,
                    (count, row["fuar_id"])
                )
                updated += 1
                log.info(f"  COUNT {tbl}: {count:,} satır → pipeline_durum güncellendi")

            except psycopg2.Error as e:
                log.warning(f"  {row['fuar_kod']}: COUNT sorgusu başarısız — {e}")
                conn.rollback()

    conn.commit()
    log.info(f"refresh_counts tamamlandı: {updated} tablo güncellendi")
    return rows


# ---------------------------------------------------------------------------
# Adım 3: KPI toplamlarını hesapla
# ---------------------------------------------------------------------------

def compute_kpis(rows: list[dict]) -> dict[str, Any]:
    """Dashboard'un en üstündeki KPI kartları için toplamları hesaplar."""
    kpi = {
        "aktif_fuar"    : len(rows),
        "toplam_kisi"   : sum(r["kayit_sayisi"]         for r in rows),
        "email_hazir"   : sum(r["email_gonder_sayisi"]  for r in rows),
        "sms_hazir"     : sum(r["sms_gonder_sayisi"]    for r in rows),
        "db_kayit"      : sum(
                              r["kayit_sayisi"] for r in rows
                              if r["db_durum"] == "D"
                          ),
        "son_guncelleme": datetime.now(timezone.utc).strftime("%d.%m.%Y"),
    }
    log.info(
        f"KPI → aktif_fuar={kpi['aktif_fuar']}  "
        f"toplam_kisi={kpi['toplam_kisi']:,}  "
        f"email_hazir={kpi['email_hazir']:,}  "
        f"sms_hazir={kpi['sms_hazir']:,}  "
        f"db_kayit={kpi['db_kayit']:,}"
    )
    return kpi


# ---------------------------------------------------------------------------
# Adım 4: data.json üret
# ---------------------------------------------------------------------------

def build_json_payload(rows: list[dict], kpi: dict) -> dict:
    """
    index.html'in okuyacağı JSON yapısını oluşturur.
    Tarih/Decimal gibi JSON-dışı tipleri str'e dönüştürür.
    """
    fuarlar = []
    for r in rows:
        fuarlar.append({
            "ad"    : r["fuar_ad"],
            "alt"   : r["fuar_alt"],
            "tarih" : r["fuar_tarih"],
            "t"     : r["temizlik_durum"],
            "db"    : r["db_durum"],
            "mx"    : r["mx_durum"],
            "mev"   : r["mev_durum"],
            "em"    : r["email_durum"],
            "sms"   : r["sms_durum"],
            "kayit" : f"{r['kayit_sayisi']:,}".replace(",", "."),   # Türkçe format
            "gonder": r["email_not"] or "",
            "pct"   : int(r["tamamlanma_pct"] or 0),
        })

    return {
        "meta"   : {"uretim_ts": datetime.now(timezone.utc).isoformat()},
        "kpi"    : kpi,
        "fuarlar": fuarlar,
    }


def write_json(payload: dict, path: Path, dry_run: bool = False) -> None:
    if dry_run:
        log.info(f"[DRY-RUN] data.json yazılmadı ({path})")
        return
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    log.info(f"data.json yazıldı → {path}  ({path.stat().st_size:,} byte)")


# ---------------------------------------------------------------------------
# Adım 5: index.html güncelle
# ---------------------------------------------------------------------------

# index.html içindeki FUARLAR array'ini bulup değiştiriyoruz.
# Regex: "var FUARLAR = [  ...  ];" bloğunu yakalar.
_FUARLAR_RE = re.compile(
    r'(var\s+FUARLAR\s*=\s*)\[.*?\];',
    re.DOTALL
)

# KPI kartları için id=".." içeren span/div değerlerini değiştirmek yerine
# JS inline değişkenlerini değiştiriyoruz (güvenli yol).
# HTML'de şu satırlar zaten mevcut; onları regex ile bul-değiştir:
#   <div class="stat-val">6</div>  → aktif_fuar
# Daha sağlıklı yol: HTML'e data-kpi="aktif_fuar" attr eklemek (bkz. index_v2.html).
# Şimdilik FUARLAR array + tarih güncellemesi yapılıyor.

def update_html(payload: dict, html_path: Path, dry_run: bool = False) -> None:
    """
    index.html içindeki FUARLAR JS array'ini ve Son Güncelleme tarihini
    güncel verilerle değiştirir.  KPI kartları için data-kpi nitelik
    yaklaşımını kullanan index_v2.html daha sağlıklıdır (bkz. 03_index_v2.html).
    """
    if not html_path.exists():
        log.warning(f"index.html bulunamadı: {html_path}, güncelleme atlandı")
        return

    html = html_path.read_text(encoding="utf-8")

    # --- FUARLAR array ---
    new_array_lines = []
    for f in payload["fuarlar"]:
        line = (
            f'  {{ad:"{f["ad"]}", alt:"{f["alt"]}", tarih:"{f["tarih"]}", '
            f't:{f["t"]}, db:{f["db"]}, mx:{f["mx"]}, mev:{f["mev"]}, '
            f'em:{f["em"]}, sms:{f["sms"]}, '
            f'kayit:"{f["kayit"]}", gonder:"{f["gonder"]}", pct:{f["pct"]}}}'
        )
        new_array_lines.append(line)

    new_array_str = "[\n" + ",\n".join(new_array_lines) + "\n];"
    new_html, n = _FUARLAR_RE.subn(r'\g<1>' + new_array_str, html)

    if n == 0:
        log.warning("FUARLAR array index.html içinde bulunamadı, güncelleme başarısız")
    else:
        log.info(f"FUARLAR array güncellendi ({n} eşleşme)")

    if dry_run:
        log.info(f"[DRY-RUN] index.html yazılmadı ({html_path})")
        return

    html_path.write_text(new_html, encoding="utf-8")
    log.info(f"index.html yazıldı → {html_path}")


# ---------------------------------------------------------------------------
# Adım 6: ETL log kaydı
# ---------------------------------------------------------------------------

def write_etl_log(conn, durum: str, etkilenen: int, cikti: str,
                  hata: str | None, sure_ms: int) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_etl_log
                    (durum, etkilenen_satir, cikti_dosya, hata_mesaji, sure_ms)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (durum, etkilenen, cikti, hata, sure_ms)
            )
        conn.commit()
        log.info(f"ETL log kaydedildi: {durum} / {sure_ms} ms")
    except psycopg2.Error as e:
        log.warning(f"ETL log yazılamadı: {e}")


# ---------------------------------------------------------------------------
# Ana akış
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="CNG Expo Pipeline ETL")
    parser.add_argument("--dry-run",   action="store_true", help="Dosya yazmadan çalış")
    parser.add_argument("--no-count",  action="store_true", help="COUNT sorgularını atla")
    args = parser.parse_args()

    t0 = time.perf_counter()
    log.info("=" * 60)
    log.info("CNG Expo Pipeline ETL başladı")
    log.info(f"  dry_run  : {args.dry_run}")
    log.info(f"  no_count : {args.no_count}")
    log.info("=" * 60)

    conn = None
    hata_mesaji = None
    etkilenen = 0

    try:
        # 1. Bağlan
        conn = get_conn()
        log.info("PostgreSQL bağlantısı başarılı")

        # 2. Ana veriyi çek
        rows = fetch_pipeline_rows(conn)

        # 3. Gerçek COUNT'ları al (isteğe bağlı)
        if not args.no_count:
            rows = refresh_counts(conn, rows)
        else:
            log.info("--no-count: COUNT sorguları atlandı")

        # 4. KPI toplamları
        kpi = compute_kpis(rows)

        # 5. JSON payload'ı oluştur
        payload = build_json_payload(rows, kpi)

        # 6. data.json yaz
        write_json(payload, DATA_JSON_PATH, dry_run=args.dry_run)

        # 7. index.html güncelle
        update_html(payload, INDEX_HTML_PATH, dry_run=args.dry_run)

        etkilenen = len(rows)
        log.info("ETL başarıyla tamamlandı")

    except psycopg2.OperationalError as e:
        hata_mesaji = f"DB bağlantı hatası: {e}"
        log.error(hata_mesaji)
        sys.exit(1)

    except Exception as e:
        hata_mesaji = f"Beklenmeyen hata: {type(e).__name__}: {e}"
        log.error(hata_mesaji, exc_info=True)
        sys.exit(1)

    finally:
        sure_ms = int((time.perf_counter() - t0) * 1000)
        durum   = "HATA" if hata_mesaji else "BASARILI"

        if conn and not conn.closed:
            if not args.dry_run:
                write_etl_log(
                    conn, durum, etkilenen,
                    str(DATA_JSON_PATH), hata_mesaji, sure_ms
                )
            conn.close()
            log.info("DB bağlantısı kapatıldı")

        log.info(f"Toplam süre: {sure_ms} ms")
        log.info("=" * 60)


if __name__ == "__main__":
    main()
