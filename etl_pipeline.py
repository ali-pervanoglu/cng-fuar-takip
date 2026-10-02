#!/usr/bin/env python3
# =============================================================================
#  CNG Expo — Pipeline ETL Script
#  Versiyon : 2.0  (2026-08) — segment_kodu / kaynak_tipi / gerçek tarih +
#             otomatik GitHub push (Task Scheduler ile 10 dk'da bir çalışır,
#             artık guncelle.bat'a manuel basmaya gerek yok)
#
#  Görev    :
#    1. PostgreSQL v_pipeline_ozet view'ından tüm satırları çeker
#       (artık segment_kodu, kaynak_tipi, fuar_tarihi_baslangic/bitis dahil).
#    2. kaynak_tablo tanımlı satırlar için COUNT(*) ile gerçek satır sayısını günceller.
#    3. data.json dosyasını fuar/dönem/segment yapısında üretir.
#    4. Değişiklik varsa git add + commit + push yapar (GitHub Pages otomatik yayınlar).
#    5. pipeline_etl_log tablosuna çalışma kaydı bırakır.
#
#  Kullanım :
#    python etl_pipeline.py                 # Normal çalıştır + push
#    python etl_pipeline.py --dry-run       # DB'yi okur, dosya/push YAPMAZ (test)
#    python etl_pipeline.py --no-count      # COUNT sorgularını atla (hızlı mod)
#    python etl_pipeline.py --no-push       # data.json'ı yazar ama GitHub'a göndermez
#
#  Task Scheduler kurulumu için bkz. dosya sonundaki not.
# =============================================================================

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

BASE_DIR   = Path(__file__).parent
OUTPUT_DIR = BASE_DIR
LOG_DIR    = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

DATA_JSON_PATH = OUTPUT_DIR / "data.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        # Task Scheduler her 10 dakikada bir çalıştıracağı için log dosyası
        # günlük değil — tek dosyada birikir, boyutu Task Scheduler'ın
        # "sonraki çalışmaya kadar bekleme" ayarıyla makul kalır.
        logging.FileHandler(LOG_DIR / f"etl_{datetime.now():%Y%m%d}.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("cng_etl")


def get_conn():
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
# Adım 1: v_pipeline_ozet'ten okuma (yeni kolonlar dahil)
# ---------------------------------------------------------------------------

def fetch_pipeline_rows(conn) -> list[dict]:
    sql = """
        SELECT
            fuar_id, fuar_kod, fuar_ad, fuar_alt, fuar_tarih,
            segment_kodu, kaynak_tipi, fuar_tarihi_baslangic, fuar_tarihi_bitis,
            kaynak_tablo, kaynak_filtre, sira,
            temizlik_durum, db_durum, mx_durum, mev_durum,
            email_durum, sms_durum,
            kayit_sayisi, email_gonder_sayisi, sms_gonder_sayisi,
            email_not, genel_not,
            tamamlanma_pct,
            guncelleme_ts,
            hedef_fuar_ad
        FROM v_pipeline_ozet
        ORDER BY fuar_tarihi_baslangic NULLS LAST, sira, fuar_id
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        rows = [dict(r) for r in cur.fetchall()]
    log.info(f"v_pipeline_ozet: {len(rows)} satır okundu")
    return rows


# ---------------------------------------------------------------------------
# Adım 2: Gerçek COUNT sorgularıyla satır sayılarını güncelle
# ---------------------------------------------------------------------------

def refresh_counts(conn, rows: list[dict]) -> list[dict]:
    updated = 0
    with conn.cursor() as cur:
        for row in rows:
            tbl    = row.get("kaynak_tablo")
            filtre = row.get("kaynak_filtre")
            if not tbl:
                continue
            if tbl in KPI_OZET_VIEWS:
                continue  # bu view'lar enrich_kpi_rows() tarafından ayrıca işlenir
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
            except psycopg2.Error as e:
                log.warning(f"  {row['fuar_kod']}: COUNT sorgusu başarısız — {e}")
                conn.rollback()

    conn.commit()
    log.info(f"refresh_counts tamamlandı: {updated} tablo güncellendi")
    return rows


# ---------------------------------------------------------------------------
# Adım 2.5: KPI aggregate view'larından gerçek metrikleri çek
# ---------------------------------------------------------------------------

# kaynak_tablo bir "*_kpi_ozet" view'ı ise, oradan tek satır çekip
# kpi_json'a dolduruyoruz. Yeni bir fuar için KPI view'ı eklenince
# buraya bir satır eklemek yeterli — kod tarafında başka değişiklik gerekmez.
KPI_OZET_VIEWS = {
    "v_aysaf_2026_kpi_ozet":         {"has_organik": True},
    "v_paintistanbul_2026_kpi_ozet": {"has_organik": True},
}


def enrich_kpi_rows(conn, rows: list[dict]) -> list[dict]:
    enriched = 0
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        for row in rows:
            if row.get("kaynak_tipi") != "KPI":
                continue
            tbl = row.get("kaynak_tablo")
            if tbl not in KPI_OZET_VIEWS:
                continue
            try:
                cur.execute(f"SELECT * FROM {tbl}")
                agg = cur.fetchone()
                if not agg:
                    continue
                toplam = agg.get("toplam_kayit") or 0
                geldi  = agg.get("geldi_toplam") or 0
                row["kayit_sayisi"] = toplam
                row["email_not"]    = f"Geldi: {geldi:,}".replace(",", ".")
                row["kpi_json"] = {
                    "donusum_pct":         float(agg["donusum_pct"]) if agg.get("donusum_pct") is not None else None,
                    "sicak_lead":          agg.get("sicak_lead") or 0,
                    "ab_sinifi_lead":      agg.get("ab_sinifi_lead") or 0,
                    "cok_gunlu_ziyaretci": agg.get("cok_gunlu_ziyaretci") or 0,
                    "geldi_ziyaretci":     geldi,
                    "geldi_katilimci":     None,  # bu view'larda ziyaretçi/katılımcı ayrımı yok — 0 değil, "takip edilmiyor"
                    # Kampanya listesinde HİÇ olmayıp fuara gelen kişi sayısı.
                    # Marketing kartındaki "gerçek benzersiz toplam" hesabı
                    # bunu Kampanya toplamına ekler — çift sayım yapmadan.
                    "organik_geldi":       agg.get("organik_geldi") or 0,
                }
                if KPI_OZET_VIEWS[tbl]["has_organik"] and agg.get("organik_oran") is not None:
                    row["kpi_json"]["organik_oran"] = float(agg["organik_oran"])
                enriched += 1
            except psycopg2.Error as e:
                log.warning(f"  {row['fuar_kod']}: KPI aggregate sorgusu başarısız ({tbl}) — {e}")
                conn.rollback()

    log.info(f"enrich_kpi_rows tamamlandı: {enriched} KPI satırı zenginleştirildi")
    return rows


# ---------------------------------------------------------------------------
# Adım 2.6: Gerçek benzersiz kişi sayısı (email bazlı dedup, tüm Kampanya
# tabloları birleştirilerek). Tablo listesi pipeline_fuar_meta'dan DİNAMİK
# çekiliyor — yeni bir fuar/kaynak tablosu eklendiğinde bu fonksiyona
# dokunmaya gerek yok, otomatik dahil olur.
# ---------------------------------------------------------------------------

def compute_unique_person_count(conn) -> int | None:
    """Kişi bazlı benzersiz sayı (kisi_tekillestir.py kuralı); modül hata verirse e-posta bazlı sayıma düşer."""
    try:
        from kisi_tekillestir import benzersiz_kisi_say
        r = benzersiz_kisi_say(conn)
        log.info(
            f"compute_unique_person_count: {r['tablo']} tablo, e-posta bazlı {r['email_tekil']:,} -> "
            f"kişi bazlı {r['kisi']:,} (birleşen {r['birlesen']:,}, toplu kayıt {r['toplu_grup']:,} grup, "
            f"belirsiz {r['belirsiz']:,})".replace(",", ".")
        )
        return r["kisi"]
    except Exception as e:  # noqa: BLE001 - dashboard sayısı yüzünden ETL durmamalı
        log.warning(f"kisi_tekillestir başarısız ({e}); e-posta bazlı sayıma dönülüyor")
        conn.rollback()
        return compute_unique_person_count_email(conn)


def compute_unique_person_count_email(conn) -> int | None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT kaynak_tablo
            FROM pipeline_fuar_meta
            WHERE aktif = true AND kaynak_tipi = 'Kampanya' AND kaynak_tablo IS NOT NULL
        """)
        tablolar = [r[0] for r in cur.fetchall()]

    # Güvenlik: yalnızca güvenli tablo adı karakterlerine izin ver (SQL injection önlemi)
    tablolar = [t for t in tablolar if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', t)]
    if not tablolar:
        return None

    union_sql = " UNION ALL ".join(
        f"SELECT lower(trim(email)) AS email_norm FROM {t} WHERE email IS NOT NULL AND trim(email) <> ''"
        for t in tablolar
    )
    sql = f"SELECT COUNT(DISTINCT email_norm) FROM ({union_sql}) birlesik"

    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            sonuc = cur.fetchone()[0]
        log.info(f"compute_unique_person_count: {len(tablolar)} tablo birleştirildi, gerçek benzersiz kişi = {sonuc:,}".replace(",", "."))
        return sonuc
    except psycopg2.Error as e:
        log.warning(f"compute_unique_person_count başarısız: {e}")
        conn.rollback()
        return None


# ---------------------------------------------------------------------------
# Adım 3: data.json payload'ı (fuar/dönem/segment yapısı)
# ---------------------------------------------------------------------------

def kisi_tablosu_ozet(conn) -> dict | None:
    """kisi_master özeti: kişi / şirket kutusu (isimsiz genel kutu) sayısı ve son güncelleme. Tablo yoksa None."""
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT count(*) FILTER (WHERE kayit_tipi = 'kisi'),
                                  count(*) FILTER (WHERE kayit_tipi = 'genel_kutu'),
                                  greatest(max(guncelleme_ts), max(olusturma)) FROM kisi_master""")
            kisi, genel, ts = cur.fetchone()
        return {"kisi": kisi, "genel_kutu": genel, "guncelleme": ts.isoformat() if ts else None}
    except Exception as e:  # noqa: BLE001 - dashboard bu yüzden durmamalı
        log.warning(f"kisi_tablosu_ozet okunamadı: {e}")
        conn.rollback()
        return None


def build_json_payload(rows: list[dict], gercek_benzersiz_kisi: int | None = None,
                       kisi_tablosu: dict | None = None) -> dict:
    fuarlar = []
    for r in rows:
        kaynak_tipi = r["kaynak_tipi"] or "Kampanya"
        kpi = r.get("kpi_json") or {}
        # Kampanya satırları için email/sms gönderim sayıları pipeline_durum'da
        # doğrudan kolon olarak duruyor — kpi{} objesine burada aktarıyoruz.
        # (Daha önce bu adım hiç yapılmıyordu, Email/SMS Hazır kartları bu
        # yüzden her zaman 0 çıkıyordu.)
        if kaynak_tipi == "Kampanya":
            kpi = {
                "email_gonder": r.get("email_gonder_sayisi") or 0,
                "sms_gonder":   r.get("sms_gonder_sayisi") or 0,
            }
        fuarlar.append({
            "ad"                     : r["fuar_ad"],
            "segment_kodu"           : r["segment_kodu"] or "Genel",
            "kaynak_tipi"            : kaynak_tipi,
            "fuar_tarihi_baslangic"  : r["fuar_tarihi_baslangic"].isoformat() if r["fuar_tarihi_baslangic"] else None,
            "fuar_tarihi_bitis"      : r["fuar_tarihi_bitis"].isoformat() if r["fuar_tarihi_bitis"] else None,
            "t"     : r["temizlik_durum"],
            "db"    : r["db_durum"],
            "mx"    : r["mx_durum"],
            "mev"   : r["mev_durum"],
            "em"    : r["email_durum"],
            "sms"   : r["sms_durum"],
            "kayit" : r["kayit_sayisi"] or 0,
            "gonder": r["email_not"] or "",
            "pct"   : int(r["tamamlanma_pct"] or 0),
            "guncelleme_ts"     : r["guncelleme_ts"].isoformat() if r["guncelleme_ts"] else None,
            "kaynak_tablo"      : r["kaynak_tablo"],
            "veri_kaynagi_tipi" : r.get("veri_kaynagi_tipi"),
            "hedef_fuar"        : r.get("hedef_fuar_ad"),
            "kpi": kpi,
        })

    return {
        "meta"   : {
            "uretim_ts": datetime.now(timezone.utc).isoformat(),
            "gercek_benzersiz_kisi": gercek_benzersiz_kisi,
            "kisi_tablosu": kisi_tablosu,
        },
        "fuarlar": fuarlar,
    }


def write_json(payload: dict, path: Path, dry_run: bool = False) -> bool:
    """Yeni içerik eskisiyle aynıysa False döner (gereksiz commit önlenir)."""
    new_content = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False)

    if path.exists():
        old_content = path.read_text(encoding="utf-8")
        # 'uretim_ts' zaten her çalışmada değişir; onu çıkarıp gerçek veri
        # değişikliği var mı diye karşılaştır.
        old_no_ts = re.sub(r'"uretim_ts":\s*"[^"]*"', '"uretim_ts":""', old_content)
        new_no_ts = re.sub(r'"uretim_ts":\s*"[^"]*"', '"uretim_ts":""', new_content)
        if old_no_ts == new_no_ts:
            log.info("data.json içerik olarak değişmedi — yazma/push atlanıyor")
            return False

    if dry_run:
        log.info(f"[DRY-RUN] data.json yazılmadı ({path})")
        return True

    path.write_text(new_content, encoding="utf-8")
    log.info(f"data.json yazıldı → {path}  ({path.stat().st_size:,} byte)")
    return True


# ---------------------------------------------------------------------------
# Adım 4: Otomatik GitHub push
# ---------------------------------------------------------------------------

def git(*args, cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True)


def push_to_github(dry_run: bool = False, no_push: bool = False) -> None:
    if dry_run or no_push:
        log.info("[SKIP] GitHub push atlandı (dry-run veya --no-push)")
        return

    repo_dir = BASE_DIR

    status = git("status", "--porcelain", "data.json", cwd=repo_dir)
    if status.returncode != 0:
        log.warning(f"git status başarısız — bu dizin bir git repo mu? {status.stderr.strip()}")
        return
    if not status.stdout.strip():
        log.info("git: data.json'da değişiklik yok, push atlanıyor")
        return

    add = git("add", "data.json", cwd=repo_dir)
    if add.returncode != 0:
        log.warning(f"git add başarısız: {add.stderr.strip()}")
        return

    commit_msg = f"ETL: otomatik güncelleme {datetime.now():%Y-%m-%d %H:%M}"
    commit = git("commit", "-m", commit_msg, cwd=repo_dir)
    if commit.returncode != 0:
        log.warning(f"git commit başarısız: {commit.stderr.strip()}")
        return

    push = git("push", cwd=repo_dir)
    if push.returncode != 0:
        log.error(f"git push BAŞARISIZ — internet/kimlik doğrulama kontrol et: {push.stderr.strip()}")
        # Not: push başarısız olsa da commit lokalde durur, bir sonraki
        # başarılı çalışmada birlikte push edilir — veri kaybı olmaz.
        return

    log.info(f"git push başarılı — GitHub Pages birkaç dakika içinde güncellenecek")


# ---------------------------------------------------------------------------
# Adım 5: ETL log kaydı
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
    except psycopg2.Error as e:
        log.warning(f"ETL log yazılamadı: {e}")


# ---------------------------------------------------------------------------
# Ana akış
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="CNG Expo Pipeline ETL")
    parser.add_argument("--dry-run",  action="store_true", help="Dosya yazmadan / push etmeden çalış")
    parser.add_argument("--no-count", action="store_true", help="COUNT sorgularını atla")
    parser.add_argument("--no-push",  action="store_true", help="data.json yaz ama GitHub'a gönderme")
    args = parser.parse_args()

    t0 = time.perf_counter()
    log.info("=" * 60)
    log.info(f"CNG Expo Pipeline ETL başladı  dry_run={args.dry_run} no_count={args.no_count} no_push={args.no_push}")

    conn = None
    hata_mesaji = None
    etkilenen = 0

    try:
        conn = get_conn()
        rows = fetch_pipeline_rows(conn)

        if not args.no_count:
            rows = refresh_counts(conn, rows)

        rows = enrich_kpi_rows(conn, rows)

        gercek_benzersiz = compute_unique_person_count(conn)

        payload = build_json_payload(rows, gercek_benzersiz, kisi_tablosu_ozet(conn))
        changed = write_json(payload, DATA_JSON_PATH, dry_run=args.dry_run)

        if changed:
            push_to_github(dry_run=args.dry_run, no_push=args.no_push)

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
                write_etl_log(conn, durum, etkilenen, str(DATA_JSON_PATH), hata_mesaji, sure_ms)
            conn.close()
        log.info(f"Toplam süre: {sure_ms} ms")
        log.info("=" * 60)


if __name__ == "__main__":
    main()

# =============================================================================
#  TASK SCHEDULER KURULUMU — 10 dakikada bir otomatik çalıştırma
#  (manuel guncelle.bat'a artık gerek yok)
#
#  1) İlk seferlik: repo dizininde git kimlik doğrulaması bir kez ayarlanmalı
#     (SSH key ya da git credential manager ile, şifre her seferinde
#     sorulmayacak şekilde) — aksi halde otomatik push her çalışmada takılır.
#
#  2) PowerShell (Yönetici) ile görev oluştur:
#
#     $action  = New-ScheduledTaskAction -Execute "python.exe" `
#                  -Argument "C:\Users\ali.pervanoglu\Downloads\Fuar Pipeline ETL Projesi\etl_pipeline.py" `
#                  -WorkingDirectory "C:\Users\ali.pervanoglu\Downloads\Fuar Pipeline ETL Projesi"
#     $trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) `
#                  -RepetitionInterval (New-TimeSpan -Minutes 10) `
#                  -RepetitionDuration ([TimeSpan]::MaxValue)
#     $settings = New-ScheduledTaskSettingsSet -StartWhenAvailable `
#                  -ExecutionTimeLimit (New-TimeSpan -Minutes 5) -DontStopOnIdleEnd
#     Register-ScheduledTask -TaskName "CNG_Fuar_Pipeline_ETL" `
#                  -Action $action -Trigger $trigger -Settings $settings `
#                  -Description "Her 10 dakikada bir Postgres'i tarar, data.json'ı günceller ve GitHub'a push eder"
#
#  3) Doğrulama:
#     Get-ScheduledTask -TaskName "CNG_Fuar_Pipeline_ETL" | Get-ScheduledTaskInfo
#     -> LastTaskResult 0 olmalı (0 = başarılı)
#
#  4) logs/etl_YYYYMMDD.log dosyasını takip ederek her çalışmayı doğrula.
#     Script akıllı: veri değişmediyse dosya yazmaz / commit atmaz — yani
#     10 dakikada bir "boşuna" GitHub'a gitmiyor, sadece gerçek değişiklik
#     olduğunda push ediyor.
# =============================================================================
