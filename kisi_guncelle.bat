@echo off
chcp 65001 >nul
REM Kisi tablosunu yeni kayitlarla gunceller (artimli import) ve portal kisi_id'lerini yeniler.
REM   kisi_guncelle.bat           -> DRY-RUN (yazar, ROLLBACK eder; sayilari gosterir)
REM   kisi_guncelle.bat uygula    -> kalici yazar (COMMIT) + portal pipeline'i yeniden calistirir
REM Parola: PGPASSWORD ortam degiskeni (set PGPASSWORD=...) ile verilir.
cd /d "C:\Users\ali.pervanoglu\Downloads\Fuar Pipeline ETL Projesi"
if /i "%1"=="uygula" (
  python kisi_kapsam_genislet.py --uygula
  if errorlevel 1 exit /b 1
  cd /d "C:\Users\ali.pervanoglu\Downloads\portal_atif_paket_20260920_v2"
  python portal_atif_pipeline.py
) else (
  python kisi_kapsam_genislet.py
)
