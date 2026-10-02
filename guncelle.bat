@echo off
chcp 65001 >nul
title CNG Expo Pipeline

cd /d "C:\Users\ali.pervanoglu\Downloads\Fuar Pipeline ETL Projesi"

echo [1/2] ETL calistiriliyor...
python etl_pipeline.py --no-count

echo [2/2] GitHub push...
git add data.json index.html
git commit -m "ETL guncelleme %date%"
git push origin main

echo.
echo TAMAMLANDI! Dashboard guncellendi.
echo https://ali-pervanoglu.github.io/cng-fuar-takip/
