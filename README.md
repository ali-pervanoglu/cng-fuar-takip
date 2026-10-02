[🇬🇧 English](#english) | [🇹🇷 Türkçe](#türkçe)

---

## English

# Trade Fair Campaign Pipeline Tracker

A live dashboard tracking multi-stage data pipelines for B2B trade fair campaign operations.

### What it does
- Tracks 34 pipeline cards (fairs × periods × segments) across six ETL stages
- Covers 150K+ unique contacts (computed live as `meta.gercek_benzersiz_kisi`)
- Monitors pipeline progress from data cleaning to campaign-ready output
- Auto-refreshes every 10 minutes via a scheduled ETL script (pushes only when data changed)
- Deployed on GitHub Pages

### Tech Stack
Python · PostgreSQL · HTML · JavaScript · CSS · GitHub Pages · Windows Task Scheduler

### Note
Built for a live production environment. Source data and internal pipeline details are not included in this repository.

---

## Türkçe

# Fuar Kampanya Pipeline Takip Paneli

B2B fuar kampanya operasyonları için çok aşamalı veri pipeline'larını izleyen canlı bir dashboard.

### Ne yapar?
- 34 pipeline kartını (fuar × dönem × segment) altı ETL aşamasında takip eder
- 150.000+ benzersiz kişiyi kapsar (canlı hesaplanır: `meta.gercek_benzersiz_kisi`)
- Veri temizlemeden kampanyaya hazır çıktıya kadar tüm süreci izler
- ETL scripti ile her 10 dakikada otomatik güncellenir (yalnız veri değiştiyse push eder)
- GitHub Pages üzerinde yayınlanmaktadır

### Teknolojiler
Python · PostgreSQL · HTML · JavaScript · CSS · GitHub Pages · Windows Task Scheduler

### Not
Canlı üretim ortamı için geliştirilmiştir. Kaynak veriler ve iç pipeline detayları bu repoda yer almamaktadır.
