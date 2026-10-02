"""
Mevcut kişileri güncel kurala göre AYIRIR (kullanıcı ilkesi 02.10: farklı kurumsal alan adı = farklı kişi).

kisi_master/kisi_kimlik daha eski (gevşek) kurallarla yüklendi. Güncel kural motoru (kisi_tekillestir.py)
bazı mevcut kişileri artık tek kişi saymıyor. Bu betik:
  - bir kişinin e-postaları motorda birden fazla bileşene düşüyorsa, birincil e-postanın bileşeni orijinal
    kisi_id'de KALIR; diğer bileşenler YENİ kisi_id alır (kimlik satırları taşınır);
  - tüm kişilerin baglanma_kurali etiketlerini güncel kenarlara göre yeniden yazar;
  - iki mevcut kişiyi BİRLEŞTİRMEZ (çakışma yalnız raporlanır; birleştirme kisi_birlestir.py ile, onayla).
temas tablosu boşken güvenlidir (temas.kisi_id taşıması gerekirse betik durur).

Kullanım:
  python kisi_ayir.py --hesapla   # SALT OKUNUR
  python kisi_ayir.py             # DRY-RUN (yazar, ROLLBACK)
  python kisi_ayir.py --uygula    # COMMIT (kullanıcı çalıştırır)
"""
import argparse
import collections
import sys

from psycopg2.extras import execute_values

import kisi_tekillestir as K
from kisi_kapsam_genislet import topla
from kisi_master_doldur import baglan, firma_norm
from kisi_sema_guncelle import genel_kutu_mu


def hesapla(conn):
    tablolar = K.kampanya_tablolari(conn) + sorted(K.EK_KAYNAK)
    r = K.benzersiz_kisi_say(conn, tablolar=tablolar)
    uf = r["uf"]
    isim_say, firma_say, tablo_say = topla(conn, tablolar)
    with conn.cursor() as cur:
        cur.execute("SELECT kisi_id, birincil_email, isim, firma, kayit_tipi FROM kisi_master")
        master = {k: (e, n, f, t) for k, e, n, f, t in cur.fetchall()}
        cur.execute("SELECT deger, kisi_id, baglanma_kurali FROM kisi_kimlik WHERE tip = 'email'")
        kimlik = {d: (k, b) for d, k, b in cur.fetchall()}
        cur.execute("SELECT coalesce(max(kisi_id), 0) FROM kisi_master")
        max_id = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM temas")
        temas = cur.fetchone()[0]

    komsu = collections.defaultdict(list)
    for a, b, kural in r["kenarlar"]:
        komsu[a].append((b, kural))
        komsu[b].append((a, kural))

    def bfs(kok):
        etiket = {kok: "birincil"}
        kuyruk = collections.deque([kok])
        while kuyruk:
            x = kuyruk.popleft()
            for y, kr in komsu[x]:
                if y not in etiket:
                    etiket[y] = kr
                    kuyruk.append(y)
        return etiket

    def en_sik(sayaclar):
        toplam = collections.Counter()
        for c in sayaclar:
            toplam.update(c)
        return sorted(toplam.items(), key=lambda kv: (-kv[1], kv[0].isupper(), kv[0]))[0][0] if toplam else None

    def puan(em, inorm):
        sinif = K.eposta_sinifi(em, inorm)
        serbest = K.serbest_mi(em.split("@")[-1])
        return (tuple(-x for x in ({"ad": 2, "genel": 1, "baska": 0}[sinif] * 2 + (0 if serbest else 1),
                                   sum(tablo_say[em].values()))), em)

    kisi_mail = collections.defaultdict(list)
    for d, (k, _) in kimlik.items():
        kisi_mail[k].append(d)

    yeni_kisiler, yeni_etiket = [], {}
    for kid, (birincil, isim, firma, tip) in master.items():
        mailler = [e for e in kisi_mail[kid] if e in uf.p]
        bilesen = collections.defaultdict(list)
        for e in mailler:
            bilesen[uf.find(e)].append(e)
        ana = uf.find(birincil) if birincil in uf.p else None
        for kok, uyeler in bilesen.items():
            if kok == ana:
                continue
            ad = en_sik([isim_say[e] for e in uyeler])
            fr = en_sik([firma_say[e] for e in uyeler])
            inorm = K.norm_isim(ad) if ad else None
            yb = sorted(uyeler, key=lambda e: puan(e, inorm))[0]
            yeni_kisiler.append(dict(birincil=yb, isim=ad, firma=fr, firma_norm=firma_norm(fr),
                                     domain=yb.split("@")[-1], mailler=sorted(uyeler),
                                     tip="genel_kutu" if (not ad and genel_kutu_mu(yb)) else "kisi", eski=kid))
        # etiketler: birincilin bileşeni için güncel kenarlardan
        if ana is not None:
            et = bfs(birincil)
            for e in bilesen[ana]:
                yeni_etiket[e] = et.get(e, "R-a")
    for k in yeni_kisiler:
        et = bfs(k["birincil"])
        for e in k["mailler"]:
            yeni_etiket[e] = et.get(e, "R-a")

    # bölünmeyen kişiler dahil etiket farkları
    etiket_degisen = [(e, yeni_etiket[e]) for e, (kid, b) in kimlik.items()
                      if e in yeni_etiket and yeni_etiket[e] != b and e not in {m for k in yeni_kisiler for m in k["mailler"]}]
    # çakışma (bilgi): bir bileşen >=2 mevcut kişi içeriyor
    kume_kisi = collections.defaultdict(set)
    for e, (kid, _) in kimlik.items():
        if e in uf.p:
            kume_kisi[uf.find(e)].add(kid)
    cakisma = sum(1 for s in kume_kisi.values() if len(s) >= 2)
    rap = dict(kisi=len(master), kimlik=len(kimlik), max_id=max_id, temas=temas,
               yeni_kisi=len(yeni_kisiler), tasinan_email=sum(len(k["mailler"]) for k in yeni_kisiler),
               etiket_degisen=len(etiket_degisen), cakisma=cakisma,
               yeni_genel=sum(1 for k in yeni_kisiler if k["tip"] == "genel_kutu"),
               kuralsiz=sum(1 for e in kimlik if e not in yeni_etiket))
    etiket_yaz = dict(etiket_degisen)
    for k in yeni_kisiler:
        for e in k["mailler"]:
            etiket_yaz[e] = yeni_etiket[e]
    return rap, yeni_kisiler, etiket_yaz


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hesapla", action="store_true")
    ap.add_argument("--uygula", action="store_true")
    args = ap.parse_args()
    conn = baglan()
    if args.hesapla:
        conn.set_session(readonly=True)
    rap, yeni_kisiler, etiket_yaz = hesapla(conn)
    n = lambda x: f"{x:,}".replace(",", ".")
    print(f"Mevcut: kisi_master {n(rap['kisi'])} | kisi_kimlik {n(rap['kimlik'])} | temas {rap['temas']}")
    print(f"AYRILACAK: {n(rap['yeni_kisi'])} yeni kisi_id ({n(rap['tasinan_email'])} e-posta taşınır; genel_kutu {rap['yeni_genel']})")
    print(f"baglanma_kurali güncellenecek (ayrılmayan kişilerde): {n(rap['etiket_degisen'])}")
    print(f"Çakışma (>=2 mevcut kişi aynı bileşende; DOKUNULMAZ): {n(rap['cakisma'])} | kuralı bulunamayan e-posta: {rap['kuralsiz']}")
    if rap["temas"]:
        sys.exit("DURDU: temas boş değil; kisi_id taşıma ayrıca ele alınmalı. Yazılmadı.")
    if rap["kuralsiz"]:
        sys.exit("DURDU: bazı e-postalar için kural bulunamadı. Yazılmadı.")
    if args.hesapla:
        print("Salt okunur hesap tamam; DB'ye yazılmadı.")
        return

    with conn.cursor() as cur:
        bas = rap["max_id"] + 1
        master = [(bas + i, k["birincil"], k["isim"], k["firma"], k["firma_norm"], k["domain"], k["tip"])
                  for i, k in enumerate(yeni_kisiler)]
        execute_values(cur, "INSERT INTO kisi_master (kisi_id, birincil_email, isim, firma, firma_norm, domain, kayit_tipi) VALUES %s",
                       master, page_size=5000)
        tasi = [(bas + i, e) for i, k in enumerate(yeni_kisiler) for e in k["mailler"]]
        execute_values(cur, "UPDATE kisi_kimlik c SET kisi_id = v.k FROM (VALUES %s) v(k, e) "
                            "WHERE c.tip = 'email' AND c.deger = v.e", tasi, page_size=5000)
        execute_values(cur, "UPDATE kisi_kimlik c SET baglanma_kurali = v.k FROM (VALUES %s) v(e, k) "
                            "WHERE c.tip = 'email' AND c.deger = v.e", list(etiket_yaz.items()), page_size=5000)
        cur.execute("SELECT count(*) FROM (SELECT kisi_id FROM kisi_kimlik GROUP BY 1 HAVING count(*) FILTER "
                    "(WHERE baglanma_kurali = 'birincil') <> 1) x")
        print("tam 1 birincil olmayan kişi (0 olmalı):", cur.fetchone()[0])
        cur.execute("SELECT count(*) FROM kisi_master m WHERE NOT EXISTS (SELECT 1 FROM kisi_kimlik k "
                    "WHERE k.kisi_id = m.kisi_id AND k.deger = m.birincil_email AND k.baglanma_kurali = 'birincil')")
        print("birincil_email'i 'birincil' etiketli olmayan kişi (0 olmalı):", cur.fetchone()[0])
        print("Yazıldı (transaction içinde): ", end="")
        cur.execute("SELECT (SELECT count(*) FROM kisi_master), (SELECT count(*) FROM kisi_kimlik)")
        print("kisi_master=%s kisi_kimlik=%s" % cur.fetchone())
        if args.uygula:
            cur.execute("SELECT setval('kisi_master_kisi_id_seq', (SELECT max(kisi_id) FROM kisi_master))")
            conn.commit()
            print("COMMIT edildi.")
        else:
            conn.rollback()
            print("DRY-RUN: ROLLBACK yapıldı, kalıcı değişiklik yok.")


if __name__ == "__main__":
    main()
