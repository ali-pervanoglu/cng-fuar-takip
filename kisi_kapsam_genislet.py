"""
kisi_master / kisi_kimlik kapsam genişletme (ARTIMLI yükleme, ilk sürüm).

Kampanya listesi dışındaki kaynakların (K.EK_KAYNAK: fuar kayıtları, CRM, kargo listesi ...) e-postalarını
kişi boyutuna ekler. Mevcut kisi_id'ler DEĞİŞMEZ.

Her bileşen (aynı kişi olan e-posta grubu) için:
  - mevcutta kisi_id yok          -> YENİ kişi (kisi_master + kisi_kimlik)
  - mevcutta tam 1 kisi_id var    -> yeni e-postalar o kişiye EKLENİR (baglanma_kurali ile)
  - mevcutta >=2 kisi_id var      -> ÇAKIŞMA: dokunulmaz, raporlanır (iki mevcut kişi birleşmez)
Mevcut kişinin boş isim/firma alanı yeni kaynaktan DOLDURULUR (dolu alan değişmez);
isim dolunca genel_kutu -> kisi olur.

Kullanım:
  python kisi_kapsam_genislet.py --hesapla   # SALT OKUNUR
  python kisi_kapsam_genislet.py             # DRY-RUN (yazar, ROLLBACK)
  python kisi_kapsam_genislet.py --uygula    # COMMIT (kullanıcı çalıştırır)
Parola: PGPASSWORD (ya da DB_PASSWORD).
"""
import argparse
import collections
import sys

from psycopg2.extras import execute_values

import kisi_tekillestir as K
from kisi_master_doldur import baglan, firma_norm, temiz_ad
from kisi_sema_guncelle import genel_kutu_mu


def topla(conn, tablolar):
    """E-posta başına ham isim/firma sayaçları ve kaynak tablo sayaçları (EK_KAYNAK eşlemesiyle)."""
    isim = collections.defaultdict(collections.Counter)
    firma = collections.defaultdict(collections.Counter)
    tablo = collections.defaultdict(collections.Counter)
    with conn.cursor() as cur:
        for t in tablolar:
            al = K.tablo_alanlari(K._kolonlar(cur, t), t)
            kosul = "email IS NOT NULL AND trim(email) <> ''" + (f" AND {al['filtre']}" if al.get("filtre") else "")
            cur.execute(f"SELECT lower(trim(email)), {al['isim']}, {al['firma']} FROM {t} WHERE {kosul}")
            for em, a, f in cur.fetchall():
                tablo[em][t] += 1
                a, f = temiz_ad(a), temiz_ad(f)
                if a:
                    isim[em][a] += 1
                if f:
                    firma[em][f] += 1
    return isim, firma, tablo


def hesapla(conn):
    tablolar = K.kampanya_tablolari(conn) + sorted(K.EK_KAYNAK)
    r = K.benzersiz_kisi_say(conn, tablolar=tablolar)
    uf = r["uf"]
    isim_say, firma_say, tablo_say = topla(conn, tablolar)
    with conn.cursor() as cur:
        cur.execute("SELECT kisi_id, birincil_email, isim, firma, kayit_tipi FROM kisi_master")
        master = {k: (e, n, f, t) for k, e, n, f, t in cur.fetchall()}
        cur.execute("SELECT deger, kisi_id FROM kisi_kimlik WHERE tip = 'email'")
        mail_kisi = dict(cur.fetchall())
        cur.execute("SELECT coalesce(max(kisi_id), 0) FROM kisi_master")
        max_id = cur.fetchone()[0]

    komsu = collections.defaultdict(list)
    for a, b, kural in r["kenarlar"]:
        komsu[a].append((b, kural))
        komsu[b].append((a, kural))

    kume = collections.defaultdict(list)
    for em in uf.p:
        kume[uf.find(em)].append(em)

    def bfs(kokler):
        etiket = {k: None for k in kokler}
        kuyruk = collections.deque(kokler)
        while kuyruk:
            x = kuyruk.popleft()
            for y, kr in komsu[x]:
                if y not in etiket:
                    etiket[y] = kr
                    kuyruk.append(y)
        return etiket

    def puan(em, inorm):
        sinif = K.eposta_sinifi(em, inorm)
        serbest = em.split("@")[-1] in K.SERBEST_ALAN
        return (tuple(-x for x in ({"ad": 2, "genel": 1, "baska": 0}[sinif] * 2 + (0 if serbest else 1),
                                   sum(tablo_say[em].values()))), em)

    def en_sik(say_listesi):
        toplam = collections.Counter()
        for c in say_listesi:
            toplam.update(c)
        return sorted(toplam.items(), key=lambda kv: (-kv[1], kv[0].isupper(), kv[0]))[0][0] if toplam else None

    yeni_kisi, ekleme, doldur, cakisma = [], [], [], []
    for uyeler in kume.values():
        ids = {mail_kisi[e] for e in uyeler if e in mail_kisi}
        yeni_mailler = [e for e in uyeler if e not in mail_kisi]
        if len(ids) >= 2:
            cakisma.append((sorted(ids), len(yeni_mailler)))
            continue
        isim = en_sik([isim_say[e] for e in uyeler])
        firma = en_sik([firma_say[e] for e in uyeler])
        if not ids:                                              # YENİ kişi
            inorm = K.norm_isim(isim) if isim else None
            birincil = sorted(uyeler, key=lambda e: puan(e, inorm))[0]
            etiket = bfs([birincil])
            tip = "genel_kutu" if (not isim and genel_kutu_mu(birincil)) else "kisi"
            yeni_kisi.append(dict(birincil=birincil, isim=isim, firma=firma, firma_norm=firma_norm(firma),
                                  domain=birincil.split("@")[-1], tip=tip,
                                  mailler=[(e, "birincil" if e == birincil else etiket.get(e) or "R-a",
                                            tablo_say[e].most_common(1)[0][0]) for e in sorted(uyeler)]))
        else:                                                    # mevcut kişiye ekleme
            kid = next(iter(ids))
            kokler = [e for e in uyeler if e in mail_kisi]
            birincil = master[kid][0]
            if birincil in uyeler:
                kokler = [birincil]
            etiket = bfs(kokler)
            if yeni_mailler:
                ekleme.append((kid, [(e, etiket.get(e) or "R-a", tablo_say[e].most_common(1)[0][0])
                                     for e in sorted(yeni_mailler)]))
            m_e, m_isim, m_firma, m_tip = master[kid]
            if (m_isim is None and isim) or (m_firma is None and firma):
                doldur.append((kid, isim if m_isim is None else None, firma if m_firma is None else None,
                               "kisi" if (m_isim is None and isim and m_tip == "genel_kutu") else None))

    # bölünme: mevcut bir kişinin e-postaları artık birden fazla bileşende mi?
    kisi_kume = collections.defaultdict(set)
    for em, kid in mail_kisi.items():
        if em in uf.p:
            kisi_kume[kid].add(uf.find(em))
    bolunen = [kid for kid, s in kisi_kume.items() if len(s) > 1]
    yeni_email_toplam = sum(1 for e in uf.p if e not in mail_kisi)
    rapor = dict(tablo=len(tablolar), email=len(uf.p), yeni_email=yeni_email_toplam, kisi_motor=r["kisi"],
                 yeni_kisi=len(yeni_kisi), yeni_kisi_email=sum(len(k["mailler"]) for k in yeni_kisi),
                 ekleme_kisi=len(ekleme), ekleme_email=sum(len(x[1]) for x in ekleme),
                 cakisma=len(cakisma), cakisma_yeni_email=sum(x[1] for x in cakisma), bolunen=len(bolunen),
                 doldur=len(doldur), doldur_tip=sum(1 for d in doldur if d[3]), max_id=max_id,
                 yeni_genel=sum(1 for k in yeni_kisi if k["tip"] == "genel_kutu"))
    return rapor, yeni_kisi, ekleme, doldur, cakisma, bolunen


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hesapla", action="store_true", help="salt okunur: yalnız hesapla")
    ap.add_argument("--uygula", action="store_true", help="COMMIT et (varsayılan: ROLLBACK'li dry-run)")
    args = ap.parse_args()
    conn = baglan()
    if args.hesapla:
        conn.set_session(readonly=True)
    rap, yeni_kisi, ekleme, doldur, cakisma, bolunen = hesapla(conn)
    n = lambda x: f"{x:,}".replace(",", ".")
    print(f"Kaynak: {rap['tablo']} tablo | e-posta {n(rap['email'])} (kimlikte olmayan: {n(rap['yeni_email'])}) | motor kişi sayısı {n(rap['kisi_motor'])}")
    print(f"YENİ kişi: {n(rap['yeni_kisi'])} ({n(rap['yeni_kisi_email'])} e-posta; bunlardan genel_kutu {n(rap['yeni_genel'])})")
    print(f"MEVCUT kişiye eklenen: {n(rap['ekleme_kisi'])} kişi / {n(rap['ekleme_email'])} yeni e-posta")
    print(f"ÇAKIŞMA (>=2 mevcut kişi, dokunulmadı): {n(rap['cakisma'])} bileşen / {n(rap['cakisma_yeni_email'])} yeni e-posta atlandı")
    print(f"Bölünen mevcut kişi (bilgi; değiştirilmez): {n(rap['bolunen'])}")
    print(f"Boş isim/firma doldurulacak mevcut kişi: {n(rap['doldur'])} (genel_kutu -> kisi: {n(rap['doldur_tip'])})")
    beklenen = rap["yeni_kisi_email"] + rap["ekleme_email"] + rap["cakisma_yeni_email"]
    print(f"Kontrol: yerleşen+atlanan yeni e-posta {n(beklenen)} = kimlikte olmayan {n(rap['yeni_email'])} -> {'OK' if beklenen == rap['yeni_email'] else 'UYUŞMUYOR'}")
    if beklenen != rap["yeni_email"]:
        sys.exit("DURDU: sayılar tutmuyor. Yazılmadı.")
    if args.hesapla:
        print("Salt okunur hesap tamam; DB'ye yazılmadı.")
        return

    with conn.cursor() as cur:
        bas = rap["max_id"] + 1
        master = [(bas + i, k["birincil"], k["isim"], k["firma"], k["firma_norm"], k["domain"], k["tip"])
                  for i, k in enumerate(yeni_kisi)]
        execute_values(cur, "INSERT INTO kisi_master (kisi_id, birincil_email, isim, firma, firma_norm, domain, kayit_tipi) VALUES %s",
                       master, page_size=5000)
        kimlik = [(bas + i, "email", e, kt, kr) for i, k in enumerate(yeni_kisi) for e, kr, kt in k["mailler"]]
        kimlik += [(kid, "email", e, kt, kr) for kid, ms in ekleme for e, kr, kt in ms]
        execute_values(cur, "INSERT INTO kisi_kimlik (kisi_id, tip, deger, kaynak_tablo, baglanma_kurali) VALUES %s",
                       kimlik, page_size=5000)
        if doldur:
            execute_values(cur, "UPDATE kisi_master m SET isim = coalesce(m.isim, v.isim), firma = coalesce(m.firma, v.firma), "
                                "firma_norm = coalesce(m.firma_norm, v.fn), kayit_tipi = coalesce(v.tip, m.kayit_tipi), "
                                "guncelleme_ts = now() FROM (VALUES %s) v(kid, isim, firma, fn, tip) WHERE m.kisi_id = v.kid",
                           [(kid, i, f, firma_norm(f), t) for kid, i, f, t in doldur], page_size=5000)
        cur.execute("SELECT (SELECT count(*) FROM kisi_master), (SELECT count(*) FROM kisi_kimlik), "
                    "(SELECT count(DISTINCT deger) FROM kisi_kimlik WHERE tip='email')")
        print("Yazıldı (transaction içinde): kisi_master=%s, kisi_kimlik=%s, tekil e-posta=%s" % cur.fetchone())
        cur.execute("SELECT kayit_tipi, count(*) FROM kisi_master GROUP BY 1 ORDER BY 1")
        print("kayit_tipi:", cur.fetchall())
        cur.execute("SELECT count(*) FROM (SELECT kisi_id FROM kisi_kimlik GROUP BY 1 HAVING count(*) FILTER "
                    "(WHERE baglanma_kurali = 'birincil') <> 1) x")
        print("tam 1 birincil olmayan kişi (0 olmalı):", cur.fetchone()[0])
        if args.uygula:
            cur.execute("SELECT setval('kisi_master_kisi_id_seq', (SELECT max(kisi_id) FROM kisi_master))")
            conn.commit()
            print("COMMIT edildi.")
        else:
            conn.rollback()
            print("DRY-RUN: ROLLBACK yapıldı, kalıcı değişiklik yok.")


if __name__ == "__main__":
    main()
