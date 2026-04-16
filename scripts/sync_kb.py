"""
sync_kb.py — Sincronizza Google Sheet → MySQL + genera JSON statici
Eseguito da GitHub Actions ogni notte alle 2:00 UTC
"""

import os
import json
import pymysql
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date

# ─── CONFIG ───────────────────────────────────────────────────
SHEET_ID = os.environ["SHEET_ID"]
SA_JSON  = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])

DB_CONFIG = {
    "host":    os.environ["DB_HOST"],
    "db":      os.environ["DB_NAME"],
    "user":    os.environ["DB_USER"],
    "passwd":  os.environ["DB_PASS"],
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

DIST_DIR = "dist"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# ─── CONNESSIONI ──────────────────────────────────────────────
def connect_sheet():
    creds = Credentials.from_service_account_info(SA_JSON, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SHEET_ID)

def connect_db():
    return pymysql.connect(**DB_CONFIG)

# ─── HELPER ───────────────────────────────────────────────────
def safe(val, t=str):
    if val is None or str(val).strip() == "":
        return None
    try:
        return t(val)
    except:
        return None

def safe_float(val):
    return safe(val, float)

def safe_int(val):
    return safe(val, int)

def safe_date(val):
    if not val or str(val).strip() == "":
        return None
    for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"]:
        try:
            return datetime.strptime(str(val).strip(), fmt).date()
        except:
            continue
    return None

def upsert(conn, table, data, pk):
    if not data:
        return
    cols = list(data.keys())
    vals = list(data.values())
    placeholders = ", ".join(["%s"] * len(cols))
    updates = ", ".join([f"`{c}` = VALUES(`{c}`)" for c in cols if c != pk])
    sql = (f"INSERT INTO `{table}` ({', '.join(f'`{c}`' for c in cols)}) "
           f"VALUES ({placeholders}) "
           f"ON DUPLICATE KEY UPDATE {updates}")
    with conn.cursor() as cur:
        cur.execute(sql, vals)

# ─── SYNC INGREDIENTI ─────────────────────────────────────────
def sync_ingredienti(sh, conn):
    log("Sync ingredienti...")
    ws = sh.worksheet("Ingredienti")
    rows = ws.get_all_records()
    count = 0
    for r in rows:
        if not r.get("id"):
            continue
        data = {
            "id": safe_int(r.get("id")),
            "nome": safe(r.get("nome")),
            "nome_latino": safe(r.get("nome_latino")),
            "famiglia": safe(r.get("famiglia")),
            "proteina_g": safe_float(r.get("proteina_g")),
            "amido_g": safe_float(r.get("amido_g")),
            "carboidrati_g": safe_float(r.get("carboidrati_g")),
            "amido_puro_g": safe_float(r.get("amido_puro_g")),
            "lipidi_g": safe_float(r.get("lipidi_g")),
            "fibra_g": safe_float(r.get("fibra_g")),
            "fibra_solubile_g": safe_float(r.get("fibra_solubile_g")),
            "fibra_insolubile_g": safe_float(r.get("fibra_insolubile_g")),
            "zuccheri_g": safe_float(r.get("zuccheri_g")),
            "kcal": safe_float(r.get("kcal")),
            "acido_fitico_mg": safe_float(r.get("acido_fitico_mg")),
            "amido_resistente_g": safe_float(r.get("amido_resistente_g")),
            "inulina_g": safe_float(r.get("inulina_g")),
            "beta_glucani_g": safe_float(r.get("beta_glucani_g")),
            "calcio_mg": safe_float(r.get("calcio_mg")),
            "ferro_mg": safe_float(r.get("ferro_mg")),
            "magnesio_mg": safe_float(r.get("magnesio_mg")),
            "potassio_mg": safe_float(r.get("potassio_mg")),
            "ig": safe_int(r.get("ig")),
            "ig_crudo": safe_int(r.get("ig_crudo")),
            "ig_cotto": safe_int(r.get("ig_cotto")),
            "ig_fonte": safe(r.get("ig_fonte")),
            "ph_nativo": safe_float(r.get("ph_nativo")),
            "ph_impasto": safe(r.get("ph_impasto")),
            "assorbimento_idrico": safe(r.get("assorbimento_idrico")),
            "assorbimento_idrico_pct": safe_int(r.get("assorbimento_idrico_pct")),
            "temp_gel_c": safe_int(r.get("temp_gel_c")),
            "viscosita_gel": safe(r.get("viscosita_gel")),
            "attivato": 1 if str(r.get("attivato","")).lower() == "si" else 0,
            "protocollo_attivazione": safe(r.get("protocollo_attivazione")),
            "protocollo_terzista": safe(r.get("protocollo_terzista")),
            "lectine_presenti": safe(r.get("lectine_presenti")),
            "prolammine_presenti": safe(r.get("prolammine_presenti")),
            "fitati_post_attivazione_pct": safe_int(r.get("fitati_post_attivazione_pct")),
            "applicazioni": safe(r.get("applicazioni")),
            "shelf_life": safe(r.get("shelf_life")),
            "busta_consigliata": safe(r.get("busta_consigliata")),
            "note_tecnologiche": safe(r.get("note_tecnologiche")),
            "usda_fdc_id_crudo": safe(r.get("usda_fdc_id_crudo")),
            "usda_fdc_id_cotto": safe(r.get("usda_fdc_id_cotto")),
            "claim_salutistico_eu": safe(r.get("claim_salutistico_eu")),
            "potenziale_marketing": safe(r.get("potenziale_marketing")),
            "storia_ingrediente": safe(r.get("storia_ingrediente")),
            "novita_mercato": safe(r.get("novita_mercato")),
            "certificazioni_possibili": safe(r.get("certificazioni_possibili")),
            "posizionamento_claim": safe(r.get("posizionamento_claim")),
            "stato": safe(r.get("stato")) or "teorico",
            "fonte": safe(r.get("fonte")),
            "fonte_primaria": safe(r.get("fonte_primaria")),
            "data_verifica": safe_date(r.get("data_verifica")),
        }
        data = {k: v for k, v in data.items() if v is not None}
        upsert(conn, "ingredienti", data, "id")
        count += 1
    conn.commit()
    log(f"  Ingredienti sincronizzati: {count}")

# ─── SYNC IG PER STATO ────────────────────────────────────────
def sync_ig_per_stato(sh, conn):
    log("Sync IG per stato...")
    try:
        ws = sh.worksheet("Ingredienti_IG_per_stato")
    except:
        log("  Tab non trovato — skip")
        return
    rows = ws.get_all_records()
    count = 0
    for r in rows:
        if not r.get("ingrediente_id"):
            continue
        data = {
            "ingrediente_id": safe_int(r.get("ingrediente_id")),
            "stato_processo": safe(r.get("stato_processo")),
            "ig_valore": safe_int(r.get("ig_valore")),
            "ig_fonte": safe(r.get("ig_fonte")),
            "note": safe(r.get("note")),
            "data_inserimento": safe_date(r.get("data_inserimento")),
        }
        data = {k: v for k, v in data.items() if v is not None}
        if "ingrediente_id" in data and "stato_processo" in data:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO ingredienti_ig_per_stato "
                    "(ingrediente_id, stato_processo, ig_valore, ig_fonte, note, data_inserimento) "
                    "VALUES (%s,%s,%s,%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE "
                    "ig_valore=VALUES(ig_valore), ig_fonte=VALUES(ig_fonte), note=VALUES(note)",
                    [data.get("ingrediente_id"), data.get("stato_processo"),
                     data.get("ig_valore"), data.get("ig_fonte"),
                     data.get("note"), data.get("data_inserimento")]
                )
            count += 1
    conn.commit()
    log(f"  IG per stato sincronizzati: {count}")

# ─── SYNC ADDITIVI ────────────────────────────────────────────
def sync_additivi(sh, conn):
    log("Sync additivi...")
    ws = sh.worksheet("Additivi")
    rows = ws.get_all_records()
    count = 0
    for r in rows:
        if not r.get("id"):
            continue
        data = {
            "id": safe(r.get("id")),
            "nome": safe(r.get("nome")),
            "categoria": safe(r.get("categoria")),
            "funzione_primaria": safe(r.get("funzione_primaria")),
            "dose_min_pct": safe_float(r.get("dose_min_pct")),
            "dose_max_pct": safe_float(r.get("dose_max_pct")),
            "dose_ottimale_pct": safe_float(r.get("dose_ottimale_pct")),
            "busta": safe(r.get("busta")),
            "meccanismo": safe(r.get("meccanismo")),
            "in_etichetta": safe(r.get("in_etichetta")),
            "ig": safe_int(r.get("ig")),
            "ph_effetto": safe(r.get("ph_effetto")),
            "interazione_critica": safe(r.get("interazione_critica")),
            "non_ogm_verificato": safe(r.get("non_ogm_verificato")),
            "ausiliario_tecnologico": safe(r.get("ausiliario_tecnologico")),
            "note_tecniche": safe(r.get("note_tecniche")),
            "shelf_life": safe(r.get("shelf_life")),
            "fonte_primaria": safe(r.get("fonte_primaria")),
            "stato": safe(r.get("stato")) or "teorico",
            "data_verifica": safe_date(r.get("data_verifica")),
        }
        data = {k: v for k, v in data.items() if v is not None}
        upsert(conn, "additivi", data, "id")
        count += 1
    conn.commit()
    log(f"  Additivi sincronizzati: {count}")

# ─── SYNC BLEND ───────────────────────────────────────────────
def sync_blend(sh, conn):
    log("Sync blend...")
    ws = sh.worksheet("Blend")
    rows = ws.get_all_records()
    count = 0
    for r in rows:
        if not r.get("id"):
            continue
        data = {
            "id": safe(r.get("id")),
            "nome": safe(r.get("nome")),
            "categoria": safe(r.get("categoria")),
            "composizione_json": safe(r.get("composizione_json")),
            "score_struttura": safe_int(r.get("score_struttura")),
            "score_sapore": safe_int(r.get("score_sapore")),
            "score_lievitazione": safe_int(r.get("score_lievitazione")),
            "score_shelflife": safe_int(r.get("score_shelflife")),
            "ig_stimato": safe_int(r.get("ig_stimato")),
            "proteine_g": safe_float(r.get("proteine_g")),
            "idratazione_min": safe_int(r.get("idratazione_min")),
            "idratazione_max": safe_int(r.get("idratazione_max")),
            "note_processo": safe(r.get("note_processo")),
            "stato": safe(r.get("stato")) or "teorico",
        }
        data = {k: v for k, v in data.items() if v is not None}
        upsert(conn, "blend", data, "id")
        count += 1
    conn.commit()
    log(f"  Blend sincronizzati: {count}")

# ─── SYNC MATRICI ─────────────────────────────────────────────
def sync_matrice(sh, conn, tab_name, table_name, pk_a, pk_b):
    log(f"Sync {tab_name}...")
    try:
        ws = sh.worksheet(tab_name)
    except:
        log(f"  Tab {tab_name} non trovato — skip")
        return
    rows = ws.get_all_records()
    count = 0
    for r in rows:
        a = safe(r.get(pk_a))
        b = safe(r.get(pk_b))
        if not a or not b:
            continue
        punteggio = safe_int(r.get("punteggio"))
        note = safe(r.get("note"))
        fonte = safe(r.get("fonte"))
        stato = safe(r.get("stato")) or "letteratura"
        data_val = safe_date(r.get("data"))

        if table_name == "matrici_additivi_ingredienti":
            tipo = safe(r.get("tipo_interazione"))
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO matrici_additivi_ingredienti "
                    "(additivo_id, ingrediente_id, tipo_interazione, punteggio, note, fonte, stato, data) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE "
                    "tipo_interazione=VALUES(tipo_interazione), punteggio=VALUES(punteggio), "
                    "note=VALUES(note), fonte=VALUES(fonte)",
                    [a, safe_int(b), tipo, punteggio, note, fonte, stato, data_val]
                )
        else:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {table_name} "
                    f"({pk_a}, {pk_b}, punteggio, note, fonte, stato, data) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE "
                    "punteggio=VALUES(punteggio), note=VALUES(note), fonte=VALUES(fonte)",
                    [safe_int(a), safe_int(b), punteggio, note, fonte, stato, data_val]
                )
        count += 1
    conn.commit()
    log(f"  {tab_name} sincronizzate: {count} coppie")

# ─── SYNC PROVE DOE ───────────────────────────────────────────
def sync_prove_doe(sh, conn):
    log("Sync prove DoE...")
    ws = sh.worksheet("Prove_DoE")
    rows = ws.get_all_records()
    count = 0
    for r in rows:
        if not r.get("id"):
            continue
        # Mappa i campi principali
        data = {"id": safe(r.get("id"))}
        for campo in ["blend_id","operatore","luogo","tipo_impastatrice",
                      "note_impasto","note_sensoriali","note_generali",
                      "esito_complessivo","foto_url"]:
            v = safe(r.get(campo))
            if v: data[campo] = v
        for campo in ["numero_prova","friction_factor","t_cottura_fase1_c",
                      "durata_fase1_min","t_cottura_fase2_c","durata_fase2_min",
                      "t_cottura_fase3_c","durata_fase3_min","sapore_1_9",
                      "texture_1_9","croccantezza_1_9","retrogusto_1_9",
                      "soddisfazione_1_9","gommosita_1_9","durata_puntatura_min",
                      "durata_apretto_min"]:
            v = safe_int(r.get(campo))
            if v is not None: data[campo] = v
        for campo in ["t_ambiente_c","ur_ambiente_pct","t_farine_c",
                      "t_acqua_calcolata_c","t_acqua_usata_c","t_impasto_uscita_c",
                      "ph_impasto","idratazione_pct","ceci_pct","quinoa_pct",
                      "psyllium_pct","lm_pct","t_puntatura_c","ur_puntatura_pct",
                      "volume_aumento_puntatura_pct","t_apretto_c","ur_apretto_pct",
                      "volume_aumento_apretto_pct","t_interna_fine_c",
                      "volume_specifico_ml_g","durezza_mollica_n","colore_crosta_l",
                      "colore_crosta_a","colore_crosta_b","ph_mollica","aw_prodotto",
                      "umidita_pct"]:
            v = safe_float(r.get(campo))
            if v is not None: data[campo] = v
        d = safe_date(r.get("data"))
        if d: data["data"] = d
        upsert(conn, "prove_doe", data, "id")
        count += 1
    conn.commit()
    log(f"  Prove DoE sincronizzate: {count}")

# ─── GENERA JSON STATICI ──────────────────────────────────────
def genera_json_statici(conn):
    log("Generazione JSON statici...")
    os.makedirs(f"{DIST_DIR}/data", exist_ok=True)

    tabelle = [
        ("ingredienti", "ingredienti.json",
         "SELECT * FROM ingredienti ORDER BY famiglia, nome"),
        ("additivi", "additivi.json",
         "SELECT * FROM additivi ORDER BY categoria, id"),
        ("blend", "blend.json",
         "SELECT * FROM blend ORDER BY id"),
        ("matrici_reol", "matrici_reol.json",
         "SELECT * FROM matrici_reol ORDER BY punteggio DESC"),
        ("matrici_chim", "matrici_chim.json",
         "SELECT * FROM matrici_chim ORDER BY punteggio DESC"),
        ("matrici_sens", "matrici_sens.json",
         "SELECT * FROM matrici_sens ORDER BY punteggio DESC"),
        ("matrici_additivi_ingredienti", "matrici_additivi.json",
         "SELECT * FROM matrici_additivi_ingredienti ORDER BY punteggio DESC"),
        ("ingredienti_ig_per_stato", "ig_per_stato.json",
         "SELECT * FROM ingredienti_ig_per_stato ORDER BY ingrediente_id"),
        ("scheda_operativa", "scheda_operativa.json",
         "SELECT * FROM scheda_operativa ORDER BY blend_id, busta"),
        ("prove_doe", "prove_doe.json",
         "SELECT id,blend_id,numero_prova,data,operatore,ceci_pct,idratazione_pct,"
         "t_apretto_c,t_interna_fine_c,aw_prodotto,sapore_1_9,soddisfazione_1_9,"
         "esito_complessivo FROM prove_doe ORDER BY data DESC"),
    ]

    for _, filename, query in tabelle:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        # Converti date in stringhe
        for row in rows:
            for k, v in row.items():
                if isinstance(v, (date, datetime)):
                    row[k] = v.isoformat()
        path = f"{DIST_DIR}/data/{filename}"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, default=str)
        log(f"  {filename}: {len(rows)} record")

    # JSON indice con metadata
    meta = {
        "generato": datetime.now().isoformat(),
        "sheet_id": SHEET_ID,
        "versione": "1.0",
    }
    with open(f"{DIST_DIR}/data/meta.json", "w") as f:
        json.dump(meta, f)
    log("  meta.json generato")

# ─── MAIN ─────────────────────────────────────────────────────
def main():
    print("=" * 50)
    print(f"ZeroCereals KB Sync — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    sh   = connect_sheet()
    conn = connect_db()
    log(f"Connesso a Sheet: {sh.title}")
    log(f"Connesso a DB: {DB_CONFIG['db']}")

    sync_ingredienti(sh, conn)
    sync_ig_per_stato(sh, conn)
    sync_additivi(sh, conn)
    sync_blend(sh, conn)
    sync_matrice(sh, conn, "Matrici_Reol", "matrici_reol", "id_a", "id_b")
    sync_matrice(sh, conn, "Matrici_Chim", "matrici_chim", "id_a", "id_b")
    sync_matrice(sh, conn, "Matrici_Sens", "matrici_sens", "id_a", "id_b")
    sync_matrice(sh, conn, "Matrici_Additivi_x_Ingredienti",
                 "matrici_additivi_ingredienti", "additivo_id", "ingrediente_id")
    sync_prove_doe(sh, conn)
    genera_json_statici(conn)

    conn.close()
    print("=" * 50)
    print("Sync completata.")
    print("=" * 50)

if __name__ == "__main__":
    main()
