"""
sync_kb.py v3 — Legge Google Sheet → genera JSON statici
Niente MySQL diretto: l'import lo fa api.php su Netsons dopo il deploy FTP.
"""

import os
import json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

SHEET_ID = os.environ["SHEET_ID"]
SA_JSON  = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
DIST_DIR = "dist"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def connect_sheet():
    creds = Credentials.from_service_account_info(SA_JSON, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SHEET_ID)

def export_tab(sh, tab_name, filename):
    """Legge un tab del Sheet e lo salva come JSON."""
    try:
        ws   = sh.worksheet(tab_name)
        rows = ws.get_all_records()
        path = f"{DIST_DIR}/data/{filename}"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, default=str)
        log(f"  {tab_name} → {filename}: {len(rows)} record")
        return len(rows)
    except gspread.WorksheetNotFound:
        log(f"  {tab_name}: tab non trovato — skip")
        return 0
    except Exception as e:
        log(f"  {tab_name}: errore — {e}")
        return 0

def main():
    print("=" * 50)
    print(f"ZeroCereals KB Sync v3 — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    os.makedirs(f"{DIST_DIR}/data", exist_ok=True)

    sh = connect_sheet()
    log(f"Sheet connesso: {sh.title}")

    # Esporta tutti i tab come JSON statici
    tabs = [
        ("Ingredienti",                    "ingredienti.json"),
        ("Additivi",                       "additivi.json"),
        ("Blend",                          "blend.json"),
        ("Matrici_Reol",                   "matrici_reol.json"),
        ("Matrici_Chim",                   "matrici_chim.json"),
        ("Matrici_Sens",                   "matrici_sens.json"),
        ("Matrici_Additivi_x_Ingredienti", "matrici_additivi.json"),
        ("Ingredienti_IG_per_stato",       "ig_per_stato.json"),
        ("Scheda_Operativa",               "scheda_operativa.json"),
        ("Prove_DoE",                      "prove_doe.json"),
    ]

    totale = 0
    for tab_name, filename in tabs:
        totale += export_tab(sh, tab_name, filename)

    # Metadati
    meta = {
        "generato":  datetime.now().isoformat(),
        "sheet_id":  SHEET_ID,
        "versione":  "1.0",
        "record_totali": totale,
    }
    with open(f"{DIST_DIR}/data/meta.json", "w") as f:
        json.dump(meta, f)
    log(f"meta.json generato — {totale} record totali")

    print("=" * 50)
    print("Export JSON completato.")
    print("=" * 50)

if __name__ == "__main__":
    main()
