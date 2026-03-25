import pandas as pd
import numpy as np
from supabase import create_client
import requests

SUPABASE_URL = "https://poyqbtjtziypzuyjybtk.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBveXFidGp0eml5cHp1eWp5YnRrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM4MjU0MDAsImV4cCI6MjA4OTQwMTQwMH0._crpKijnR3XnCC7zPSUeG8lwi8qIJp5A1GP4OVVgzAQ"
FILE = "/Users/jennieansellem/Desktop/dataset_ikea.xlsx"

USELESS = ["scraping_server_ip", "user_agent_string", "deprecated_field_v2", "processing_time_ms"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_table_columns(table):
    """Récupère les colonnes existantes dans la table Supabase"""
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}?limit=1", headers=headers)
    if r.status_code == 200:
        data = r.json()
        if data:
            return list(data[0].keys())
    # fallback : essaye avec OPTIONS
    r2 = requests.get(f"{SUPABASE_URL}/rest/v1/{table}", headers={**headers, "Accept": "application/openapi+json"})
    return None

def clean(df, table):
    print(f"  Lignes brutes : {len(df)}")
    df = df.drop(columns=[c for c in USELESS if c in df.columns])
    df = df.dropna(how="all")

    # Normalise les noms de colonnes en lowercase
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]

    # Gère les dates
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["date"] = df["date"].dt.strftime("%Y-%m-%dT%H:%M:%S")

    # Gère les booléens
    if "is_verified" in df.columns:
        df["is_verified"] = df["is_verified"].map({True: True, False: False, 1: True, 0: False, "True": True, "False": False}).fillna(False)

    # Gère les numériques
    for col in ["rating", "likes", "user_followers", "share_count", "reply_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Remplace les NaN par None
    df = df.where(pd.notnull(df), None)

    # Retire les colonnes réservées Supabase
    for col in ["id", "inserted_at", "sentiment_detected", "topic", "confidence"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    print(f"  Lignes propres : {len(df)}")
    print(f"  Colonnes : {list(df.columns)}")
    return df

def inject(df, table):
    rows = df.to_dict(orient="records")
    batch = 100
    errors = 0
    for i in range(0, len(rows), batch):
        chunk = rows[i:i+batch]
        chunk = [{k: v for k, v in r.items() if v is not None and v == v} for r in chunk]
        try:
            supabase.table(table).insert(chunk).execute()
            print(f"  ✓ {min(i+batch, len(rows))}/{len(rows)}", end="\r")
        except Exception as e:
            errors += 1
            if errors == 1:
                print(f"\n  ⚠ Erreur : {str(e)[:120]}")
                print(f"  → Tentative sans colonnes problématiques...")
            # Retry en retirant les colonnes inconnues du 1er item
            try:
                safe_chunk = [{k: v for k, v in r.items() if k not in ["date", "brand", "category", "post_type"]} for r in chunk]
                supabase.table(table).insert(safe_chunk).execute()
                print(f"  ✓ {min(i+batch, len(rows))}/{len(rows)} (mode safe)", end="\r")
            except Exception as e2:
                print(f"\n  ✗ Échec : {str(e2)[:100]}")
    print(f"\n  ✅ {table} — terminé ({len(rows)} lignes)")

# ─── SQL pour corriger les tables ─────────────────────────────
FIX_SQL = """
-- Colle ce SQL dans Supabase → SQL Editor → Run
-- Puis relance ce script

ALTER TABLE reputation_crise
  ADD COLUMN IF NOT EXISTS brand VARCHAR(100),
  ADD COLUMN IF NOT EXISTS post_type VARCHAR(50),
  ADD COLUMN IF NOT EXISTS date TIMESTAMP,
  ADD COLUMN IF NOT EXISTS sentiment VARCHAR(20),
  ADD COLUMN IF NOT EXISTS user_followers INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS is_verified BOOLEAN,
  ADD COLUMN IF NOT EXISTS language VARCHAR(10),
  ADD COLUMN IF NOT EXISTS share_count INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS reply_count INTEGER DEFAULT 0;

ALTER TABLE benchmark_marche
  ADD COLUMN IF NOT EXISTS date TIMESTAMP,
  ADD COLUMN IF NOT EXISTS user_followers INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS is_verified BOOLEAN,
  ADD COLUMN IF NOT EXISTS language VARCHAR(10),
  ADD COLUMN IF NOT EXISTS share_count INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS reply_count INTEGER DEFAULT 0;

ALTER TABLE voix_client_cx
  ADD COLUMN IF NOT EXISTS brand VARCHAR(100),
  ADD COLUMN IF NOT EXISTS category VARCHAR(100),
  ADD COLUMN IF NOT EXISTS date TIMESTAMP,
  ADD COLUMN IF NOT EXISTS sentiment VARCHAR(20),
  ADD COLUMN IF NOT EXISTS user_followers INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS is_verified BOOLEAN,
  ADD COLUMN IF NOT EXISTS language VARCHAR(10),
  ADD COLUMN IF NOT EXISTS share_count INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS reply_count INTEGER DEFAULT 0;
"""

sheets = {
    "Reputation_Crise":  "reputation_crise",
    "Benchmark_Marche":  "benchmark_marche",
    "Voix_Client_CX":    "voix_client_cx"
}

print("\n🚀 Injection dataset IKEA → Supabase\n")
print("⚠️  Si des erreurs de colonnes apparaissent, exécute ce SQL dans Supabase d'abord :")
print("─" * 60)
print(FIX_SQL)
print("─" * 60)
print()

for sheet, table in sheets.items():
    print(f"→ {sheet}...")
    try:
        df = pd.read_excel(FILE, sheet_name=sheet)
        df = clean(df, table)
        inject(df, table)
    except Exception as e:
        print(f"  ✗ Erreur fatale : {e}")
    print()

print("🎉 Script terminé — vérifie dans Supabase Table Editor")
