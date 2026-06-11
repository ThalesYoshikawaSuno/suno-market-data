"""
scripts/update_gtrends_snowflake.py
Busca Google Trends via SerpAPI para os 7 FIIs Suno e faz MERGE
na tabela AI_WORKSPACE.SANDBOX.TB_MS_GOOGLE_TRENDS no Snowflake.

Uso:
  python scripts/update_gtrends_snowflake.py

Variáveis de ambiente necessárias:
  SERPAPI_KEY          — chave da SerpAPI
  SNOWFLAKE_ACCOUNT    — account identifier
  SNOWFLAKE_USERNAME
  SNOWFLAKE_PASSWORD
  SNOWFLAKE_WAREHOUSE  (padrão: WH_AI_AGENTS)
  SNOWFLAKE_ROLE       (padrão: AI_AGENTS)
  SNOWFLAKE_DB_TRENDS  (padrão: AI_WORKSPACE)
  SNOWFLAKE_SCHEMA_TRENDS (padrão: SANDBOX)
"""

import os
import sys
import time
import hashlib
import requests
import snowflake.connector
from datetime import datetime, timezone, timedelta

SERPAPI_KEY = os.environ["SERPAPI_KEY"]

SF = dict(
    account   = os.environ["SNOWFLAKE_ACCOUNT"],
    user      = os.environ["SNOWFLAKE_USERNAME"],
    password  = os.environ["SNOWFLAKE_PASSWORD"],
    database  = os.environ.get("SNOWFLAKE_DB_TRENDS", "AI_WORKSPACE"),
    schema    = os.environ.get("SNOWFLAKE_SCHEMA_TRENDS", "SANDBOX"),
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "WH_AI_AGENTS"),
    role      = os.environ.get("SNOWFLAKE_ROLE", "AI_AGENTS"),
)

TABLE = f"{SF['database']}.{SF['schema']}.TB_MS_GOOGLE_TRENDS"

# ── Grupos de query — âncora é sempre o primeiro termo ────────────────────────
QUERY_GROUPS = {
    # SNAG11 — FIAGRO PAPEL
    "G_SNAG_1": ["KNCA11", "SNAG11", "RZAG11", "VGIA11", "VCRA11"],
    "G_SNAG_2": ["KNCA11", "SNAG11", "RURA11", "AAZQ11", "CPTR11"],
    "G_SNAG_3": ["KNCA11", "SNAG11", "GCRA11", "AGRX11", "BBGO11"],
    # SNFZ11 — FIAGRO HÍBRIDO
    "G_SNFZ_1": ["RZTR11", "SNFZ11", "FGAA11", "BBGO11", "RURA11"],
    "G_SNFZ_2": ["RZTR11", "SNFZ11", "FZDA11", "FZDB11"],
    # SNCI11 — CRI / PAPEL
    "G_SNCI_1": ["MXRF11", "SNCI11", "KNCR11", "KNIP11", "RBRR11"],
    "G_SNCI_2": ["MXRF11", "SNCI11", "HGCR11", "VGIR11", "BTCI11"],
    # SNID11 — FI-INFRA
    "G_SNID_1": ["KDIF11", "SNID11", "BDIF11", "JURO11", "IFRA11"],
    "G_SNID_2": ["KDIF11", "SNID11", "NVIF11", "BIDB11", "BODB11"],
    # SNME11 — MULTIESTRATÉGIA / FOF
    "G_SNME_1": ["CPTS11", "SNME11", "BTHF11", "PSEC11", "KFOF11"],
    "G_SNME_2": ["CPTS11", "SNME11", "RBFF11", "KNHF11"],
    # SNFF11 — FOF
    "G_SNFF_1": ["BCFF11", "SNFF11", "HFOF11", "RBRF11", "HGFF11"],
    # SNEL11 — ENERGIA / INFRA
    "G_SNEL_1": ["TRXF11", "SNEL11", "GGRC11", "GARE11", "HGLG11"],
}


def fetch_trends(terms: list[str]) -> list[dict]:
    url = "https://serpapi.com/search"
    params = {
        "engine": "google_trends",
        "q": ",".join(terms),
        "date": "today 12-m",
        "geo": "BR",
        "hl": "pt-BR",
        "data_type": "TIMESERIES",
        "api_key": SERPAPI_KEY,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json().get("interest_over_time", {}).get("timeline_data", [])


def parse_rows(timeline: list[dict], grupo: str, termos: list[str]) -> list[tuple]:
    rows = []
    ancora = termos[0]

    for point in timeline:
        dt = datetime.fromtimestamp(int(point["timestamp"])).strftime("%Y-%m-%d")
        valores = {v["query"]: v["extracted_value"] for v in point["values"]}
        valor_ancora = valores.get(ancora, 0)

        for termo, valor in valores.items():
            valor_norm = round(valor / valor_ancora * 100, 4) if valor_ancora > 0 else None
            id_hash = hashlib.md5(f"Asset|{termo}|{dt}|semanal".encode()).hexdigest()

            rows.append((
                id_hash,           # ID
                dt,                # DT_REFERENCIA
                "semanal",         # PERIODO_GRANULARIDADE
                "BR",              # GEO
                "Asset",           # BU
                termo,             # TERMO
                grupo,             # GRUPO_QUERY
                ancora,            # ANCORA
                valor,             # VALOR_RELATIVO
                float(valor_ancora) if valor_ancora is not None else None,  # VALOR_ANCORA_NO_GRUPO
                valor_norm,        # VALOR_NORMALIZADO
            ))

    return rows


def merge_into_snowflake(all_rows: list[tuple]) -> int:
    if not all_rows:
        print("  Nenhuma linha para inserir.")
        return 0

    conn = snowflake.connector.connect(**SF)
    cur = conn.cursor()

    merge_sql = f"""
    MERGE INTO {TABLE} AS tgt
    USING (SELECT %s AS ID, %s::DATE AS DT_REFERENCIA, %s AS PERIODO_GRANULARIDADE,
                  %s AS GEO, %s AS BU, %s AS TERMO, %s AS GRUPO_QUERY, %s AS ANCORA,
                  %s AS VALOR_RELATIVO, %s AS VALOR_ANCORA_NO_GRUPO, %s AS VALOR_NORMALIZADO
           FROM dual) AS src
    ON tgt.ID = src.ID
    WHEN NOT MATCHED THEN INSERT (
        ID, DT_REFERENCIA, PERIODO_GRANULARIDADE, GEO, BU,
        TERMO, GRUPO_QUERY, ANCORA, VALOR_RELATIVO,
        VALOR_ANCORA_NO_GRUPO, VALOR_NORMALIZADO,
        DT_EXTRACAO, DT_CARGA, VERSAO_SCRIPT
    ) VALUES (
        src.ID, src.DT_REFERENCIA, src.PERIODO_GRANULARIDADE, src.GEO, src.BU,
        src.TERMO, src.GRUPO_QUERY, src.ANCORA, src.VALOR_RELATIVO,
        src.VALOR_ANCORA_NO_GRUPO, src.VALOR_NORMALIZADO,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'github_actions_v2'
    )
    WHEN MATCHED THEN UPDATE SET
        VALOR_RELATIVO          = src.VALOR_RELATIVO,
        VALOR_ANCORA_NO_GRUPO   = src.VALOR_ANCORA_NO_GRUPO,
        VALOR_NORMALIZADO       = src.VALOR_NORMALIZADO,
        DT_CARGA                = CURRENT_TIMESTAMP(),
        VERSAO_SCRIPT           = 'github_actions_v2'
    """

    inserted = 0
    try:
        # Lote de 500 para não estourar o tamanho da query
        BATCH = 500
        for i in range(0, len(all_rows), BATCH):
            batch = all_rows[i:i + BATCH]
            cur.executemany(merge_sql, batch)
            inserted += len(batch)
            print(f"  ✅ Lote {i // BATCH + 1}: {len(batch)} linhas processadas")
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return inserted


def main():
    print(f"\n🔍 Google Trends — Atualização Snowflake")
    print(f"   Tabela: {TABLE}")
    print(f"   Grupos: {len(QUERY_GROUPS)}\n")

    all_rows: list[tuple] = []
    errors = []

    for grupo, termos in QUERY_GROUPS.items():
        try:
            print(f"  Buscando {grupo} ({', '.join(termos)})...")
            timeline = fetch_trends(termos)
            rows = parse_rows(timeline, grupo, termos)
            all_rows.extend(rows)
            print(f"    → {len(rows)} pontos")
            time.sleep(1.5)  # respeitar rate limit da SerpAPI
        except Exception as e:
            print(f"    ❌ Erro em {grupo}: {e}")
            errors.append(grupo)

    print(f"\n📦 Total coletado: {len(all_rows)} linhas ({len(errors)} grupos com erro)")

    if all_rows:
        print("\n🔄 Fazendo MERGE no Snowflake...")
        total = merge_into_snowflake(all_rows)
        print(f"\n✅ MERGE concluído — {total} linhas processadas")
    else:
        print("\n⚠️  Nenhum dado coletado — verifique a SERPAPI_KEY e os grupos.")
        sys.exit(1)

    if errors:
        print(f"\n⚠️  Grupos com falha: {', '.join(errors)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
