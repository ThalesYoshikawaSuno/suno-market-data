"""
scripts/zeus_supabase_ingest.py
Coleta datasets do Snowflake e faz upsert no Supabase (Zeus MCP).

Uso:
  python scripts/zeus_supabase_ingest.py --datasets all
  python scripts/zeus_supabase_ingest.py --datasets portais,noticias
  python scripts/zeus_supabase_ingest.py --datasets liquidez,pvp

Datasets disponíveis:
  portais   → ms_trafego_portais
  noticias  → ms_trafego_noticias
  liquidez  → ms_asset_liquidez_ms
  pvp       → ms_asset_pvp_ms

Variáveis de ambiente necessárias:
  Snowflake: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USERNAME, SNOWFLAKE_PASSWORD
             SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE
             SNOWFLAKE_DB_ASSET, SNOWFLAKE_SCHEMA_ASSET
  Supabase:  SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
"""

import os, sys, argparse
from datetime import datetime, timezone, timedelta

import snowflake.connector
from supabase import create_client, Client

# ── Configuração ──────────────────────────────────────────────────────────────

NOW_UTC = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _sf_auth() -> dict:
    """Key-pair se SNOWFLAKE_PRIVATE_KEY estiver definida, senha caso contrário."""
    pem = os.environ.get("SNOWFLAKE_PRIVATE_KEY", "").strip()
    if pem:
        from cryptography.hazmat.primitives.serialization import (
            load_pem_private_key, Encoding, PrivateFormat, NoEncryption,
        )
        from cryptography.hazmat.backends import default_backend
        raw_passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")
        passphrase = raw_passphrase.encode() if raw_passphrase else None
        key = load_pem_private_key(pem.encode(), password=passphrase, backend=default_backend())
        return {"private_key": key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())}
    return {"password": os.environ["SNOWFLAKE_PASSWORD"]}

SF_BASE = dict(
    account   = os.environ["SNOWFLAKE_ACCOUNT"],
    user      = os.environ["SNOWFLAKE_USERNAME"],
    database  = os.environ.get("SNOWFLAKE_DATABASE",  "RAW_MARKETING"),
    schema    = os.environ.get("SNOWFLAKE_SCHEMA",    "MARKET_SHARE"),
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "WH_AI_AGENTS"),
    role      = os.environ.get("SNOWFLAKE_ROLE",      "AI_AGENTS"),
    **_sf_auth(),
)
SF_ASSET = {
    **SF_BASE,
    "database": os.environ.get("SNOWFLAKE_DB_ASSET",     "REFINED_ASSET"),
    "schema":   os.environ.get("SNOWFLAKE_SCHEMA_ASSET", "QUANTUM"),
}

SUPABASE_URL      = os.environ["SUPABASE_URL"]
SUPABASE_KEY      = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
BATCH_SIZE        = 500   # linhas por requisição de upsert

# ── Helpers Snowflake ─────────────────────────────────────────────────────────

def run_query(sql: str, cfg: dict = SF_BASE) -> list[dict]:
    conn = snowflake.connector.connect(**cfg)
    cur  = conn.cursor(snowflake.connector.DictCursor)
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        return [
            {k.lower(): (v.isoformat() if hasattr(v, "isoformat") else v)
             for k, v in row.items()}
            for row in rows
        ]
    finally:
        cur.close()
        conn.close()

# ── Helpers Supabase ──────────────────────────────────────────────────────────

def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def upsert_table(client: Client, table: str, rows: list[dict], pk_cols: list[str]) -> int:
    """Upsert em lotes de BATCH_SIZE. Retorna total de linhas processadas."""
    if not rows:
        print(f"  ⚠️  {table}: sem linhas para inserir.")
        return 0

    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        client.table(table).upsert(
            batch,
            on_conflict=",".join(pk_cols),
        ).execute()
        total += len(batch)
        print(f"  ↑ {table}: {total}/{len(rows)} linhas enviadas...")

    return total

# ── Datasets ──────────────────────────────────────────────────────────────────

DATASETS: dict[str, callable] = {}

def dataset(name: str):
    def decorator(fn):
        DATASETS[name] = fn
        return fn
    return decorator


@dataset("portais")
def ingest_portais(client: Client):
    print("📡 Portais Financeiros → ms_trafego_portais")
    rows = run_query("""
        SELECT
            TO_CHAR(DT_REFERENCIA, 'YYYY-MM-DD') AS dt_referencia,
            ANO                                   AS ano,
            SEMANA                                AS semana,
            ANO_SEMANA                            AS ano_semana,
            EMPRESA                               AS empresa,
            VISITAS                               AS visitas,
            SHARE                                 AS share
        FROM RAW_MARKETING.MARKET_SHARE.TB_TRAFEGO_SITES_NOTICIAS
        WHERE CATEGORIA = 'portais'
        ORDER BY DT_REFERENCIA, EMPRESA
    """)
    n = upsert_table(client, "ms_trafego_portais", rows, ["dt_referencia", "empresa"])
    print(f"  ✅ portais: {n} linhas | última ref: {rows[-1]['dt_referencia'] if rows else '—'}")


@dataset("noticias")
def ingest_noticias(client: Client):
    print("🏢 Suno Portais (FII) → ms_trafego_noticias")
    rows = run_query("""
        SELECT
            TO_CHAR(DT_REFERENCIA, 'YYYY-MM-DD') AS dt_referencia,
            ANO                                   AS ano,
            SEMANA                                AS semana,
            ANO_SEMANA                            AS ano_semana,
            EMPRESA                               AS empresa,
            VISITAS                               AS visitas,
            SHARE                                 AS share
        FROM RAW_MARKETING.MARKET_SHARE.TB_TRAFEGO_SITES_NOTICIAS
        WHERE CATEGORIA = 'noticias'
        ORDER BY DT_REFERENCIA, EMPRESA
    """)
    n = upsert_table(client, "ms_trafego_noticias", rows, ["dt_referencia", "empresa"])
    print(f"  ✅ noticias: {n} linhas | última ref: {rows[-1]['dt_referencia'] if rows else '—'}")


@dataset("liquidez")
def ingest_liquidez(client: Client):
    print("💧 Asset — Market Share Liquidez → ms_asset_liquidez_ms")
    rows = run_query("""
        SELECT
            TO_CHAR(DATE_TRUNC('MONTH', DATA), 'YYYY-MM') AS mes,
            FUNDO_SUNO                                     AS fundo_suno,
            CODIGO                                         AS codigo,
            NOME_DO_ATIVO                                  AS nome_do_ativo,
            CAST(IS_FUNDO_SUNO AS BOOLEAN)                 AS is_fundo_suno,
            ROUND(AVG(MARKET_SHARE), 6)                    AS market_share_medio,
            ROUND(SUM(LIQUIDEZ_DIARIA), 2)                 AS liquidez_mes,
            ROUND(AVG(LIQUIDEZ_DIARIA_TOTAL_GRUPO), 2)     AS liquidez_grupo_medio
        FROM REFINED_ASSET.QUANTUM.MARKETSHARE_LIQUIDEZ_FUNDOS_ESTRUTURADOS
        WHERE DATA >= DATEADD(MONTH, -13, CURRENT_DATE())
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY fundo_suno, mes, is_fundo_suno DESC
    """, cfg=SF_ASSET)
    n = upsert_table(client, "ms_asset_liquidez_ms", rows, ["mes", "codigo", "fundo_suno"])
    print(f"  ✅ liquidez: {n} linhas | último mês: {rows[-1]['mes'] if rows else '—'}")


@dataset("pvp")
def ingest_pvp(client: Client):
    print("⚖️  Asset — P/VP → ms_asset_pvp_ms")
    rows = run_query("""
        SELECT
            TO_CHAR(DATA, 'YYYY-MM-DD')                   AS data,
            FUNDO_SUNO                                     AS fundo_suno,
            CODIGO                                         AS codigo,
            NOME_DO_ATIVO                                  AS nome_do_ativo,
            CAST(IS_FUNDO_SUNO AS BOOLEAN)                 AS is_fundo_suno,
            ROUND(P_VP, 4)                                 AS p_vp,
            ROUND(P_VP_TOTAL_GRUPO, 4)                     AS p_vp_total_grupo,
            ROUND(MARKET_SHARE_PVP, 6)                     AS market_share_pvp,
            ROUND(MARKET_SHARE_PVP_FUNDO_SUNO, 6)         AS market_share_pvp_fundo_suno,
            ROUND(PRECO_FECHAMENTO, 4)                     AS preco_fechamento,
            ROUND(VPA, 4)                                  AS vpa
        FROM REFINED_ASSET.QUANTUM.MARKETSHARE_PVP_FUNDOS_ESTRUTURADOS
        WHERE DATA >= DATEADD(MONTH, -13, CURRENT_DATE())
          AND FECHAMENTO_MES = 1
        ORDER BY fundo_suno, data, is_fundo_suno DESC
    """, cfg=SF_ASSET)
    n = upsert_table(client, "ms_asset_pvp_ms", rows, ["data", "codigo", "fundo_suno"])
    print(f"  ✅ pvp: {n} linhas | última data: {rows[-1]['data'] if rows else '—'}")


# ── Entrypoint ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--datasets", default="all",
        help="Datasets separados por vírgula. Ex: portais,noticias,liquidez,pvp  ou  all"
    )
    args = parser.parse_args()

    targets = list(DATASETS.keys()) if args.datasets == "all" else args.datasets.split(",")
    invalid = [t for t in targets if t not in DATASETS]
    if invalid:
        print(f"❌ Datasets desconhecidos: {invalid}. Disponíveis: {list(DATASETS.keys())}")
        sys.exit(1)

    print(f"\n🚀 Zeus Supabase Ingest — {NOW_UTC}")
    print(f"   Datasets: {targets}\n")

    client = get_supabase()
    errors = []

    for name in targets:
        try:
            DATASETS[name](client)
        except Exception as e:
            print(f"  ❌ Erro em [{name}]: {e}")
            errors.append((name, str(e)))

    print(f"\n{'✅ Concluído sem erros.' if not errors else f'⚠️  {len(errors)} erro(s): {errors}'}")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
