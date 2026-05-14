"""
scripts/fetch_snowflake.py

Busca dados do Snowflake e salva como JSON em data/.
Atualiza data/meta.json com timestamp e próximo update.

Uso:
  python scripts/fetch_snowflake.py --datasets all
  python scripts/fetch_snowflake.py --datasets portais,noticias
  python scripts/fetch_snowflake.py --datasets youtube_canais,youtube_videos,youtube_trending,youtube_top
"""

import os, json, argparse, sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
import snowflake.connector

# ── Config ────────────────────────────────────────────────────────────────────
ROOT   = Path(__file__).parent.parent
DATA   = ROOT / "data"
DATA.mkdir(exist_ok=True)

NOW    = datetime.now(timezone.utc)
NOW_BR = datetime.now(timezone(timedelta(hours=-3)))  # horário de Brasília

SF = dict(
    account   = os.environ["SNOWFLAKE_ACCOUNT"],
    user      = os.environ["SNOWFLAKE_USERNAME"],
    password  = os.environ["SNOWFLAKE_PASSWORD"],
    database  = os.environ.get("SNOWFLAKE_DATABASE", "RAW_MARKETING"),
    schema    = os.environ.get("SNOWFLAKE_SCHEMA",   "MARKET_SHARE"),
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE","WH_AI_AGENTS"),
    role      = os.environ.get("SNOWFLAKE_ROLE",     "AI_AGENTS"),
)

SF_TRENDS = {
    **SF,
    "database": os.environ.get("SNOWFLAKE_DB_TRENDS",     "AI_WORKSPACE"),
    "schema":   os.environ.get("SNOWFLAKE_SCHEMA_TRENDS", "SANDBOX"),
}

# ── Helpers ───────────────────────────────────────────────────────────────────
def connect(cfg=SF):
    return snowflake.connector.connect(**cfg)

def run_query(sql: str, cfg=SF) -> list[dict]:
    conn = connect(cfg)
    cur  = conn.cursor(snowflake.connector.DictCursor)
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        return [
            {k: (v.isoformat() if hasattr(v, 'isoformat') else v)
             for k, v in row.items()}
            for row in rows
        ]
    finally:
        cur.close()
        conn.close()

def save(filename: str, data: object):
    path = DATA / filename
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"  ✅ {filename} — {len(data) if isinstance(data, list) else '...'} registros")

def next_update(freq: str) -> str:
    if freq == "diario":
        d = NOW + timedelta(days=1)
    elif freq == "semanal":
        d = NOW + timedelta(weeks=1)
    elif freq == "quinzenal":
        d = NOW + timedelta(days=15)
    elif freq == "mensal":
        d = NOW + timedelta(days=30)
    else:
        d = NOW + timedelta(days=7)
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")

def update_meta(dataset: str, status: str = "ok", error: str = None):
    meta_path = DATA / "meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    if dataset in meta["datasets"]:
        freq = meta["datasets"][dataset]["frequencia"]
        meta["datasets"][dataset]["ultima_atualizacao"] = NOW.strftime("%Y-%m-%dT%H:%M:%SZ")
        meta["datasets"][dataset]["ultima_atualizacao_br"] = NOW_BR.strftime("%d/%m/%Y %H:%M")
        meta["datasets"][dataset]["proximo_update"] = next_update(freq)
        meta["datasets"][dataset]["status"] = status
        if error:
            meta["datasets"][dataset]["ultimo_erro"] = error[:200]
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

# ── Datasets ──────────────────────────────────────────────────────────────────

DATASETS = {}

def dataset(name):
    def decorator(fn):
        DATASETS[name] = fn
        return fn
    return decorator

@dataset("portais")
def fetch_portais():
    print("📡 Portais Financeiros...")
    rows = run_query("""
        SELECT
            TO_CHAR(DT_REFERENCIA, 'YYYY-MM-DD') AS DT_REFERENCIA,
            ANO, SEMANA, ANO_SEMANA, EMPRESA, VISITAS, SHARE
        FROM RAW_MARKETING.MARKET_SHARE.TB_TRAFEGO_SITES_NOTICIAS
        WHERE CATEGORIA = 'portais'
        ORDER BY DT_REFERENCIA, EMPRESA
    """)
    save("trafego_portais.json", rows)
    update_meta("trafego_portais")

@dataset("noticias")
def fetch_noticias():
    print("🏢 Suno Portais (FII)...")
    rows = run_query("""
        SELECT
            TO_CHAR(DT_REFERENCIA, 'YYYY-MM-DD') AS DT_REFERENCIA,
            ANO, SEMANA, ANO_SEMANA, EMPRESA, VISITAS, SHARE
        FROM RAW_MARKETING.MARKET_SHARE.TB_TRAFEGO_SITES_NOTICIAS
        WHERE CATEGORIA = 'noticias'
        ORDER BY DT_REFERENCIA, EMPRESA
    """)
    save("trafego_noticias.json", rows)
    update_meta("trafego_noticias")

@dataset("anbima_pl")
def fetch_anbima_pl():
    print("💰 ANBIMA PL...")
    rows = run_query("""
        SELECT
            TO_CHAR(DT_REFERENCIA, 'YYYY-MM-DD') AS DT_REFERENCIA,
            TIPO_INSTITUICAO, GESTOR, TIPO_VISAO, COLUNA_ORIGEM, VALOR
        FROM RAW_MARKETING.MARKET_SHARE.TB_ANBIMA_PL_RAW
        ORDER BY DT_REFERENCIA DESC, VALOR DESC
    """)
    save("anbima_pl.json", rows)
    update_meta("anbima_pl")

@dataset("anbima_cap")
def fetch_anbima_cap():
    print("💰 ANBIMA Captação...")
    rows = run_query("""
        SELECT
            TO_CHAR(DT_REFERENCIA, 'YYYY-MM-DD') AS DT_REFERENCIA,
            TIPO_INSTITUICAO, GESTOR, JANELA, TIPO_VISAO, COLUNA_ORIGEM, VALOR
        FROM RAW_MARKETING.MARKET_SHARE.TB_ANBIMA_CAPTACAO_RAW
        ORDER BY DT_REFERENCIA DESC, VALOR DESC
    """)
    save("anbima_cap.json", rows)
    update_meta("anbima_cap")

@dataset("google_trends")
def fetch_google_trends():
    print("📈 Google Trends...")
    rows = run_query("""
        SELECT
            TO_CHAR(DT_REFERENCIA, 'YYYY-MM-DD') AS DT_REFERENCIA,
            BU, TERMO, GRUPO_QUERY, ANCORA,
            VALOR_RELATIVO, VALOR_NORMALIZADO
        FROM TB_MS_GOOGLE_TRENDS
        WHERE GRUPO_QUERY NOT IN ('G1','G2')
        ORDER BY DT_REFERENCIA, TERMO
    """, cfg=SF_TRENDS)
    save("google_trends.json", rows)
    update_meta("google_trends")

@dataset("youtube_canais")
def fetch_youtube_canais():
    print("▶️  YouTube Canais...")
    rows = run_query("""
        SELECT
            TO_CHAR(DATE_TRUNC('MONTH', DT_SNAPSHOT), 'YYYY-MM-DD') AS MES,
            CANAL_NOME, CANAL_HANDLE, TIPO, BU,
            MAX(SUBSCRIBERS)  AS SUBSCRIBERS,
            MAX(VIEW_COUNT)   AS VIEW_COUNT,
            MAX(VIDEO_COUNT)  AS VIDEO_COUNT
        FROM RAW_MARKETING.MARKET_SHARE.TB_MS_YOUTUBE_CHANNEL_STATS_DAILY
        GROUP BY 1,2,3,4,5
        ORDER BY 1, SUBSCRIBERS DESC
    """)
    save("youtube_canais.json", rows)
    update_meta("youtube_canais")

@dataset("youtube_videos")
def fetch_youtube_videos():
    print("▶️  YouTube Vídeos Recentes...")
    rows = run_query("""
        SELECT
            TO_CHAR(DT_SNAPSHOT, 'YYYY-MM-DD') AS DT_SNAPSHOT,
            CANAL_NOME, TIPO, TITLE, TO_CHAR(PUBLISHED_AT, 'YYYY-MM-DD') AS PUBLISHED_AT,
            DURATION_SECONDS, IS_SHORT, VIEW_COUNT, LIKE_COUNT,
            COMMENT_COUNT, ENGAGEMENT_RATE
        FROM RAW_MARKETING.MARKET_SHARE.TB_MS_YOUTUBE_VIDEO_RECENT
        WHERE DT_SNAPSHOT = (SELECT MAX(DT_SNAPSHOT) FROM RAW_MARKETING.MARKET_SHARE.TB_MS_YOUTUBE_VIDEO_RECENT)
        ORDER BY VIEW_COUNT DESC
        LIMIT 200
    """)
    save("youtube_videos.json", rows)
    update_meta("youtube_videos")

@dataset("youtube_top")
def fetch_youtube_top():
    print("▶️  YouTube Top Vídeos...")
    rows = run_query("""
        SELECT
            TO_CHAR(DT_SNAPSHOT, 'YYYY-MM-DD') AS DT_SNAPSHOT,
            CANAL_NOME, TIPO, TITLE, TO_CHAR(PUBLISHED_AT, 'YYYY-MM-DD') AS PUBLISHED_AT,
            DURATION_SECONDS, IS_SHORT, VIEW_COUNT, LIKE_COUNT,
            COMMENT_COUNT, ENGAGEMENT_RATE, SEARCH_RANK
        FROM RAW_MARKETING.MARKET_SHARE.TB_MS_YOUTUBE_VIDEO_TOP_VIEWED
        WHERE DT_SNAPSHOT = (SELECT MAX(DT_SNAPSHOT) FROM RAW_MARKETING.MARKET_SHARE.TB_MS_YOUTUBE_VIDEO_TOP_VIEWED)
        ORDER BY VIEW_COUNT DESC
        LIMIT 200
    """)
    save("youtube_top.json", rows)
    update_meta("youtube_top")

@dataset("youtube_trending")
def fetch_youtube_trending():
    print("▶️  YouTube Trending BR...")
    rows = run_query("""
        SELECT
            TO_CHAR(DT_SNAPSHOT, 'YYYY-MM-DD') AS DT_SNAPSHOT,
            POSITION, CANAL_NOME, IS_MONITORED, TIPO,
            TITLE, TO_CHAR(PUBLISHED_AT, 'YYYY-MM-DD') AS PUBLISHED_AT,
            DURATION_SECONDS, IS_SHORT, VIEW_COUNT, LIKE_COUNT,
            COMMENT_COUNT, ENGAGEMENT_RATE, IS_FINANCE_RELATED
        FROM RAW_MARKETING.MARKET_SHARE.TB_MS_YOUTUBE_TRENDING_BR
        WHERE DT_SNAPSHOT = (SELECT MAX(DT_SNAPSHOT) FROM RAW_MARKETING.MARKET_SHARE.TB_MS_YOUTUBE_TRENDING_BR)
        ORDER BY POSITION
    """)
    save("youtube_trending.json", rows)
    update_meta("youtube_trending")

@dataset("reputacao_apps")
def fetch_reputacao_apps():
    print("⭐ App Stores...")
    rows = run_query("""
        SELECT
            TO_CHAR(DT_REFERENCIA, 'YYYY-MM-DD') AS DT_REFERENCIA,
            EMPRESA, NOTA_GOOGLE, AVALIACOES_GOOGLE, NOTA_IOS, AVALIACOES_IOS
        FROM RAW_MARKETING.MARKET_SHARE.TB_APPS_EMPRESAS
        ORDER BY DT_REFERENCIA DESC, EMPRESA
    """)
    save("reputacao_apps.json", rows)
    update_meta("reputacao_apps")

@dataset("reputacao_reclame")
def fetch_reputacao_reclame():
    print("💬 Reclame Aqui...")
    rows = run_query("""
        SELECT
            TO_CHAR(DT_REFERENCIA, 'YYYY-MM-DD') AS DT_REFERENCIA,
            EMPRESA, R1000, NOTA_6M, RECLAMACOES,
            RESPOSTAS_PCT, NOTA_RECLAMACAO, VOLTARIA_PCT,
            RESOLVEU_PCT, TEMPO_RESPOSTA
        FROM RAW_MARKETING.MARKET_SHARE.TB_RA_EMPRESAS
        ORDER BY DT_REFERENCIA DESC, EMPRESA
    """)
    save("reputacao_reclame.json", rows)
    update_meta("reputacao_reclame")

@dataset("reputacao_glassdoor")
def fetch_reputacao_glassdoor():
    print("👔 Glassdoor...")
    rows = run_query("""
        SELECT
            TO_CHAR(DT_REFERENCIA, 'YYYY-MM-DD') AS DT_REFERENCIA,
            EMPRESA, NOTA, RECOMENDA_PCT, QUANTIDADE
        FROM RAW_MARKETING.MARKET_SHARE.TB_GLASSDOOR_EMPRESAS
        ORDER BY DT_REFERENCIA DESC, EMPRESA
    """)
    save("reputacao_glassdoor.json", rows)
    update_meta("reputacao_glassdoor")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--datasets", default="all",
        help="Datasets separados por vírgula ou 'all'")
    args = parser.parse_args()

    if args.datasets == "all":
        selected = list(DATASETS.keys())
    else:
        selected = [d.strip() for d in args.datasets.split(",")]

    print(f"\n🚀 Atualizando: {', '.join(selected)}\n")
    errors = []

    for name in selected:
        if name not in DATASETS:
            print(f"  ⚠️  Dataset '{name}' não encontrado. Disponíveis: {list(DATASETS.keys())}")
            continue
        try:
            DATASETS[name]()
        except Exception as e:
            print(f"  ❌ Erro em '{name}': {e}")
            errors.append(name)
            # Atualiza meta com status de erro
            ds_key = f"trafego_{name}" if name in ("portais","noticias") else name
            update_meta(ds_key, status="erro", error=str(e))

    print(f"\n{'✅ Tudo atualizado!' if not errors else f'⚠️  Erros em: {errors}'}\n")
    if errors:
        sys.exit(1)

if __name__ == "__main__":
    main()
