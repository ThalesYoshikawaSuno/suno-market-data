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

ROOT   = Path(__file__).parent.parent
DATA   = ROOT / "data"
DATA.mkdir(exist_ok=True)

NOW    = datetime.now(timezone.utc)
NOW_BR = datetime.now(timezone(timedelta(hours=-3)))

SF = dict(
    account   = os.environ["SNOWFLAKE_ACCOUNT"],
    user      = os.environ["SNOWFLAKE_USERNAME"],
    password  = os.environ["SNOWFLAKE_PASSWORD"],
    database  = os.environ.get("SNOWFLAKE_DATABASE", "RAW_MARKETING"),
    schema    = os.environ.get("SNOWFLAKE_SCHEMA",   "MARKET_SHARE"),
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE","WH_AI_AGENTS"),
    role      = os.environ.get("SNOWFLAKE_ROLE",     "AI_AGENTS"),
)

SF_TRENDS = {**SF, "database": os.environ.get("SNOWFLAKE_DB_TRENDS","AI_WORKSPACE"), "schema": os.environ.get("SNOWFLAKE_SCHEMA_TRENDS","SANDBOX")}
SF_SERVING = {**SF, "database": "SERVING_LAYER", "schema": "MARKET_SHARE"}
SF_YOUTUBE = {**SF, "database": "SERVING_LAYER", "schema": "YOUTUBE"}

def run_query(sql, cfg=SF):
    conn = snowflake.connector.connect(**cfg)
    cur  = conn.cursor(snowflake.connector.DictCursor)
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        return [{k: (v.isoformat() if hasattr(v,'isoformat') else v) for k,v in row.items()} for row in rows]
    finally:
        cur.close(); conn.close()

def save(filename, data):
    path = DATA / filename
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"  ✅ {filename} — {len(data) if isinstance(data,list) else '...'} registros")

def next_update(freq):
    d = {"diario": 1, "semanal": 7, "quinzenal": 15, "mensal": 30}.get(freq, 7)
    return (NOW + timedelta(days=d)).strftime("%Y-%m-%dT%H:%M:%SZ")

def update_meta(dataset, status="ok", error=None):
    meta_path = DATA / "meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    if dataset in meta["datasets"]:
        freq = meta["datasets"][dataset]["frequencia"]
        meta["datasets"][dataset]["ultima_atualizacao"] = NOW.strftime("%Y-%m-%dT%H:%M:%SZ")
        meta["datasets"][dataset]["ultima_atualizacao_br"] = NOW_BR.strftime("%d/%m/%Y %H:%M")
        meta["datasets"][dataset]["proximo_update"] = next_update(freq)
        meta["datasets"][dataset]["status"] = status
        if error: meta["datasets"][dataset]["ultimo_erro"] = error[:200]
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

DATASETS = {}
def dataset(name):
    def decorator(fn):
        DATASETS[name] = fn
        return fn
    return decorator

# ── Tráfego ────────────────────────────────────────────────────────────────
@dataset("portais")
def fetch_portais():
    print("📡 Portais Financeiros...")
    rows = run_query("SELECT TO_CHAR(DT_REFERENCIA,'YYYY-MM-DD') AS DT_REFERENCIA, ANO, SEMANA, ANO_SEMANA, EMPRESA, VISITAS, SHARE FROM RAW_MARKETING.MARKET_SHARE.TB_TRAFEGO_SITES_NOTICIAS WHERE CATEGORIA='portais' ORDER BY DT_REFERENCIA, EMPRESA")
    save("trafego_portais.json", rows); update_meta("trafego_portais")

@dataset("noticias")
def fetch_noticias():
    print("🏢 Suno Portais...")
    rows = run_query("SELECT TO_CHAR(DT_REFERENCIA,'YYYY-MM-DD') AS DT_REFERENCIA, ANO, SEMANA, ANO_SEMANA, EMPRESA, VISITAS, SHARE FROM RAW_MARKETING.MARKET_SHARE.TB_TRAFEGO_SITES_NOTICIAS WHERE CATEGORIA='noticias' ORDER BY DT_REFERENCIA, EMPRESA")
    save("trafego_noticias.json", rows); update_meta("trafego_noticias")

# ── ANBIMA ─────────────────────────────────────────────────────────────────
@dataset("anbima_pl")
def fetch_anbima_pl():
    print("💰 ANBIMA PL...")
    rows = run_query("SELECT TO_CHAR(DT_REFERENCIA,'YYYY-MM-DD') AS DT_REFERENCIA, TIPO_INSTITUICAO, GESTOR, TIPO_VISAO, COLUNA_ORIGEM, VALOR FROM RAW_MARKETING.MARKET_SHARE.TB_ANBIMA_PL_RAW ORDER BY DT_REFERENCIA DESC, VALOR DESC")
    save("anbima_pl.json", rows); update_meta("anbima_pl")

@dataset("anbima_cap")
def fetch_anbima_cap():
    print("💰 ANBIMA Captação...")
    rows = run_query("SELECT TO_CHAR(DT_REFERENCIA,'YYYY-MM-DD') AS DT_REFERENCIA, TIPO_INSTITUICAO, GESTOR, JANELA, TIPO_VISAO, COLUNA_ORIGEM, VALOR FROM RAW_MARKETING.MARKET_SHARE.TB_ANBIMA_CAPTACAO_RAW ORDER BY DT_REFERENCIA DESC, VALOR DESC")
    save("anbima_cap.json", rows); update_meta("anbima_cap")

# ── Google Trends ──────────────────────────────────────────────────────────
@dataset("google_trends")
def fetch_google_trends():
    print("📈 Google Trends...")
    rows = run_query("SELECT TO_CHAR(DT_REFERENCIA,'YYYY-MM-DD') AS DT_REFERENCIA, BU, TERMO, GRUPO_QUERY, ANCORA, VALOR_RELATIVO, VALOR_NORMALIZADO FROM TB_MS_GOOGLE_TRENDS WHERE GRUPO_QUERY NOT IN ('G1','G2') ORDER BY DT_REFERENCIA, TERMO", cfg=SF_TRENDS)
    save("google_trends.json", rows); update_meta("google_trends")

# ── YouTube Market Share (novas views SERVING_LAYER) ──────────────────────
@dataset("youtube_canais")
def fetch_youtube_canais():
    print("▶️  YouTube Canais (Market Share Daily)...")
    rows = run_query("""
        SELECT TO_CHAR(DT_SNAPSHOT,'YYYY-MM-DD') AS DT_SNAPSHOT,
               CANAL_ID, CANAL_NOME, CANAL_HANDLE, TIPO, BU,
               SUBSCRIBERS, VIEW_COUNT, VIDEO_COUNT,
               SHARE_SUBSCRIBERS_PCT, SHARE_VIEWS_PCT, DELTA_SUBSCRIBERS_DOD
        FROM SERVING_LAYER.MARKET_SHARE.VW_YT_MARKET_SHARE_DAILY
        WHERE DT_SNAPSHOT >= DATEADD('month', -12, CURRENT_DATE())
        ORDER BY DT_SNAPSHOT DESC, SUBSCRIBERS DESC
    """, cfg=SF_SERVING)
    save("youtube_canais.json", rows); update_meta("youtube_canais")

@dataset("youtube_mensal")
def fetch_youtube_mensal():
    print("▶️  YouTube Mensal (Market Share Monthly)...")
    rows = run_query("""
        SELECT TO_CHAR(MES_REFERENCIA,'YYYY-MM-DD') AS MES_REFERENCIA,
               TO_CHAR(DT_ULTIMO_SNAPSHOT_MES,'YYYY-MM-DD') AS DT_ULTIMO_SNAPSHOT_MES,
               CANAL_ID, CANAL_NOME, CANAL_HANDLE, TIPO, BU,
               SUBSCRIBERS, VIEW_COUNT, VIDEO_COUNT,
               DELTA_SUBSCRIBERS_MOM, SHARE_SUBSCRIBERS_PCT
        FROM SERVING_LAYER.MARKET_SHARE.VW_YT_MARKET_SHARE_MONTHLY
        WHERE MES_REFERENCIA >= DATEADD('month', -12, CURRENT_DATE())
        ORDER BY MES_REFERENCIA DESC, SUBSCRIBERS DESC
    """, cfg=SF_SERVING)
    save("youtube_mensal.json", rows); update_meta("youtube_mensal")

@dataset("youtube_videos")
def fetch_youtube_videos():
    print("▶️  YouTube Vídeos Recentes...")
    rows = run_query("""
        SELECT TO_CHAR(DT_SNAPSHOT,'YYYY-MM-DD') AS DT_SNAPSHOT,
               VIDEO_ID, CANAL_ID, CANAL_NOME, TIPO, TITLE,
               TO_CHAR(PUBLISHED_AT,'YYYY-MM-DD') AS PUBLISHED_AT,
               DAYS_SINCE_PUBLISHED, DURATION_SECONDS, IS_SHORT,
               VIEW_COUNT, LIKE_COUNT, COMMENT_COUNT, ENGAGEMENT_RATE,
               TOPIC_CATEGORIES
        FROM SERVING_LAYER.MARKET_SHARE.VW_YT_VIDEO_RECENT
        WHERE DT_SNAPSHOT = (SELECT MAX(DT_SNAPSHOT) FROM SERVING_LAYER.MARKET_SHARE.VW_YT_VIDEO_RECENT)
        ORDER BY VIEW_COUNT DESC
    """, cfg=SF_SERVING)
    save("youtube_videos.json", rows); update_meta("youtube_videos")

@dataset("youtube_top")
def fetch_youtube_top():
    print("▶️  YouTube Top Vídeos...")
    rows = run_query("""
        SELECT TO_CHAR(DT_SNAPSHOT,'YYYY-MM-DD') AS DT_SNAPSHOT,
               VIDEO_ID, CANAL_ID, CANAL_NOME, TIPO, SEARCH_RANK, TITLE,
               TO_CHAR(PUBLISHED_AT,'YYYY-MM-DD') AS PUBLISHED_AT,
               DAYS_SINCE_PUBLISHED, IS_EVERGREEN, DURATION_SECONDS, IS_SHORT,
               VIEW_COUNT, LIKE_COUNT, COMMENT_COUNT, ENGAGEMENT_RATE,
               TOPIC_CATEGORIES
        FROM SERVING_LAYER.MARKET_SHARE.VW_YT_VIDEO_TOP_VIEWED
        WHERE DT_SNAPSHOT = (SELECT MAX(DT_SNAPSHOT) FROM SERVING_LAYER.MARKET_SHARE.VW_YT_VIDEO_TOP_VIEWED)
        ORDER BY VIEW_COUNT DESC
    """, cfg=SF_SERVING)
    save("youtube_top.json", rows); update_meta("youtube_top")

@dataset("youtube_trending")
def fetch_youtube_trending():
    print("▶️  YouTube Trending BR (finanças)...")
    rows = run_query("""
        SELECT TO_CHAR(DT_SNAPSHOT,'YYYY-MM-DD') AS DT_SNAPSHOT,
               POSITION, VIDEO_ID, CANAL_ID, CANAL_NOME, IS_MONITORED, TIPO,
               TITLE, TO_CHAR(PUBLISHED_AT,'YYYY-MM-DD') AS PUBLISHED_AT,
               DURATION_SECONDS, IS_SHORT, VIEW_COUNT, LIKE_COUNT,
               COMMENT_COUNT, ENGAGEMENT_RATE, IS_FINANCE_RELATED, IS_TECH_RELATED
        FROM SERVING_LAYER.MARKET_SHARE.VW_YT_TRENDING_FINANCE_BR
        WHERE DT_SNAPSHOT = (SELECT MAX(DT_SNAPSHOT) FROM SERVING_LAYER.MARKET_SHARE.VW_YT_TRENDING_FINANCE_BR)
        ORDER BY POSITION
    """, cfg=SF_SERVING)
    save("youtube_trending.json", rows); update_meta("youtube_trending")

# ── YouTube Performance Interna (canais próprios Suno) ─────────────────────
@dataset("youtube_interno_historico")
def fetch_youtube_interno_historico():
    print("▶️  YouTube Interno — Histórico diário...")
    rows = run_query("""
        SELECT CAST(DIA_DESEMPENHO AS VARCHAR) AS DIA_DESEMPENHO,
               CHANNEL_NAME, SUBSCRIBERS, NET_SUBSCRIBERS
        FROM SERVING_LAYER.YOUTUBE.INSCRITOS_CANAL_HISTORICO
        WHERE DIA_DESEMPENHO >= DATEADD('month', -12, CURRENT_DATE())
        ORDER BY DIA_DESEMPENHO DESC, SUBSCRIBERS DESC
    """, cfg=SF_YOUTUBE)
    save("youtube_interno_historico.json", rows); update_meta("youtube_interno_historico")

@dataset("youtube_interno_mensal")
def fetch_youtube_interno_mensal():
    print("▶️  YouTube Interno — Mensal...")
    rows = run_query("""
        SELECT CAST(MES AS VARCHAR) AS MES, CHANNEL_NAME, SUBSCRIBERS
        FROM SERVING_LAYER.YOUTUBE.INSCRITOS_CANAL_POR_MES
        ORDER BY MES DESC, SUBSCRIBERS DESC
    """, cfg=SF_YOUTUBE)
    save("youtube_interno_mensal.json", rows); update_meta("youtube_interno_mensal")

@dataset("youtube_interno_videos")
def fetch_youtube_interno_videos():
    print("▶️  YouTube Interno — Métricas de vídeos...")
    rows = run_query("""
        SELECT CAST(DATE AS VARCHAR) AS DATE,
               VIDEO_ID, CHANNEL_ID, CHANNEL_NAME, VIDEO_TITLE,
               CAST(DATA_PUBLICACAO AS VARCHAR) AS DATA_PUBLICACAO,
               CREATORCONTENTTYPE, VIEWS, LIKES, DISLIKES, SHARES, COMMENTS,
               TIME_WATCHED, SUBSCRIBERSGAINED, SUBSCRIBERSLOST
        FROM SERVING_LAYER.YOUTUBE.VIDEOS_METRICS_INFO
        WHERE DATE >= DATEADD('month', -12, CURRENT_DATE())
        ORDER BY DATE DESC, VIEWS DESC
        LIMIT 5000
    """, cfg=SF_YOUTUBE)
    save("youtube_interno_videos.json", rows); update_meta("youtube_interno_videos")

# ── Reputação ──────────────────────────────────────────────────────────────
@dataset("reputacao_apps")
def fetch_reputacao_apps():
    print("⭐ App Stores...")
    rows = run_query("SELECT TO_CHAR(DT_REFERENCIA,'YYYY-MM-DD') AS DT_REFERENCIA, EMPRESA, NOTA_GOOGLE, AVALIACOES_GOOGLE, NOTA_IOS, AVALIACOES_IOS FROM RAW_MARKETING.MARKET_SHARE.TB_APPS_EMPRESAS ORDER BY DT_REFERENCIA DESC, EMPRESA")
    save("reputacao_apps.json", rows); update_meta("reputacao_apps")

@dataset("reputacao_reclame")
def fetch_reputacao_reclame():
    print("💬 Reclame Aqui...")
    rows = run_query("SELECT TO_CHAR(DT_REFERENCIA,'YYYY-MM-DD') AS DT_REFERENCIA, EMPRESA, R1000, NOTA_6M, RECLAMACOES, RESPOSTAS_PCT, NOTA_RECLAMACAO, VOLTARIA_PCT, RESOLVEU_PCT, TEMPO_RESPOSTA FROM RAW_MARKETING.MARKET_SHARE.TB_RA_EMPRESAS ORDER BY DT_REFERENCIA DESC, EMPRESA")
    save("reputacao_reclame.json", rows); update_meta("reputacao_reclame")

@dataset("reputacao_glassdoor")
def fetch_reputacao_glassdoor():
    print("👔 Glassdoor...")
    rows = run_query("SELECT TO_CHAR(DT_REFERENCIA,'YYYY-MM-DD') AS DT_REFERENCIA, EMPRESA, NOTA, RECOMENDA_PCT, QUANTIDADE FROM RAW_MARKETING.MARKET_SHARE.TB_GLASSDOOR_EMPRESAS ORDER BY DT_REFERENCIA DESC, EMPRESA")
    save("reputacao_glassdoor.json", rows); update_meta("reputacao_glassdoor")

# ── Mídia Paga Asset ───────────────────────────────────────────────────────
@dataset("midia_asset")
def fetch_midia_asset():
    print("📢 Mídia Paga Asset...")
    rows = run_query("""
        SELECT TO_CHAR(DATA,'YYYY-MM-DD') AS DATA,
               BU, CAMPANHA, ORCADO, VALOR_PLANEJADO, ORCADO_BU,
               CUSTO_REALIZADO, DESVIO, PCT_REALIZADO
        FROM AI_WORKSPACE.SANDBOX.MS_VW_MIDIA_ASSET
        WHERE DATA >= DATEADD('month', -12, CURRENT_DATE())
        ORDER BY DATA, CAMPANHA
    """, cfg=SF_TRENDS)
    save("midia_asset.json", rows); update_meta("midia_asset")

# ── Vendas Líquidas Comercial ──────────────────────────────────────────────
@dataset("vendas_comercial")
def fetch_vendas_comercial():
    print("💼 Vendas Líquidas Comercial...")
    rows = run_query("""
        SELECT
            TO_CHAR(DATA_PAGAMENTO,'YYYY-MM-DD')      AS DATA_PAGAMENTO,
            PEDIDO_ID, PRODUTO_AREA_V2,
            AUDITORIA_PRODUTO_TITULO, AUDITORIA_PRODUTO_TIPO_NAME,
            AUDITORIA_OFERTA_NOME_AGG,
            PRECO_FINAL, PEDIDO_PRECO, PEDIDO_VALOR_DESCONTO,
            CLASSIFICACAO_DEVOLUCAO, TIPO_ENTRADA,
            CLASSIFICACAO_COMPRADOR_VS_META,
            PEDIDO_METODO_PAGAMENTO_DESC, PEDIDO_PARCELAS,
            CHANNEL_GROUP_VS_META, FONTE_PIPELINE,
            ASSINATURA_TIPO_DESC, MESES_DA_COMPRA
        FROM AI_WORKSPACE.SANDBOX.MS_VW_VENDAS_LIQUIDAS_COMERCIAL
        WHERE CLASSIFICACAO_DEVOLUCAO IN ('Completo','Devolução')
          AND DATA_PAGAMENTO >= DATEADD('month', -12, CURRENT_DATE())
        ORDER BY DATA_PAGAMENTO DESC
    """, cfg=SF_TRENDS)
    save("vendas_comercial.json", rows); update_meta("vendas_comercial")


@dataset("youtube_interno_diag")
def fetch_youtube_interno_diag():
    print("🔍 Diagnóstico colunas views internas...")
    for view, cfg_name in [
        ("SERVING_LAYER.YOUTUBE.INSCRITOS_CANAL_HISTORICO", "SF_YOUTUBE"),
        ("SERVING_LAYER.YOUTUBE.INSCRITOS_CANAL_POR_MES", "SF_YOUTUBE"),
        ("SERVING_LAYER.YOUTUBE.VIDEOS_METRICS_INFO", "SF_YOUTUBE"),
    ]:
        try:
            rows = run_query(f"SELECT * FROM {view} LIMIT 1", cfg=SF_YOUTUBE)
            if rows:
                print(f"  {view}: {list(rows[0].keys())}")
            else:
                print(f"  {view}: sem dados")
        except Exception as e:
            print(f"  {view}: ERRO — {e}")

# ── Main ───────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--datasets", default="all")
    args = parser.parse_args()
    selected = list(DATASETS.keys()) if args.datasets == "all" else [d.strip() for d in args.datasets.split(",")]
    print(f"\n🚀 Atualizando: {', '.join(selected)}\n")
    errors = []
    for name in selected:
        if name not in DATASETS:
            print(f"  ⚠️ Dataset '{name}' não encontrado. Disponíveis: {list(DATASETS.keys())}"); continue
        try:
            DATASETS[name]()
        except Exception as e:
            print(f"  ❌ Erro em '{name}': {e}")
            errors.append(name)
    print(f"\n{'✅ Tudo atualizado!' if not errors else f'⚠️  Erros em: {errors}'}\n")
    if errors: sys.exit(1)

if __name__ == "__main__":
    main()
