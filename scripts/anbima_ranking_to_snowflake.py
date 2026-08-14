"""
scripts/anbima_ranking_to_snowflake.py
=======================================
ETL: ANBIMA Ranking de Gestores de Fundos de Investimento → Snowflake

Baixa o arquivo Excel publicado em:
  https://data.anbima.com.br/publicacoes/ranking-de-gestores-de-fundos-de-investimento

Parseia 4 abas e carrega em:
  RAW_MARKETING.MARKET_SHARE.TB_ANBIMA_PL_RAW
    colunas: DT_REFERENCIA, TIPO_INSTITUICAO, GESTOR, TIPO_VISAO, COLUNA_ORIGEM, VALOR

  RAW_MARKETING.MARKET_SHARE.TB_ANBIMA_CAPTACAO_RAW
    colunas: DT_REFERENCIA, TIPO_INSTITUICAO, GESTOR, JANELA, TIPO_VISAO, COLUNA_ORIGEM, VALOR

Uso:
  # Descoberta automática (scraping Strapi CMS da ANBIMA):
  python scripts/anbima_ranking_to_snowflake.py

  # URL direta (mais confiável para DAG):
  python scripts/anbima_ranking_to_snowflake.py --url "https://www.anbima.com.br/data/files/XX/.../Ranking de Gestao - 202507_valor.xls"

  # Arquivo local já baixado:
  python scripts/anbima_ranking_to_snowflake.py --file /tmp/ranking.xls --ano-mes 202507

  # Testar sem gravar no Snowflake:
  python scripts/anbima_ranking_to_snowflake.py --file /tmp/ranking.xls --dry-run

Variáveis de ambiente (mesmas do fetch_snowflake.py):
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USERNAME, SNOWFLAKE_PASSWORD
  SNOWFLAKE_DATABASE  (padrão: RAW_MARKETING)
  SNOWFLAKE_SCHEMA    (padrão: MARKET_SHARE)
  SNOWFLAKE_WAREHOUSE (padrão: WH_AI_AGENTS)
  SNOWFLAKE_ROLE      (padrão: AI_AGENTS)

Dependências (além de snowflake-connector-python):
  pip install requests pandas openpyxl xlrd
"""

import os
import re
import sys
import json
import logging
import argparse
import tempfile
from datetime import date
from pathlib import Path
from urllib.parse import unquote

import requests
import pandas as pd
import snowflake.connector

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Constantes ────────────────────────────────────────────────────────────────
PAGE_URL   = "https://data.anbima.com.br/publicacoes/ranking-de-gestores-de-fundos-de-investimento"
STRAPI_URL = "https://data-strapi.prd.anbima.com.br"

PL_TABLE   = "TB_ANBIMA_PL_RAW"
CAP_TABLE  = "TB_ANBIMA_CAPTACAO_RAW"

TIPO_INSTITUICAO = "IFINANCEIRAS"  # padrão histórico — garante consistência com dados de março

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://data.anbima.com.br/",
}

# ── Snowflake ─────────────────────────────────────────────────────────────────
def get_sf_conn():
    return snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USERNAME"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        database  = os.environ.get("SNOWFLAKE_DATABASE",  "RAW_MARKETING"),
        schema    = os.environ.get("SNOWFLAKE_SCHEMA",    "MARKET_SHARE"),
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "WH_AI_AGENTS"),
        role      = os.environ.get("SNOWFLAKE_ROLE",      "AI_AGENTS"),
    )


# ── Descoberta de URL ─────────────────────────────────────────────────────────
_EXCEL_RE = re.compile(
    r"https?://[^\s\"'<>]+[Rr]anking[^\s\"'<>]*[Gg]esta[oa][^\s\"'<>]*\.xlsx?",
    re.IGNORECASE,
)


def _safe_get(url: str) -> requests.Response | None:
    """GET com fallback SSL e absorção de erros de conexão."""
    try:
        return requests.get(url, headers=HTTP_HEADERS, timeout=30, verify=True)
    except requests.exceptions.SSLError:
        requests.packages.urllib3.disable_warnings()
        try:
            return requests.get(url, headers=HTTP_HEADERS, timeout=30, verify=False)
        except Exception:
            return None
    except Exception:
        return None


def _try_upload_files_api() -> str | None:
    """
    Busca o arquivo diretamente na API de mídia do Strapi (/api/upload/files).
    Mais confiável que APIs de conteúdo pois aponta para onde o arquivo realmente está.
    Retorna a URL completa ou None.
    """
    for term in ["gestao", "ranking"]:
        endpoint = (
            f"{STRAPI_URL}/api/upload/files"
            f"?filters[name][$containsi]={term}"
            "&sort[0]=createdAt:desc"
            "&pagination[pageSize]=10"
        )
        log.debug(f"  Upload API: {endpoint}")
        resp = _safe_get(endpoint)
        if not resp or resp.status_code != 200:
            continue
        try:
            payload = resp.json()
        except Exception:
            continue

        # Strapi v4 upload pode retornar lista direta ou { results: [...] }
        files = payload if isinstance(payload, list) else payload.get("results", payload.get("data", []))
        for f in (files if isinstance(files, list) else []):
            file_url = f.get("url", "")
            if not file_url:
                continue
            if not file_url.startswith("http"):
                file_url = f"{STRAPI_URL}{file_url}"
            if _is_excel_url(file_url):
                return file_url

    return None


def _try_playwright_scrape() -> str | None:
    """
    Renderiza a página com Playwright (headless Chromium) e extrai o link de download.
    O site da ANBIMA é um SPA — o HTML estático não contém o link; é necessário
    executar o JavaScript para que o React renderize o botão de download.
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        log.debug("  Playwright não instalado — pulando esta etapa")
        return None

    log.info("  Usando Playwright para renderizar a página da ANBIMA...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.goto(PAGE_URL, wait_until="networkidle", timeout=45_000)

            # Aguarda o botão de download aparecer (até 15s extras)
            try:
                page.wait_for_selector("a[download]", timeout=15_000)
            except PWTimeout:
                pass

            # Coleta todos os hrefs da página renderizada
            links = page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
            for link in links:
                if _is_excel_url(link) and (
                    "ranking" in link.lower() or "gestao" in link.lower() or "gestão" in link.lower()
                ):
                    return link

            # Fallback: regex no HTML renderizado
            html = page.content()
            m = _EXCEL_RE.search(html)
            if m:
                return m.group(0)
        except Exception as exc:
            log.warning(f"  Playwright falhou: {exc}")
        finally:
            browser.close()

    return None


def discover_download_url() -> str:
    """
    Tenta descobrir a URL do Excel mais recente via:
      1. API de uploads do Strapi (mais direta)
      2. Endpoints de conteúdo Strapi (fallback)
      3. Playwright — renderiza a página SPA com Chromium (mais confiável)
    """
    log.info("Buscando URL de download via API Strapi da ANBIMA...")

    # ── 1. API de uploads (mais confiável sem browser) ────────────────────────
    url = _try_upload_files_api()
    if url:
        log.info(f"  URL encontrada via upload API: {url}")
        return url

    # ── 2. Endpoints de conteúdo Strapi ──────────────────────────────────────
    candidates = [
        f"{STRAPI_URL}/api/publicacoes?filters[slug][$eq]=ranking-de-gestores-de-fundos-de-investimento&populate=*",
        f"{STRAPI_URL}/api/publicacoes?filters[titulo][$containsi]=ranking+gestores&populate=*&sort=publishedAt:desc&pagination[pageSize]=3",
        f"{STRAPI_URL}/api/publicacoes?populate=*&sort=publishedAt:desc&pagination[pageSize]=10",
        f"{STRAPI_URL}/api/rankings?populate=*&sort=data:desc&pagination[pageSize]=5",
        f"{STRAPI_URL}/api/arquivos?filters[tipo][$containsi]=ranking&populate=*&sort=createdAt:desc",
    ]

    for endpoint in candidates:
        log.debug(f"  Tentando: {endpoint}")
        resp = _safe_get(endpoint)
        if not resp or resp.status_code != 200:
            continue
        try:
            data = resp.json()
        except Exception:
            continue
        url = _extract_file_url_from_strapi(data)
        if url:
            log.info(f"  URL encontrada via conteúdo Strapi: {url}")
            return url

    # ── 3. Playwright (renderiza o SPA, extrai link do DOM) ───────────────────
    log.info("  Endpoints Strapi falharam — usando Playwright para renderizar a página...")
    url = _try_playwright_scrape()
    if url:
        log.info(f"  URL encontrada via Playwright: {url}")
        return url

    raise RuntimeError(
        "\n"
        "Não foi possível descobrir a URL do arquivo automaticamente.\n"
        "A ANBIMA usa hashes aleatórios nos links — acesse o site manualmente\n"
        "e copie a URL do botão de download:\n"
        f"  {PAGE_URL}\n\n"
        "Depois passe via --url:\n"
        "  python scripts/anbima_ranking_to_snowflake.py \\\n"
        "    --url 'https://data-strapi.prd.anbima.com.br/uploads/Ranking_de_Gestao_AAAAMM_valor_<hash>.xlsx'\n"
    )


def _extract_file_url_from_strapi(payload: dict) -> str | None:
    """
    Navega na resposta Strapi (formatos v3/v4) procurando uma URL de arquivo Excel.
    """
    items = payload.get("data", [])
    if isinstance(items, dict):
        items = [items]

    for item in items:
        attrs = item.get("attributes", item)

        # Procura em campos comuns de arquivo
        for field_name in ["arquivo", "file", "anexo", "planilha", "download", "excel", "documento"]:
            field = attrs.get(field_name)
            if not field:
                continue
            url = _resolve_strapi_file(field)
            if url and _is_excel_url(url):
                return url

        # Procura em campos de texto livre (pode ser URL direta)
        for field_name in ["url", "link", "href"]:
            val = attrs.get(field_name, "")
            if isinstance(val, str) and _is_excel_url(val):
                return val

    return None


def _resolve_strapi_file(field) -> str | None:
    """Resolve diferentes formatos de campo de arquivo Strapi para uma URL."""
    if isinstance(field, str) and field.startswith("http"):
        return field

    if isinstance(field, dict):
        # Strapi v4 shape: { data: { attributes: { url: ... } } }
        inner = field.get("data", field)
        if isinstance(inner, list) and inner:
            inner = inner[0]
        if isinstance(inner, dict):
            attrs = inner.get("attributes", inner)
            url = attrs.get("url", "")
            if url:
                return url if url.startswith("http") else f"{STRAPI_URL}{url}"

    return None


def _is_excel_url(url: str) -> bool:
    return bool(url) and any(url.lower().endswith(ext) for ext in [".xls", ".xlsx", ".xlsb"])


# ── Download ──────────────────────────────────────────────────────────────────
def download_excel(url: str) -> Path:
    """Baixa o Excel para um arquivo temporário e retorna o caminho."""
    log.info(f"Baixando arquivo: {url}")
    # verify=False necessário em ambientes corporativos com proxy SSL interceptor.
    # Em produção (DAG), configure REQUESTS_CA_BUNDLE ou passe --ssl-no-verify.
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=180, stream=True, verify=True)
    except requests.exceptions.SSLError:
        log.warning("SSL verification falhou — tentando com verify=False (ambiente corporativo)")
        requests.packages.urllib3.disable_warnings()
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=180, stream=True, verify=False)
    resp.raise_for_status()

    # Detecta extensão
    ext = ".xls"
    if url.lower().endswith(".xlsx"):
        ext = ".xlsx"
    ct = resp.headers.get("Content-Type", "")
    if "openxmlformats" in ct or "xlsx" in ct:
        ext = ".xlsx"

    tmp = Path(tempfile.mkdtemp()) / f"anbima_ranking{ext}"
    with open(tmp, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)

    size_kb = tmp.stat().st_size // 1024
    log.info(f"Salvo em {tmp}  ({size_kb} KB)")
    return tmp


# ── Extração de data de referência ────────────────────────────────────────────
def extract_ref_date(filepath: Path, url: str = "") -> date:
    """
    Extrai o mês de referência do nome do arquivo ou URL.
    Ex: "Ranking de Gestao - 202507_valor.xls" → 2025-07-01
    Usa findall para testar todos os matches (o hash da URL pode conter dígitos espúrios).
    """
    for source in [filepath.name, unquote(url), str(filepath)]:
        for m in re.finditer(r"(\d{4})(\d{2})", source):
            y, mo = int(m.group(1)), int(m.group(2))
            if 2000 <= y <= 2100 and 1 <= mo <= 12:
                log.info(f"Data de referência: {mo:02d}/{y}")
                return date(y, mo, 1)
    raise ValueError(
        f"Não foi possível detectar ano/mês em: {filepath.name!r}\n"
        "Use --ano-mes AAAAMM para especificar manualmente."
    )


# ── Leitura do Excel ──────────────────────────────────────────────────────────
def _engine(filepath: Path) -> str:
    return "openpyxl" if str(filepath).lower().endswith(".xlsx") else "xlrd"


def _list_sheets(filepath: Path) -> list[str]:
    xl = pd.ExcelFile(filepath, engine=_engine(filepath))
    log.info(f"Abas encontradas: {xl.sheet_names}")
    return xl.sheet_names


def _find_sheet_name(sheets: list[str], patterns: list[str]) -> str | None:
    """Retorna o nome da aba que contém algum dos padrões (case-insensitive)."""
    for sheet in sheets:
        for pat in patterns:
            if pat.lower() in sheet.lower():
                return sheet
    return None


def _read_raw(filepath: Path, sheet: str) -> pd.DataFrame:
    return pd.read_excel(filepath, sheet_name=sheet, header=None, engine=_engine(filepath))


def _find_header_row(df: pd.DataFrame, keywords: list[str] | None = None) -> int:
    """Retorna o índice da linha que contém a palavra-chave principal do cabeçalho."""
    kw = keywords or ["gestor", "total"]
    for i in range(min(30, len(df))):
        row_lower = df.iloc[i].astype(str).str.lower()
        if any(row_lower.str.contains(k).any() for k in kw):
            return i
    return 0


def _clean_col_name(name: str) -> str:
    """Limpa e normaliza um nome de coluna."""
    return str(name).strip().replace("\n", " ").replace("  ", " ")


def _to_float(val) -> float | None:
    """Converte valor para float, ignorando strings inválidas."""
    if pd.isna(val):
        return None
    s = str(val).strip().replace("\xa0", "").replace(" ", "")
    # Formato brasileiro: 1.234.567,89 → 1234567.89
    if re.match(r"^-?[\d.]+,\d+$", s):
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _is_skip_row(gestor: str) -> bool:
    """Verifica se uma linha deve ser ignorada (total, rodapé, cabeçalho, etc.)."""
    g = gestor.strip().upper()
    skip_exact = {"", "NAN", "TOTAL", "GESTOR", "GESTORES", "RANKING", "POSIÇÃO", "POSICAO"}
    if g in skip_exact:
        return True
    if re.match(r"^\d+$", g):           # linha só com número (ex: ranking position)
        return True
    if g.startswith("FONTE"):           # rodapé "Fonte: ANBIMA..."
        return True
    if g.startswith("OBS") or g.startswith("NOTA"):
        return True
    return False


# ── Parser: abas de PL ────────────────────────────────────────────────────────
def parse_pl_sheet(
    filepath: Path,
    sheet_patterns: list[str],
    tipo_visao: str,
    sheets: list[str],
) -> list[dict]:
    """
    Parseia uma aba de PL (por Classe, por Segmento ou por Estrutura).

    Layout esperado:
      Linha N:   [#] [Gestor] [Col1] [Col2] ... [Total]
      Linha N+1: [1] [Nome]   [val1] [val2] ... [valT]
      ...

    Retorna lista de dicts com: GESTOR, TIPO_VISAO, COLUNA_ORIGEM, VALOR
    """
    sheet = _find_sheet_name(sheets, sheet_patterns)
    if not sheet:
        log.warning(f"Aba não encontrada para padrões: {sheet_patterns}")
        return []

    df = _read_raw(filepath, sheet)
    log.info(f"Processando '{sheet}' ({tipo_visao}) — {df.shape[0]} linhas × {df.shape[1]} colunas")

    header_row = _find_header_row(df)
    headers = [_clean_col_name(h) for h in df.iloc[header_row].tolist()]

    # Identifica coluna do gestor e colunas de valor
    gestor_col: int | None = None
    value_cols: dict[int, str] = {}   # col_index → COLUNA_ORIGEM

    for ci, h in enumerate(headers):
        hl = h.lower()
        if hl in ["gestor", "gestores", "nome", "instituição", "instituicao"]:
            gestor_col = ci
        elif h and hl not in ["nan", "", "#", "r$", "%", "posição", "posicao",
                               "rank", "ranking", "ordem", "posição/variação",
                               "posicao/variacao", "var.", "variação", "variacao"]:
            value_cols[ci] = h

    if gestor_col is None:
        log.warning(f"  Coluna 'Gestor' não encontrada em '{sheet}' — tentando coluna 1")
        gestor_col = 1  # fallback: segunda coluna (após #)

    rows: list[dict] = []
    for ri in range(header_row + 1, len(df)):
        row = df.iloc[ri]
        gestor = str(row.iloc[gestor_col]).strip()
        if _is_skip_row(gestor):
            continue

        for ci, col_name in value_cols.items():
            val = _to_float(row.iloc[ci])
            if val is None:
                continue
            rows.append({
                "GESTOR":        gestor.upper(),
                "TIPO_VISAO":    tipo_visao,
                "COLUNA_ORIGEM": col_name,
                "VALOR":         val,
            })

    log.info(f"  → {len(rows)} linhas extraídas")
    return rows


# ── Parser: aba de Captação ───────────────────────────────────────────────────
def parse_captacao_sheet(filepath: Path, sheets: list[str]) -> list[dict]:
    """
    Parseia a aba de Captação (cabeçalho multi-nível: Janela × Classe).

    Layout esperado (2 linhas de header):
      Linha N:   [   ] [Gestor] [--- Mês ---]         [--- Ano ---]          [--- 12M ---]
      Linha N+1: [   ] [Gestor] [RF] [AÇ] [...] [Tot] [RF] [AÇ] [...] [Tot] [RF] ...
      Linha N+2: [1]   [Nome]   [v]  [v]  [...] [v]   [v]  ...

    Retorna lista de dicts com: GESTOR, JANELA, TIPO_VISAO, COLUNA_ORIGEM, VALOR
    """
    sheet = _find_sheet_name(sheets, ["captação", "captacao", "pag. 5", "pag 5", "cap"])
    if not sheet:
        log.warning("Aba de Captação não encontrada")
        return []

    df = _read_raw(filepath, sheet)
    log.info(f"Processando '{sheet}' (Captação) — {df.shape[0]} linhas × {df.shape[1]} colunas")

    # ── Localiza as 2 linhas de header ──────────────────────────────────────
    janela_row_idx: int | None = None
    classe_row_idx: int | None = None

    for i in range(min(30, len(df))):
        row_lower = df.iloc[i].astype(str).str.lower()
        # Janela: linha que contém "mês"/"mes", "ano", "12m"
        if (row_lower.str.contains(r"\bm[eê]s\b", regex=True).any()
                or row_lower.str.contains(r"\b12\s*m\b", regex=True).any()):
            janela_row_idx = i
        # Classe: linha que contém nomes de classes de ativos
        if (row_lower.str.contains("renda fixa").any()
                or row_lower.str.contains(r"a[çc][õo]es", regex=True).any()
                or row_lower.str.contains("multimercado").any()):
            classe_row_idx = i
            break

    if classe_row_idx is None:
        # Fallback: usa detecção genérica de header
        classe_row_idx = _find_header_row(df, ["gestor", "total", "renda"])
        janela_row_idx = max(0, classe_row_idx - 1)

    log.debug(f"  janela_row={janela_row_idx}, classe_row={classe_row_idx}")

    # ── Identifica coluna do gestor ──────────────────────────────────────────
    gestor_col = 1  # default
    if classe_row_idx is not None:
        for ci, v in enumerate(df.iloc[classe_row_idx]):
            if str(v).strip().lower() in ["gestor", "gestores"]:
                gestor_col = ci
                break

    # ── Monta mapa de colunas: col_idx → (JANELA, COLUNA_ORIGEM) ─────────────
    janelas_raw = df.iloc[janela_row_idx].tolist() if janela_row_idx is not None else []
    classes_raw = df.iloc[classe_row_idx].tolist() if classe_row_idx is not None else []

    # Forward-fill janelas (células mescladas)
    cur_janela = ""
    janela_map: dict[int, str] = {}
    for ci, v in enumerate(janelas_raw):
        v_str = _clean_col_name(str(v))
        if v_str and v_str.lower() not in ["nan", ""]:
            cur_janela = v_str
        janela_map[ci] = cur_janela

    value_cols: dict[int, tuple[str, str]] = {}   # col_idx → (JANELA, COLUNA_ORIGEM)
    for ci, cls in enumerate(classes_raw):
        cls_str = _clean_col_name(str(cls))
        if not cls_str or cls_str.lower() in ["nan", "", "gestor", "gestores", "#", "r$", "%"]:
            continue
        janela = janela_map.get(ci, "")
        if janela and ci != gestor_col:
            value_cols[ci] = (janela, cls_str)

    # ── Itera linhas de dados ────────────────────────────────────────────────
    rows: list[dict] = []
    data_start = (classe_row_idx or 0) + 1
    for ri in range(data_start, len(df)):
        row = df.iloc[ri]
        gestor = str(row.iloc[gestor_col]).strip()
        if _is_skip_row(gestor):
            continue

        for ci, (janela, col_name) in value_cols.items():
            val = _to_float(row.iloc[ci])
            if val is None:
                continue
            rows.append({
                "GESTOR":        gestor.upper(),
                "JANELA":        janela,
                "TIPO_VISAO":    "CATEGORIA",   # padrão histórico — garante consistência
                "COLUNA_ORIGEM": col_name,
                "VALOR":         val,
            })

    log.info(f"  → {len(rows)} linhas extraídas")
    return rows


# ── Orquestração do parse ──────────────────────────────────────────────────────
def parse_excel(
    filepath: Path,
    url: str = "",
    ref_date_override: date | None = None,
) -> tuple[list[dict], list[dict]]:
    """
    Parseia o arquivo Excel completo.
    Retorna (pl_rows, cap_rows) já com DT_REFERENCIA e TIPO_INSTITUICAO preenchidos.
    """
    ref_date = ref_date_override or extract_ref_date(filepath, url)
    dt_str   = ref_date.strftime("%Y-%m-%d")

    sheets = _list_sheets(filepath)

    # ── PL (3 abas) ──────────────────────────────────────────────────────────
    pl_rows: list[dict] = []
    # Nota: ANBIMA renomeou "PL por Classe" para "PL por Categoria" em 2024.
    # Os padrões cobrem ambos os nomes.
    pl_rows += parse_pl_sheet(filepath, ["pag. 2", "pag2", "pl por classe", "pl classe", "pl por categoria", "por categoria", "classe", "categoria"], "CATEGORIA", sheets)
    pl_rows += parse_pl_sheet(filepath, ["pag. 3", "pag3", "pl por segmento", "segmento"],                                                             "SEGMENTO", sheets)
    pl_rows += parse_pl_sheet(filepath, ["pag. 4", "pag4", "pl por estrut",   "estrutur"],                                                             "ESTRUTURA", sheets)

    for r in pl_rows:
        r["DT_REFERENCIA"]    = dt_str
        r["TIPO_INSTITUICAO"] = TIPO_INSTITUICAO

    # ── Captação (1 aba) ──────────────────────────────────────────────────────
    cap_rows = parse_captacao_sheet(filepath, sheets)
    for r in cap_rows:
        r["DT_REFERENCIA"]    = dt_str
        r["TIPO_INSTITUICAO"] = TIPO_INSTITUICAO

    log.info(f"Resumo: {len(pl_rows)} linhas PL | {len(cap_rows)} linhas Captação | ref {dt_str}")
    return pl_rows, cap_rows


# ── Carga no Snowflake ────────────────────────────────────────────────────────
def load_pl(conn, rows: list[dict]) -> int:
    """DELETE da competência + INSERT das novas linhas em TB_ANBIMA_PL_RAW."""
    if not rows:
        log.warning("Nenhuma linha PL para carregar.")
        return 0

    dt = rows[0]["DT_REFERENCIA"]
    cur = conn.cursor()

    cur.execute(
        f"DELETE FROM {PL_TABLE} WHERE DT_REFERENCIA = %s AND TIPO_INSTITUICAO = %s",
        (dt, TIPO_INSTITUICAO),
    )
    log.info(f"  {cur.rowcount} linhas removidas de {PL_TABLE} para {dt}")

    cur.executemany(
        f"""
        INSERT INTO {PL_TABLE}
            (DT_REFERENCIA, TIPO_INSTITUICAO, GESTOR, TIPO_VISAO, COLUNA_ORIGEM, VALOR)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        [
            (r["DT_REFERENCIA"], r["TIPO_INSTITUICAO"], r["GESTOR"],
             r["TIPO_VISAO"], r["COLUNA_ORIGEM"], r["VALOR"])
            for r in rows
        ],
    )
    conn.commit()
    log.info(f"  {len(rows)} linhas inseridas em {PL_TABLE}")
    return len(rows)


def load_captacao(conn, rows: list[dict]) -> int:
    """DELETE da competência + INSERT das novas linhas em TB_ANBIMA_CAPTACAO_RAW."""
    if not rows:
        log.warning("Nenhuma linha de Captação para carregar.")
        return 0

    dt = rows[0]["DT_REFERENCIA"]
    cur = conn.cursor()

    cur.execute(
        f"DELETE FROM {CAP_TABLE} WHERE DT_REFERENCIA = %s AND TIPO_INSTITUICAO = %s",
        (dt, TIPO_INSTITUICAO),
    )
    log.info(f"  {cur.rowcount} linhas removidas de {CAP_TABLE} para {dt}")

    cur.executemany(
        f"""
        INSERT INTO {CAP_TABLE}
            (DT_REFERENCIA, TIPO_INSTITUICAO, GESTOR, JANELA, TIPO_VISAO, COLUNA_ORIGEM, VALOR)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        [
            (r["DT_REFERENCIA"], r["TIPO_INSTITUICAO"], r["GESTOR"], r["JANELA"],
             r["TIPO_VISAO"], r["COLUNA_ORIGEM"], r["VALOR"])
            for r in rows
        ],
    )
    conn.commit()
    log.info(f"  {len(rows)} linhas inseridas em {CAP_TABLE}")
    return len(rows)


# ── CLI ───────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="ETL: ANBIMA Ranking de Gestores → Snowflake",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--url",
        help=(
            "URL direta do arquivo Excel no site da ANBIMA. "
            "Se omitida, o script tenta descobrir automaticamente via Strapi."
        ),
    )
    parser.add_argument(
        "--file",
        help="Caminho local de um arquivo Excel já baixado (pula o download).",
    )
    parser.add_argument(
        "--ano-mes",
        metavar="AAAAMM",
        help=(
            "Mês de referência no formato AAAAMM (ex: 202507). "
            "Se omitido, é extraído do nome do arquivo."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Processa e exibe as primeiras linhas, mas NÃO grava no Snowflake.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Ativa logs de DEBUG.",
    )
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # ── 1. Obter arquivo ─────────────────────────────────────────────────────
    if args.file:
        filepath = Path(args.file)
        if not filepath.exists():
            log.error(f"Arquivo não encontrado: {filepath}")
            sys.exit(1)
        url = args.url or ""
        log.info(f"Usando arquivo local: {filepath}")
    else:
        url = args.url or discover_download_url()
        filepath = download_excel(url)

    # ── 2. Data de referência ────────────────────────────────────────────────
    ref_date: date | None = None
    if args.ano_mes:
        am = args.ano_mes.strip()
        if not re.match(r"^\d{6}$", am):
            log.error("--ano-mes deve ter formato AAAAMM, ex: 202507")
            sys.exit(1)
        ref_date = date(int(am[:4]), int(am[4:]), 1)

    # ── 3. Parsear Excel ─────────────────────────────────────────────────────
    pl_rows, cap_rows = parse_excel(filepath, url=url, ref_date_override=ref_date)

    if not pl_rows and not cap_rows:
        log.error(
            "Nenhuma linha foi extraída do arquivo.\n"
            "Verifique se o layout do Excel mudou em relação ao esperado.\n"
            "Use --debug para ver logs detalhados."
        )
        sys.exit(1)

    # ── 4. Dry run ───────────────────────────────────────────────────────────
    if args.dry_run:
        log.info("[DRY RUN] Nada sera gravado no Snowflake.")
        print("\n--- Primeiras 5 linhas PL ---")
        print(json.dumps(pl_rows[:5], indent=2, ensure_ascii=True, default=str))
        print("\n--- Primeiras 5 linhas Captacao ---")
        print(json.dumps(cap_rows[:5], indent=2, ensure_ascii=True, default=str))
        return

    # ── 5. Carregar no Snowflake ─────────────────────────────────────────────
    log.info("Conectando ao Snowflake...")
    conn = get_sf_conn()
    try:
        pl_loaded  = load_pl(conn, pl_rows)
        cap_loaded = load_captacao(conn, cap_rows)
    finally:
        conn.close()

    log.info(
        f"\n✅  ETL concluído — "
        f"{pl_loaded} linhas PL + {cap_loaded} linhas Captação carregadas no Snowflake."
    )


if __name__ == "__main__":
    main()
