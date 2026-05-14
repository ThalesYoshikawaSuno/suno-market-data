# Suno Market Data

Repositório de dados do Market Share Intelligence Dashboard.  
Os dados são atualizados automaticamente via GitHub Actions e consumidos pelo dashboard no Vercel.

## Frequência de atualização

| Dataset | Frequência | Workflow | Horário BRT |
|---|---|---|---|
| YouTube (canais, vídeos, trending) | **Diário** | `update_daily.yml` | 06:00 todo dia |
| Portais Financeiros + Google Trends | **Semanal** | `update_weekly.yml` | 07:00 toda segunda |
| App Stores + Reclame Aqui + Glassdoor | **Quinzenal** | `update_quinzenal.yml` | 07:00 dias 1 e 15 |
| ANBIMA (PL + Captação) | **Mensal** | `update_mensal.yml` | 07:00 dia 5 |

## Como rodar manualmente

1. Acesse **Actions** no GitHub
2. Escolha o workflow
3. Clique em **Run workflow**

## Estrutura dos arquivos

```
data/
  meta.json                  ← status e última atualização de cada dataset
  trafego_portais.json
  trafego_noticias.json
  anbima_pl.json
  anbima_cap.json
  google_trends.json
  youtube_canais.json
  youtube_videos.json
  youtube_top.json
  youtube_trending.json
  reputacao_apps.json
  reputacao_reclame.json
  reputacao_glassdoor.json
```

## Secrets necessários no GitHub

Configure em **Settings → Secrets and variables → Actions**:

```
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USERNAME
SNOWFLAKE_PASSWORD
SNOWFLAKE_DATABASE       RAW_MARKETING
SNOWFLAKE_SCHEMA         MARKET_SHARE
SNOWFLAKE_WAREHOUSE      WH_AI_AGENTS
SNOWFLAKE_ROLE           AI_AGENTS
SNOWFLAKE_DB_TRENDS      AI_WORKSPACE
SNOWFLAKE_SCHEMA_TRENDS  SANDBOX
```
