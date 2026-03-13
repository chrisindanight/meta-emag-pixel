# Meta CAPI Enhanced Tracking — Suplimente

## Fisiere

| Fisier | Unde merge | Scop |
|--------|-----------|------|
| `meta-pixel-enhanced.js` | Shopify `theme.liquid` → `<head>` | Pixel browser-side, 40+ semnale |
| `cloudflare-worker.js` | Cloudflare Workers | CAPI proxy, IP real, geo |
| `sync.py` | GitHub repo root | Sync eMAG + Shopify → Meta |
| `requirements.txt` | GitHub repo root | Dependinte Python |
| `.github/workflows/sync.yml` | GitHub repo | Cron job 15 min |
| `shopify-integration.html` | Shopify theme + checkout | Snippets + checklist setup |

## Secrets necesare

### Cloudflare Worker (Settings → Variables → Encrypted)
- `META_CAPI_TOKEN`
- `META_DATASET_ID`
- `ALLOWED_ORIGIN` = https://tudomeniu.ro

### GitHub Actions (Settings → Secrets → Actions)
- `EMAG_USERNAME`
- `EMAG_PASSWORD`
- `SHOPIFY_STORE_URL`
- `SHOPIFY_API_KEY`
- `SHOPIFY_API_SECRET`
- `META_CAPI_TOKEN`
- `META_DATASET_ID`
- `META_PIXEL_ID`

## Ordinea de implementare
1. Deploy Cloudflare Worker (15 min)
2. Pixel enhanced in Shopify theme.liquid (30 min)
3. Repo GitHub + secrets + Actions (45 min)
4. Snippets Shopify checkout (15 min)
