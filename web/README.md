# Web app: prescriptions per 1,000 patients by deprivation decile

Static page. DuckDB-WASM reads `practice_month.parquet` in the browser, so there is no API and no
Databricks compute at view time. The pipeline's `export` stage publishes the data.

## Data location

The page loads `manifest.json` and `practice_month.parquet` from `./data` by default, or from
`?data=https://<your-r2-domain>`.

## Run locally with real data

```
databricks fs cp dbfs:/Volumes/workspace/epd/raw/export/practice_month.parquet web/data/
databricks fs cp dbfs:/Volumes/workspace/epd/raw/export/manifest.json web/data/
python3 -m http.server 8000 --directory web      # then open http://localhost:8000
```
(`web/data/` is git-ignored. Don't deploy it to Pages; data belongs in R2.)

## Publish to Cloudflare

1. **R2 bucket**: create one, then make it readable: a custom domain, or the `r2.dev` subdomain for testing.
2. **CORS** on the bucket, so the page may fetch from it (GET only; the app sends no Range header):
   ```json
   [{"AllowedOrigins": ["https://<project>.pages.dev", "http://localhost:8000"],
     "AllowedMethods": ["GET"], "AllowedHeaders": ["*"], "MaxAgeSeconds": 3600}]
   ```
3. **API token**: R2 token with Object Read & Write, scoped to that bucket.
4. **Databricks secrets** (the `export` stage reads scope `epd`):
   ```
   databricks secrets create-scope epd
   databricks secrets put-secret epd r2_account_id        --string-value <account id>
   databricks secrets put-secret epd r2_access_key_id     --string-value <key id>
   databricks secrets put-secret epd r2_secret_access_key --string-value <secret>
   databricks secrets put-secret epd r2_bucket            --string-value <bucket name>
   ```
   Without these the stage still writes the files to the volume and skips the upload.
5. **Pages**: `npx wrangler pages deploy web --project-name <name>`, then open
   `https://<project>.pages.dev/?data=https://<your-r2-domain>`. To drop the query string, change
   the `DATA_BASE` default at the top of `app.js`.

Files are cached for five minutes (`Cache-Control: max-age=300`), so a new export shows up quickly.
