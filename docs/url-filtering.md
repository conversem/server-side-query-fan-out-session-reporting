# URL Filtering

The pipeline filters URLs by resource type before storing data. This keeps reporting tables clean by dropping non-user-facing assets (JavaScript, CSS, fonts) and classifying kept URLs as either documents or images.

## Why Filter URLs?

LLM bots request all types of resources — not just web pages. A single page visit may trigger requests for dozens of JS chunks, CSS files, and font files. Without filtering, these technical assets dominate your reporting tables and obscure the actual content bots are accessing.

Filtering is applied **before storage**, which means:
- Non-user-facing URLs never reach BigQuery or SQLite
- Storage costs are reduced
- Reporting views and dashboards show only meaningful data

## How It Works

URL classification follows three steps (in order):

1. **Extension check** — If the URL ends with a known drop extension (`.js`, `.css`, `.woff2`, etc.), the record is dropped.
2. **Image check** — If the URL ends with a known image extension (`.jpg`, `.png`, `.svg`, etc.), it's kept with `resource_type = "image"`.
3. **Path prefix check** — If the URL path starts with a known asset prefix (`/assets/js/`, `/static/`, etc.), the record is dropped.
4. **Default** — Everything else is kept with `resource_type = "document"`.

This runs consistently across all four processing modes (`local_sqlite`, `local_bq_buffered`, `local_bq_streaming`, `gcp_bq`).

## Configuration

All filtering rules are configurable via `config.yaml` (or environment variables). The defaults work well for most websites.

### YAML Configuration

```yaml
url_filtering:
  enabled: true

  drop_extensions:
    - js
    - mjs
    - css
    - map
    - woff
    - woff2
    - ttf
    - eot
    - otf
    - ico

  image_extensions:
    - jpg
    - jpeg
    - png
    - gif
    - svg
    - webp
    - avif
    - bmp

  drop_path_prefixes:
    - /assets/js/
    - /assets/css/
    - /static/
    - /_next/
    - /chunks/
    - /vendor/
    - /node_modules/
    - /__/
```

### Environment Variables

```bash
URL_FILTERING_ENABLED=true
URL_FILTERING_DROP_EXTENSIONS=js,mjs,css,map,woff,woff2,ttf,eot,otf,ico
URL_FILTERING_IMAGE_EXTENSIONS=jpg,jpeg,png,gif,svg,webp,avif,bmp
URL_FILTERING_DROP_PATH_PREFIXES=/assets/js/,/assets/css/,/static/,/_next/,/chunks/,/vendor/,/node_modules/,/__/
```

## The `resource_type` Column

Records that pass filtering get a `resource_type` column in `bot_requests_daily`:

| Value | Meaning | Examples |
|-------|---------|---------|
| `document` | User-facing page or file | `/about`, `/blog/article`, `/report.pdf` |
| `image` | Image file | `/images/hero.jpg`, `/logo.svg` |

### Querying by Resource Type

```sql
-- Only document pages (exclude images)
SELECT * FROM bot_requests_daily
WHERE resource_type = 'document'

-- Include images
SELECT * FROM bot_requests_daily
WHERE resource_type IN ('document', 'image')
```

## Customizing for Your Website

### Adding site-specific asset paths

If your website uses a custom asset directory structure:

```yaml
url_filtering:
  drop_path_prefixes:
    - /assets/js/
    - /assets/css/
    - /static/
    - /cdn-cgi/         # Cloudflare internal paths
    - /wp-content/themes/mytheme/assets/  # WordPress theme assets
    - /build/           # Build output directory
```

### Adding custom drop extensions

```yaml
url_filtering:
  drop_extensions:
    - js
    - css
    - map
    - woff2
    - ts        # TypeScript source files (if exposed)
    - scss      # Sass source files (if exposed)
```

## Disabling Filtering

To process all URLs without filtering:

```yaml
url_filtering:
  enabled: false
```

When disabled, all URLs get `resource_type = "document"` and nothing is dropped.

## Migration

For existing deployments upgrading to a version with URL filtering:

1. The `resource_type` column is added to `bot_requests_daily` with a default value of `"document"`.
2. Existing rows retain `resource_type = "document"` — no data is modified.
3. New data processed after the upgrade will have correct classification and filtering applied.
4. No backfill is required, but you can run one if desired.
