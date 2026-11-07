# Helix Finance landing page

A self-contained static landing page for the Helix Finance data-engineering
portfolio project. It uses plain HTML, CSS, and JavaScript — no package
manager, framework, or build step.

## Preview locally

From the repository root:

```bash
python3 -m http.server 8080 --directory landing
```

Open <http://localhost:8080>.

## Deploy to Cloudflare Pages

The page is designed to be deployed with Cloudflare Pages Git integration.
There is no build output step: Pages should upload the `landing/` directory
as-is.

1. In the Cloudflare dashboard, open **Workers & Pages**.
2. Select **Create application** → **Pages** → **Connect to Git**.
3. Select the `Mo-Shakib/Helix-Finance` repository.
4. Configure the project:
   - **Production branch:** `landing-page`
   - **Framework preset:** None
   - **Build command:** leave blank (or `exit 0`)
   - **Build output directory:** `landing`
   - **Root directory:** leave at the repository root
5. Select **Save and Deploy**.

Every future push to `landing-page` will trigger a new production deployment.
If the Pages project already uses `main` as its production branch, this branch
will first appear as a preview deployment; change the production branch in
**Settings → Build & deployments** when you are ready to promote it.

The Cloudflare Pages build environment automatically reads `_headers` from the
output directory, so the security headers and long-lived asset caching are
applied without any extra configuration. `404.html` and `robots.txt` are also
picked up automatically.

## Files

| File | Purpose |
|---|---|
| `index.html` | Landing page markup and inline SVG icon sprite |
| `styles.css` | Responsive dark visual system; no external assets or fonts |
| `script.js` | Header scroll state, current year, copy-to-clipboard |
| `_headers` | Cloudflare Pages security and cache headers |
| `404.html` | Custom not-found page |
| `robots.txt` | Search crawler policy |
| `site.webmanifest` | Installable-site metadata |
| `assets/dashboard.png` | Screenshot of the Streamlit control room |
| `assets/favicon.svg` | Helix mark and favicon |

## Notes

- The landing page links to the repository, README, data dictionary, ETL
  orchestration, schema, and analytical SQL on `main`.
- The Streamlit dashboard is not hosted as part of this static site. The
  Quickstart terminal shows how to run it locally.
- All screenshots and metrics are from synthetic demo data.
