name: Geocode with Azure Maps

on:
  workflow_dispatch:  # Run manually from the Actions tab

jobs:
  geocode:
    runs-on: ubuntu-latest

    steps:
      # 1) Pull your repository files
      - name: Checkout
        uses: actions/checkout@v4

      # 2) Install Python 3.11
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      # 3) Install the few packages the script needs
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install pandas openpyxl requests

      # 4) Verify both required files are present at the REPO ROOT
      #    (same level as the .github folder)
      - name: Verify input files exist
        run: |
          echo "Listing repo files:"
          ls -la
          if [ ! -f "azure_maps_batch_geocode_and_merge.py" ]; then
            echo "❌ Missing: azure_maps_batch_geocode_and_merge.py at repo root"
            exit 1
          fi
          if [ ! -f "Locations_geocode_ready.xlsx" ]; then
            echo "❌ Missing: Locations_geocode_ready.xlsx at repo root"
            exit 1
          fi
          echo "✔️ Both files found."

      # 5) (Optional but helpful) Probe your Azure Maps key BEFORE running the big batch
      #    Expect a 202 Accepted here if the key is valid and the secret is available to the runner.
      - name: Probe Azure Maps key (sanity test)
        env:
          AZURE_MAPS_KEY: ${{ secrets.AZURE_MAPS_KEY }}
        run: |
          set -e
          echo '{"batchItems":[{"query":"1 Microsoft Way, Redmond, WA","countrySet":"US"}]}' > body.json
          curl -sS -D headers.txt -o /dev/null \
            -X POST "https://atlas.microsoft.com/search/address/batch/json?api-version=1.0&subscription-key=${AZURE_MAPS_KEY}" \
            -H "Content-Type: application/json" \
            --data @body.json
          echo "---- Azure Maps status line (expect 202 Accepted) ----"
          head -n1 headers.txt || true

      # 6) Run your batch geocoding script
      - name: Run batch geocoding
        env:
          AZURE_MAPS_KEY: ${{ secrets.AZURE_MAPS_KEY }}   # Set this secret in: Settings → Secrets and variables → Actions
        run: |
          python azure_maps_batch_geocode_and_merge.py

      # 7) Upload the updated workbook as a run artifact
      - name: Upload result workbook
        uses: actions/upload-artifact@v4
        with:
          name: geocoded-workbook
          path: Locations_geocode_ready.xlsx
