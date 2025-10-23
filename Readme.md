# 2GIS Reviews Scraper

This Python script scrapes restaurant reviews from 2GIS for a specified city (default: Tashkent) using Selenium WebDriver. It processes an input Excel file containing organization details (e.g., IDs, names, phone numbers, coordinates) and collects reviews, saving them to CSV files.

## Features
- Scrapes reviews from 2GIS firm/branch pages.
- Supports search by organization ID, name, or phone number.
- Handles pagination and dynamic content loading.
- Saves progress incrementally to CSV files.
- Configurable parameters (e.g., headless mode, request rate, timeouts).

## Prerequisites
- Python 3.8+
- Google Chrome browser
- Required Python packages: `selenium`, `webdriver_manager`, `pandas`, `openpyxl`

## Installation
1. Clone or download the repository.
   ```bash
   git clone https://github.com/uroplatus666/2gis_reviews_scraper.git
   cd 2gis_reviews_scraper
   ```
3. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   .venv\Scripts\Activate
   ```
4. Install dependencies:
   ```bash
   pip install selenium webdriver_manager pandas openpyxl
   ```

## Usage
1. Ensure an input Excel file (e.g., `Ташкент_рестораны.xlsx`) is present in the script directory with columns for organization ID, name, phone, latitude, and longitude.
2. Run the script:
   ```bash
   python scrape_gis.py
   ```
3. Output:
   - Progress is saved to `2gis_reviews_progress.csv`.
   - Chunked results are saved in the `out` directory as `2gis_reviews_chunk_*.csv`.

## Configuration
Edit the script's constants to customize behavior:
- `EXCEL_PATH`: Path to the input Excel file.
- `CITY_SLUG`: City slug for 2GIS URLs (default: `tashkent`).
- `HEADLESS`: Set to `True` for headless browser mode.
- `REQUESTS_PER_MIN`: Controls scraping rate to avoid blocking.
- `VERBOSE`: Enable/disable detailed logging.

## Notes
- The script uses a Chrome profile for persistent sessions (`chrome-profile-2gis` directory).
- Debug HTML and screenshots are saved for problematic pages.
- Ensure a stable internet connection to avoid timeouts.
