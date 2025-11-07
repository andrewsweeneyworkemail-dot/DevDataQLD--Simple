#!/usr/bin/env python3
"""End-to-end Development.i harvesting pipeline.

This script coordinates the following steps:

1. Download the Development.i CSV for the previous *N* days.
2. Parse the CSV to determine application numbers and addresses.
3. Visit each application's document library and download any DA Form PDFs.
4. Extract lightweight metadata from the downloaded PDFs and merge it back
   into the CSV to produce an enriched report.

The implementation relies on Playwright (Chromium) so that it can run in
GitHub Actions without needing ChromeDriver. The workflow mirrors the
behaviour of a local Selenium script that performed the same tasks.

Example usage::

    python -m scripts.dev_i_pipeline --days 30 --headless

The command above runs the entire pipeline headlessly, which is the
configuration used in CI.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import pdfplumber
from playwright.sync_api import Page, TimeoutError as PWTimeout, sync_playwright
from tqdm import tqdm


# ---------------------------------------------------------------------------
# Shared helpers from the CSV automation script
# ---------------------------------------------------------------------------

if __package__:
    from .dev_i_csv_last30 import (  # type: ignore[import-not-found]
        BASE_URL,
        DBG_DIR,
        OUT_DIR,
        SS_DIR,
        click_download_csv,
        date_range_ddmmyyyy,
        maybe_dismiss_banners,
        open_date_range,
        set_date_range,
        show_results,
        ss,
        wait_for_results,
    )
else:  # pragma: no cover - support running via ``python scripts/dev_i_pipeline.py``
    SCRIPT_DIR = Path(__file__).resolve().parent
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))
    from dev_i_csv_last30 import (  # type: ignore  # noqa: E402
        BASE_URL,
        DBG_DIR,
        OUT_DIR,
        SS_DIR,
        click_download_csv,
        date_range_ddmmyyyy,
        maybe_dismiss_banners,
        open_date_range,
        set_date_range,
        show_results,
        ss,
        wait_for_results,
    )


# ---------------------------------------------------------------------------
# Constants and utility helpers
# ---------------------------------------------------------------------------

DOC_URL_TEMPLATE = (
    "https://developmenti.brisbane.qld.gov.au/DocumentSearch/"
    "GetAllDocument?applicationId={}"
)

LOG_FILE = OUT_DIR / "download_log.csv"


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def clean_for_fs(raw: str, max_len: int = 120) -> str:
    """Sanitise text for use inside filenames."""

    if not raw:
        return "unnamed"
    safe = re.sub(r"[\\/*?:\"<>|]", "_", raw.strip())
    return safe[:max_len].strip(" .") or "unnamed"


def build_da_folder(app_no: str, address: str) -> Path:
    base = f"{app_no} - {clean_for_fs(address, 90)}" if address else app_no
    dest = OUT_DIR / base / "DA Form"
    ensure_dir(dest)
    return dest


def load_log() -> pd.DataFrame:
    if LOG_FILE.exists():
        try:
            return pd.read_csv(LOG_FILE, dtype=str)
        except Exception:
            pass
    return pd.DataFrame(columns=["app_no", "file_name", "file_path", "downloaded_at"])


def append_log(app_no: str, file_name: str, file_path: Path) -> None:
    df = load_log()
    new = pd.DataFrame(
        [
            {
                "app_no": app_no,
                "file_name": file_name,
                "file_path": str(file_path),
                "downloaded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        ]
    )
    df = pd.concat([df, new], ignore_index=True)
    df.to_csv(LOG_FILE, index=False)


def in_log(app_no: str, file_name: str) -> bool:
    if not LOG_FILE.exists():
        return False
    try:
        df = pd.read_csv(LOG_FILE, dtype=str)
    except Exception:
        return False
    return not df[(df["app_no"] == app_no) & (df["file_name"] == file_name)].empty


# ---------------------------------------------------------------------------
# CSV download helpers
# ---------------------------------------------------------------------------


def download_csv(page: Page, days: int, out_dir: Path) -> Path:
    """Download the Development.i CSV for the supplied date range."""

    ensure_dir(out_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"brisbane_last{days}d_{timestamp}.csv"

    print("[INFO] Opening Application Search…")
    page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)
    ss(page, "01_loaded")

    maybe_dismiss_banners(page)
    open_date_range(page)
    start, end = date_range_ddmmyyyy(days)
    print(f"[INFO] Applying date range {start} – {end}")
    if not set_date_range(page, start, end):
        raise RuntimeError("Unable to set the date range inputs.")
    show_results(page)
    if not wait_for_results(page):
        raise RuntimeError("Search results did not render in time.")
    if not click_download_csv(page, csv_path):
        raise RuntimeError("CSV download did not succeed.")
    print(f"[OK] CSV saved -> {csv_path}")
    return csv_path


# ---------------------------------------------------------------------------
# CSV parsing helpers
# ---------------------------------------------------------------------------


def get_applications(csv_path: Path) -> list[dict[str, str]]:
    """Extract unique application numbers and addresses from the CSV."""

    df = pd.read_csv(csv_path, dtype=str, low_memory=False)
    records: list[dict[str, str]] = []
    for _, row in df.iterrows():
        joined = " ".join(str(value) for value in row.values if pd.notna(value))
        match = re.search(r"(A00\d{6,})", joined)
        if not match:
            continue
        app_no = match.group(1)
        addr_columns = [c for c in df.columns if "address" in c.lower()]
        address = " ".join(str(row.get(col, "")) for col in addr_columns if pd.notna(row.get(col)))
        records.append({"app_no": app_no, "address": address.strip()})
    deduped = {rec["app_no"]: rec for rec in records}
    print(f"[INFO] Found {len(deduped)} unique applications in {csv_path.name}.")
    return list(deduped.values())


# ---------------------------------------------------------------------------
# Document library helpers
# ---------------------------------------------------------------------------


@dataclass
class DownloadResult:
    app_no: str
    file_name: str
    file_path: Path


def open_document_library(page: Page, app_no: str) -> None:
    url = DOC_URL_TEMPLATE.format(app_no)
    print(f"[INFO] Opening documents for {app_no}…")
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    try:
        page.wait_for_selector("body", timeout=30000)
    except PWTimeout:
        raise RuntimeError(f"Document library did not load for {app_no}.")
    page.wait_for_timeout(3000)
    try:
        page.select_option("select[name='logisticList_length']", value="100")
        page.wait_for_timeout(3000)
        print("[INFO] Page size set to 100 entries.")
    except Exception:
        print("[WARN] Could not adjust page size; continuing with defaults.")


def parse_onclick_arguments(onclick: str) -> Optional[tuple[str, str, str]]:
    match = re.search(r"fileDownload\('([^']+)',\s*'([^']+)',\s*'([^']+)'\)", onclick)
    if not match:
        return None
    return match.group(1), match.group(2), match.group(3)


def download_da_forms(
    page: Page,
    app_no: str,
    address: str,
    retry_limit: int,
) -> list[DownloadResult]:
    """Download DA Form documents for a single application."""

    open_document_library(page, app_no)
    rows = page.locator("table tr")
    results: list[DownloadResult] = []
    total_rows = rows.count()
    dest = build_da_folder(app_no, address)
    print(f"[INFO] {app_no}: scanning {total_rows} document rows…")

    for idx in range(total_rows):
        row = rows.nth(idx)
        try:
            text = row.inner_text(timeout=2000)
        except Exception:
            continue
        if not re.search(r"\bDA\s*Form\b", text, re.IGNORECASE):
            continue
        link = row.locator("a[onclick*='fileDownload']").first
        if link.count() == 0:
            continue
        onclick = link.get_attribute("onclick") or ""
        parsed = parse_onclick_arguments(onclick)
        if not parsed:
            continue
        file_id, file_name, file_type = parsed
        safe_name = clean_for_fs(file_name)
        final_path = dest / f"{app_no}_{safe_name}.{file_type.lower()}"
        if final_path.exists() or in_log(app_no, safe_name):
            continue

        for attempt in range(1, retry_limit + 1):
            try:
                with page.expect_download(timeout=90000) as download_info:
                    page.evaluate(
                        "([fid, fname, ftype]) => { fileDownload(fid, fname, ftype); }",
                        [file_id, file_name, file_type],
                    )
                download = download_info.value
                tmp_path = final_path.with_suffix(".partial")
                download.save_as(str(tmp_path))
                tmp_path.rename(final_path)
                append_log(app_no, safe_name, final_path)
                results.append(
                    DownloadResult(app_no=app_no, file_name=safe_name, file_path=final_path)
                )
                break
            except Exception as exc:  # pragma: no cover - defensive retries
                tmp_path = final_path.with_suffix(".partial")
                if tmp_path.exists():
                    tmp_path.unlink(missing_ok=True)
                print(
                    f"[WARN] Download failed for {app_no} / {file_name} "
                    f"(attempt {attempt}/{retry_limit}): {exc}"
                )
                page.wait_for_timeout(2000)
        else:
            print(f"[ERROR] Unable to download DA Form '{file_name}' for {app_no}.")

    print(f"[INFO] {app_no}: downloaded {len(results)} DA Form document(s).")
    return results


# ---------------------------------------------------------------------------
# Enrichment helpers
# ---------------------------------------------------------------------------


def extract_form_data(pdf_path: Path) -> dict[str, str]:
    text = ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                content = page.extract_text() or ""
                if content:
                    text += content + "\n"
    except Exception:
        return {}
    return {"RawText": text[:3000]}


def enrich_and_merge(csv_path: Path) -> Optional[Path]:
    print("[INFO] Enriching CSV using local DA Form PDFs…")
    df = pd.read_csv(csv_path, dtype=str, low_memory=False)
    forms = list(OUT_DIR.rglob("*.pdf"))
    if not forms:
        print("[WARN] No DA Forms found for enrichment.")
        return None

    enriched: list[dict[str, str]] = []
    for pdf_path in tqdm(forms, desc="Processing DA Forms", unit="pdf"):
        match = re.search(r"(A00\d{6,})", str(pdf_path))
        if not match:
            continue
        data = extract_form_data(pdf_path)
        if not data:
            continue
        data["Application_No"] = match.group(1)
        data["Source_File"] = str(pdf_path)
        enriched.append(data)

    if not enriched:
        print("[WARN] DA Forms were downloaded but no text could be extracted.")
        return None

    forms_df = pd.DataFrame(enriched)
    merged = df.copy()

    def _extract_app_no(row: pd.Series) -> str:
        joined = " ".join(str(value) for value in row.values if pd.notna(value))
        match = re.search(r"(A00\d{6,})", joined)
        return match.group(1) if match else ""

    merged["Application_No"] = merged.apply(_extract_app_no, axis=1)
    merged = pd.merge(merged, forms_df, on="Application_No", how="left")

    output_path = OUT_DIR / f"{csv_path.stem}_enriched_{datetime.now():%Y%m%d_%H%M%S}.csv"
    merged.to_csv(output_path, index=False)
    print(f"[OK] Enriched CSV saved -> {output_path}")
    return output_path


# ---------------------------------------------------------------------------
# CLI handling
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Development.i end-to-end pipeline")
    parser.add_argument("--days", type=int, default=30, help="Days back to query (default: 30).")
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run Chromium headlessly (default for CI).",
    )
    parser.add_argument("--headed", dest="headless", action="store_false", help="Show the browser UI.")
    parser.set_defaults(headless=True)
    parser.add_argument(
        "--skip-csv",
        action="store_true",
        help="Skip the CSV download step and reuse an existing file.",
    )
    parser.add_argument(
        "--skip-forms",
        action="store_true",
        help="Skip DA Form downloads and only perform enrichment (if enabled).",
    )
    parser.add_argument(
        "--skip-enrich",
        action="store_true",
        help="Skip the enrichment stage and keep the raw CSV only.",
    )
    parser.add_argument(
        "--csv-path",
        type=Path,
        help="Path to an existing CSV file to reuse (required when skipping CSV download).",
    )
    parser.add_argument(
        "--max-apps",
        type=int,
        default=None,
        help="Limit the number of applications processed when downloading forms.",
    )
    parser.add_argument(
        "--retry-limit",
        type=int,
        default=3,
        help="Number of download retries for each DA Form (default: 3).",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def latest_csv_file() -> Optional[Path]:
    csv_files = sorted(OUT_DIR.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return csv_files[0] if csv_files else None


def run_pipeline(args: argparse.Namespace) -> int:
    ensure_dir(OUT_DIR)
    ensure_dir(SS_DIR)
    ensure_dir(DBG_DIR)

    csv_path: Optional[Path] = args.csv_path

    if args.skip_csv and not csv_path:
        csv_path = latest_csv_file()
        if not csv_path:
            print("[ERROR] --skip-csv was provided but no existing CSV was found in output/.")
            return 2

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=args.headless, args=["--no-sandbox", "--disable-gpu"])
        context = browser.new_context(
            accept_downloads=True,
            viewport={"width": 1600, "height": 1000},
            timezone_id="Australia/Brisbane",
            locale="en-AU",
        )
        page = context.new_page()

        try:
            if not args.skip_csv:
                csv_path = download_csv(page, args.days, OUT_DIR)
            if not args.skip_forms and csv_path:
                apps = get_applications(csv_path)
                if args.max_apps is not None:
                    apps = apps[: args.max_apps]
                if not apps:
                    print("[WARN] No applications found in the CSV; skipping DA Form downloads.")
                for app in apps:
                    download_da_forms(
                        page=page,
                        app_no=app["app_no"],
                        address=app.get("address", ""),
                        retry_limit=max(1, args.retry_limit),
                    )
        finally:
            browser.close()

    if not args.skip_enrich and csv_path:
        enrich_and_merge(csv_path)

    return 0


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    try:
        return run_pipeline(args)
    except Exception as exc:  # pragma: no cover - ensure CI surfaces failures
        print(f"[ERROR] Pipeline failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())

