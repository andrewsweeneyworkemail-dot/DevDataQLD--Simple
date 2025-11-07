#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Development.i — CSV downloader via real browser (Playwright)
Runs on GitHub Actions (headless Chromium, no sandbox) and downloads the CSV.

Usage (locally):
    python scripts/dev_i_csv_last30.py --days 30 --out output/dev_i_last30.csv --headless
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import sys
from pathlib import Path
from typing import Callable, Iterable, Optional, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# The page with the search + results; you used this already:
BASE_URL = "https://developmenti.brisbane.qld.gov.au/Home/ApplicationSearch"

# folders we’ll actually have in GitHub
OUT_DIR = Path("output")
SS_DIR = Path("logs/screenshots")
DBG_DIR = Path("logs/debug")
for d in (OUT_DIR, SS_DIR, DBG_DIR):
    d.mkdir(parents=True, exist_ok=True)


def date_range_ddmmyyyy(days: int) -> tuple[str, str]:
    end = dt.date.today()
    start = end - dt.timedelta(days=days)
    return start.strftime("%d/%m/%Y"), end.strftime("%d/%m/%Y")


def ss(page, name: str) -> None:
    try:
        page.screenshot(path=str(SS_DIR / f"{name}.png"), full_page=True)
    except Exception:
        pass


def try_click_many(page, candidates: Iterable[Tuple[str, str]], timeout: int = 4000) -> bool:
    for kind, label in candidates:
        try:
            if kind == "role_button":
                page.get_by_role("button", name=re.compile(label, re.I)).first.click(timeout=timeout)
                return True
            if kind == "role_link":
                page.get_by_role("link", name=re.compile(label, re.I)).first.click(timeout=timeout)
                return True
            if kind == "text":
                page.get_by_text(re.compile(label, re.I)).first.click(timeout=timeout)
                return True
            if kind == "css":
                page.locator(label).first.click(timeout=timeout)
                return True
        except Exception:
            continue
    return False


def maybe_dismiss_banners(page) -> None:
    try_click_many(
        page,
        [
            ("role_button", r"Accept|I Agree|Got it|Close|Dismiss"),
            ("text", r"Accept|I Agree|Got it|Close|Dismiss"),
        ],
        timeout=2000,
    )


def open_date_range(page) -> None:
    try_click_many(
        page,
        [
            ("role_button", r"Date Range"),
            ("text", r"Date Range"),
        ],
        timeout=8000,
    )
    page.wait_for_timeout(300)
    ss(page, "02_date_range_open")


def set_date_range(page, start: str, end: str) -> bool:
    """Attempt to populate the date range inputs with multiple selector strategies."""

    def _by_css(start_sel: str, end_sel: str) -> Callable[[], Tuple]:
        return lambda: (
            page.locator(start_sel).first,
            page.locator(end_sel).first,
        )

    def _by_css_indices(selector: str, start_idx: int, end_idx: int) -> Callable[[], Tuple]:
        return lambda: (
            page.locator(selector).nth(start_idx),
            page.locator(selector).nth(end_idx),
        )

    def _by_label(start_label: str, end_label: str) -> Callable[[], Tuple]:
        return lambda: (
            page.get_by_label(re.compile(start_label, re.I)).first,
            page.get_by_label(re.compile(end_label, re.I)).first,
        )

    candidate_locators: Iterable[Callable[[], Tuple]] = (
        _by_label(r"from|start", r"to|end"),
        _by_css("input[placeholder*='Start']", "input[placeholder*='End']"),
        _by_css("input[placeholder*='From']", "input[placeholder*='To']"),
        _by_css("input[data-placeholder*='From']", "input[data-placeholder*='To']"),
        _by_css("input[aria-label*='from']", "input[aria-label*='to']"),
        _by_css_indices("input[type='text']", 0, 1),
    )

    for resolver in candidate_locators:
        try:
            start_inp, end_inp = resolver()
            start_inp.wait_for(state="visible", timeout=5000)
            end_inp.wait_for(state="visible", timeout=5000)

            for inp, value in ((start_inp, start), (end_inp, end)):
                inp.click()
                inp.press("Control+A")
                inp.fill(value)

            page.keyboard.press("Enter")
            page.wait_for_timeout(800)

            start_val = start_inp.input_value().strip()
            end_val = end_inp.input_value().strip()
            if start_val == start and end_val == end:
                ss(page, "03_dates_set")
                return True
        except Exception:
            continue

    return False


def show_results(page) -> None:
    try_click_many(page, [("text", r"Show Results"), ("role_button", r"Show Results")], timeout=8000)
    page.wait_for_timeout(800)
    try_click_many(page, [("text", r"List"), ("role_button", r"List")], timeout=5000)
    page.wait_for_timeout(800)
    ss(page, "04_results_view")


def wait_for_results(page, timeout_ms: int = 20000) -> bool:
    try:
        page.wait_for_selector("table, .mat-table, .results, .list", timeout=timeout_ms, state="visible")
        return True
    except PWTimeout:
        return False


def click_download_csv(page, save_path: Path) -> bool:
    patterns = [
        ("role_button", r"CSV"),
        ("role_button", r"Download CSV"),
        ("text", r"CSV"),
        ("text", r"Download CSV"),
        ("css", "button:has-text('CSV'), a:has-text('CSV')"),
    ]
    for kind, label in patterns:
        try:
            page.wait_for_timeout(500)
            with page.expect_download(timeout=20000) as dl_wait:
                if kind == "role_button":
                    page.get_by_role("button", name=re.compile(label, re.I)).first.click(timeout=6000)
                elif kind == "text":
                    page.get_by_text(re.compile(label, re.I)).first.click(timeout=6000)
                else:
                    page.locator(label).first.click(timeout=6000)
            dl = dl_wait.value
            dl.save_as(str(save_path))
            return save_path.exists() and save_path.stat().st_size > 0
        except Exception:
            continue
    return False


def run(days: int, status: Optional[str], out_csv: Path, headless: bool) -> int:
    dates_set = False

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, args=["--no-sandbox", "--disable-gpu"])
        ctx = browser.new_context(
            accept_downloads=True,
            viewport={"width": 1440, "height": 900},
            timezone_id="Australia/Brisbane",
            locale="en-AU",
        )
        page = ctx.new_page()

        page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)
        ss(page, "01_loaded")

        maybe_dismiss_banners(page)
        open_date_range(page)
        start, end = date_range_ddmmyyyy(days)
        dates_set = set_date_range(page, start, end)
        show_results(page)
        ok_results = wait_for_results(page)

        ok_csv = click_download_csv(page, out_csv)
        ss(page, "05_after_download")

        browser.close()

    if ok_csv:
        print(f"[OK] CSV saved -> {out_csv} ({out_csv.stat().st_size} bytes)")
        return 0

    print(
        "[ERROR] CSV not downloaded or empty.\n"
        f"  - Date range applied: {dates_set}\n"
        f"  - Results visible: {ok_results}\n"
        f"See {SS_DIR} for screenshots.",
        file=sys.stderr,
    )
    return 2


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Development.i CSV downloader (Playwright).")
    ap.add_argument("--days", type=int, default=30, help="Days back (default: 30).")
    ap.add_argument("--status", type=str, default=None, help="Optional status filter (not always needed).")
    ap.add_argument("--out", type=Path, default=OUT_DIR / "dev_i_last30.csv", help="Output CSV file.")
    ap.add_argument("--headless", action="store_true", help="Run headless (CI).")
    ap.add_argument("--headed", dest="headless", action="store_false", help="Run with a visible browser.")
    ap.set_defaults(headless=True)
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    code = run(
        days=args.days,
        status=args.status,
        out_csv=args.out,
        headless=args.headless,
    )
    sys.exit(code)


if __name__ == "__main__":
    main()
