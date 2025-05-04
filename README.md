# Web Privacy Tracking Analysis

This project analyzes web tracking behaviors in a dataset of 177 websites crawled using OpenWPM. The analysis examines various tracking techniques including third-party requests, cookie usage, cookie syncing, and JavaScript-based fingerprinting.

## Project Structure

- `question_a.py` - Crawl status analysis
- `question_b.py` - Third-party request analysis
- `question_c.py` - JavaScript cookie analysis
- `question_d.py` - HTTP cookie analysis
- `question_e.py` - Cookie syncing analysis
- `question_f.py` - Fingerprinting API analysis
- `run_all_analyses.py` - Script to run all analyses in sequence
- `third_party_distribution.png` - Visualization of third-party distribution
- `cookie_sync_distribution.png` - Visualization of cookie syncing distribution

## Requirements

- Python 3.x
- Required packages: sqlite3, pandas, matplotlib, seaborn, tldextract

## Usage

To run all analyses: