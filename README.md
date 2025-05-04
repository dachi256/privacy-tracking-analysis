# Web Privacy Tracking Analysis

## Overview
This project analyzes web tracking behaviors in a dataset of 177 websites crawled using OpenWPM. The analysis examines various tracking techniques including third-party requests, cookie usage, cookie syncing, and JavaScript-based fingerprinting.

## Dataset
The analysis uses the `crawl-data-177.sqlite` database, which contains browsing data from 177 website visits. *Note: The database file exceeds GitHub's size limits and is not included in this repository.*

## Project Structure
- `question_a.py` - Crawl status analysis (success/failure rates)
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
- Required packages:
  - sqlite3
  - pandas
  - matplotlib
  - seaborn
  - tldextract
  - urllib.parse
  - logging
  - json
  - collections

## Usage
To run all analyses:
```
python run_all_analyses.py
```

To run individual analysis:
```
python question_a.py  # For crawl status analysis
python question_b.py  # For third-party analysis
# etc.
```

## Key Findings
- Out of 177 attempted site visits, 15 failed to load and 25 were incomplete
- The site with the highest number of third parties was imgur.com (133)
- The most common third-party domain was google.com (present on 89 sites)
- The script setting the most cookies was a tag manager with 214 operations
- The most common cookie name set via HTTP headers was __cf_bm (267 instances)
- The site with the most cookie syncing was cnn.com (132 instances)
- Canvas fingerprinting (toDataURL) was used on 34 sites

## Note on Database File
The SQLite database (`crawl-data-177.sqlite`) used for this analysis exceeds GitHub's file size limit (>1GB) and is not included in this repository. You will need to obtain this file separately to run the analysis scripts.