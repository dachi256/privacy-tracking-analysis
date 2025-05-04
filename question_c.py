import sqlite3
import pandas as pd
import tldextract
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
conn = sqlite3.connect('crawl-data-177.sqlite')

# Helper function to extract eTLD+1 domain
def get_etld1(url):
    """Extracts the effective top-level domain plus one from a URL."""
    if not isinstance(url, str) or not url.strip():
        return None
    try:
        extracted = tldextract.extract(url)
        if extracted.domain and extracted.suffix:
            return f"{extracted.domain}.{extracted.suffix}".lower()
        else:
            return None
    except:
        return None

# Get JavaScript operations that set document.cookie
cookie_set_query = """
SELECT j.script_url, j.document_url, j.top_level_url, sv.site_url
FROM javascript j
JOIN site_visits sv ON j.visit_id = sv.visit_id
WHERE j.symbol = 'window.document.cookie' AND j.operation = 'set'
"""
cookie_set_df = pd.read_sql_query(cookie_set_query, conn)
logging.info(f"Found {len(cookie_set_df)} cookie-setting operations")

# Count cookie-setting operations by script
script_counts = cookie_set_df.groupby('script_url').size().sort_values(ascending=False)

# Get the top script
if not script_counts.empty:
    top_script = script_counts.index[0]
    top_script_count = script_counts.iloc[0]
    
    # Extract domains for first-party analysis
    cookie_set_df['script_domain'] = cookie_set_df['script_url'].apply(get_etld1)
    cookie_set_df['site_domain'] = cookie_set_df['site_url'].apply(get_etld1)
    
    # Determine if cookie-setting is in first-party context
    cookie_set_df['is_first_party'] = cookie_set_df['script_domain'] == cookie_set_df['site_domain']
    
    # Count first-party cookies for the top script
    top_script_ops = cookie_set_df[cookie_set_df['script_url'] == top_script]
    first_party_count = top_script_ops['is_first_party'].sum()
    
    print(f"\nScript setting most cookies: {top_script}")
    print(f"Total cookie operations: {top_script_count}")
    print(f"First-party cookie operations: {first_party_count}")
else:
    print("No scripts found setting cookies")

# Close the connection
conn.close()