import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
from urllib.parse import urlparse
import tldextract 
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
DB_FILE = 'crawl-data-177.sqlite'

# --- Helper Function ---
def get_etld1(url):
    """Extracts the eTLD+1 (effective top-level domain plus one) from a URL."""
    if not isinstance(url, str) or not url.strip():
        return None
    try:
        # urlparse helps handle protocols and ensures tldextract gets the domain part
        parsed_url = urlparse(url)
        extracted = tldextract.extract(parsed_url.netloc)
        # Combine domain and suffix for eTLD+1
        if extracted.domain and extracted.suffix:
            return f"{extracted.domain}.{extracted.suffix}".lower()
        else:
            # Handle cases like bare IP addresses or localhosts if necessary

            return parsed_url.netloc.lower() if parsed_url.netloc else None
    except Exception as e:
        return None

# --- Main Analysis ---
conn = None 
try:
    logging.info(f"Connecting to database: {DB_FILE}")
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. Identify successfully crawled visit_ids
    logging.info("Identifying successful crawls...")
    successful_visits_query = """
    SELECT DISTINCT visit_id
    FROM crawl_history
    WHERE command = 'GetCommand' AND command_status = 'ok';
    """
    successful_visit_ids = pd.read_sql_query(successful_visits_query, conn)['visit_id'].tolist()
    logging.info(f"Found {len(successful_visit_ids)} successful visits.")

    if not successful_visit_ids:
        logging.error("No successful visits found. Cannot proceed.")
        exit()

    # 2. Fetch relevant HTTP requests for successful crawls
    logging.info("Fetching HTTP requests for successful visits...")
    # Using placeholders for the list of IDs for security and efficiency
    placeholders = ','.join('?' for _ in successful_visit_ids)
    requests_query = f"""
    SELECT
        r.visit_id,
        r.url,
        r.top_level_url,
        sv.site_url  -- Get the canonical site URL from site_visits
    FROM http_requests r
    JOIN site_visits sv ON r.visit_id = sv.visit_id
    WHERE r.visit_id IN ({placeholders});
    """
    # Execute query with the list of IDs as parameters
    requests_df = pd.read_sql_query(requests_query, conn, params=successful_visit_ids)
    logging.info(f"Fetched {len(requests_df)} HTTP requests.")

    # 3. Determine first-party vs. third-party requests
    logging.info("Analyzing requests for third parties...")
    # Apply domain extraction function
    # Use site_url from site_visits as the definitive first-party context
    requests_df['request_etld1'] = requests_df['url'].apply(get_etld1)
    requests_df['top_level_etld1'] = requests_df['site_url'].apply(get_etld1) # Compare against the visited site's domain

    # Filter out rows where domain extraction failed or is identical
    requests_df.dropna(subset=['request_etld1', 'top_level_etld1'], inplace=True)

    # Identify third-party requests
    third_party_requests = requests_df[
        (requests_df['request_etld1'] != requests_df['top_level_etld1']) &
        (requests_df['request_etld1'] != '') & # Ensure request domain is not empty
        (requests_df['top_level_etld1'] != '') # Ensure top-level domain is not empty
    ].copy() 

    logging.info(f"Identified {len(third_party_requests)} third-party requests.")

    # 4. Calculate the number of unique third-party domains per site
    logging.info("Calculating unique third parties per site...")
    # Group by visit_id and count unique third-party domains
    third_parties_per_site = third_party_requests.groupby('visit_id')['request_etld1'].nunique().reset_index()
    third_parties_per_site.rename(columns={'request_etld1': 'third_party_count'}, inplace=True)

    # Add site_url back for context
    site_visits_df = pd.read_sql_query("SELECT visit_id, site_url FROM site_visits", conn)
    third_parties_per_site = pd.merge(third_parties_per_site, site_visits_df, on='visit_id', how='left')

    # Handle sites with ZERO third parties (they wouldn't be in third_party_requests)
    # Create a dataframe with all successful visits and merge
    all_successful_sites = pd.DataFrame({'visit_id': successful_visit_ids})
    all_successful_sites = pd.merge(all_successful_sites, site_visits_df, on='visit_id', how='left')
    third_parties_per_site_full = pd.merge(all_successful_sites, third_parties_per_site[['visit_id', 'third_party_count']], on='visit_id', how='left')
    third_parties_per_site_full['third_party_count'].fillna(0, inplace=True) 
    third_parties_per_site_full['third_party_count'] = third_parties_per_site_full['third_party_count'].astype(int)


    logging.info("Analysis complete. Preparing results.")

    # --- Results ---

    # 5. Plot distribution of third parties per site
    plt.figure(figsize=(12, 6))
    sns.histplot(third_parties_per_site_full['third_party_count'], bins=30, kde=False)
    plt.title('Distribution of Third Parties per Site')
    plt.xlabel('Number of Unique Third Parties')
    plt.ylabel('Number of Sites')
    plt.grid(axis='y', alpha=0.5)
    # Save the plot
    plot_filename = 'third_party_distribution.png'
    plt.savefig(plot_filename)
    logging.info(f"Distribution plot saved as {plot_filename}")

    # 6. Which site had the highest number of third parties?
    max_third_parties_site = third_parties_per_site_full.loc[third_parties_per_site_full['third_party_count'].idxmax()]
    print("\n--- Analysis Results ---")
    print(f"\nSite with the highest number of third parties:")
    print(f"  Site URL: {max_third_parties_site['site_url']}")
    print(f"  Number of Third Parties: {max_third_parties_site['third_party_count']}")

    # 7. Which third party was present on the largest number of sites?
    # We need to count how many *unique sites* each third party appeared on.
    # Get unique pairs of (visit_id, third_party_domain)
    unique_site_third_party = third_party_requests[['visit_id', 'request_etld1']].drop_duplicates()

    # Count occurrences of each third party domain across different sites
    third_party_site_counts = Counter(unique_site_third_party['request_etld1'])

    # Find the most common third party
    if third_party_site_counts:
        most_common_third_party, count = third_party_site_counts.most_common(1)[0]
        print(f"\nMost common third party (present on the largest number of sites):")
        print(f"  Third Party Domain: {most_common_third_party}")
        print(f"  Number of Sites Present On: {count}")
    else:
        print("\nNo third parties found across any sites.")


    print("\nNote: Analysis based on successfully crawled sites only.")
    print(f"Distribution plot saved as {plot_filename}")

except sqlite3.Error as e:
    logging.error(f"Database error: {e}")
except FileNotFoundError:
    logging.error(f"Database file not found: {DB_FILE}")
except Exception as e:
    logging.error(f"An unexpected error occurred: {e}")
finally:
    if conn:
        conn.close()
        logging.info("Database connection closed.")