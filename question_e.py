import sqlite3
import json
from collections import Counter, defaultdict
import re
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd 
from urllib.parse import quote 

# --- Configuration ---
DB_PATH = 'crawl-data-177.sqlite'

# Minimum length for a cookie value to be considered for syncing
MIN_COOKIE_VALUE_LEN = 6
# Set to True to also check for URL-encoded cookie values in URLs
CHECK_URL_ENCODED_VALUES = True

# --- Helper Functions ---

def parse_cookie_string(cookie_str):
    """
    Parses the value from a Set-Cookie header string.
    Returns the cookie value or None if parsing fails.
    """
    if not cookie_str or '=' not in cookie_str:
        return None
    # Get the part before the first semicolon (if any)
    name_value_part = cookie_str.split(';', 1)[0]
    # Split by the first '='
    parts = name_value_part.split('=', 1)
    if len(parts) == 2:
        # Return the value part, stripping whitespace
        return parts[1].strip()
    return None

def extract_set_cookie_values(headers_json):
    """
    Extracts all cookie values from Set-Cookie headers in the headers JSON.
    Handles multiple Set-Cookie headers.
    """
    values = set()
    try:
        headers = json.loads(headers_json)
        
        if isinstance(headers, list): 
            for header_pair in headers:
                 if len(header_pair) == 2 and header_pair[0].lower() == 'set-cookie':
                     # Value might itself contain multiple cookies separated by newline in older formats
                     # Or just be a single cookie string
                     for single_cookie_str in header_pair[1].split('\n'):
                         value = parse_cookie_string(single_cookie_str)
                         if value and len(value) >= MIN_COOKIE_VALUE_LEN:
                             values.add(value)
                             
        elif isinstance(headers, dict): 
             # dict format loses multiple headers with the same name unless value concatenates them.
      
             for key, header_content in headers.items():
                 if key.lower() == 'set-cookie':
                    # The value associated with 'set-cookie' might be a single string
                    # or multiple strings joined by newline
                    for single_cookie_str in header_content.split('\n'):
                        value = parse_cookie_string(single_cookie_str)
                        if value and len(value) >= MIN_COOKIE_VALUE_LEN:
                            values.add(value)

    except (json.JSONDecodeError, TypeError, AttributeError) as e:

        pass
    return values

# --- Main Analysis Logic ---

print(f"Connecting to database: {DB_PATH}")
conn = sqlite3.connect(DB_PATH)
# Use dictionary cursor for easier row access by column name
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# 1. Get successful visit IDs
print("Finding successful visit IDs...")
cursor.execute("""
    SELECT DISTINCT visit_id
    FROM crawl_history
    WHERE command = 'GetCommand' AND command_status = 'ok'
""")
# Store in a set for efficient lookup
successful_visit_ids = {row['visit_id'] for row in cursor.fetchall()}
print(f"Found {len(successful_visit_ids)} successful visits.")

if not successful_visit_ids:
    print("No successful visits found. Exiting.")
    conn.close()
    exit()

# 2. Extract Set-Cookie values for successful visits
print(f"Extracting cookie values (min length {MIN_COOKIE_VALUE_LEN}) for successful visits...")
cookies_by_visit = defaultdict(set)
processed_responses = 0
# Fetch responses in batches to manage memory
cursor.execute("SELECT visit_id, headers FROM http_responses")
while True:

    rows = cursor.fetchmany(10000) 
    if not rows:
        break
    for row in rows:
        visit_id = row['visit_id']
        if visit_id in successful_visit_ids:
            headers_json = row['headers']
            if headers_json:
                cookie_values = extract_set_cookie_values(headers_json)
                if cookie_values:
                    cookies_by_visit[visit_id].update(cookie_values)
            processed_responses += 1
            if processed_responses % 50000 == 0:
                print(f"  Processed {processed_responses} responses...")

print(f"Finished extracting cookies. Found cookies for {len(cookies_by_visit)} visits.")

# 3. Scan HTTP requests for cookie values in URLs
print("Scanning HTTP requests for cookie values in URLs...")
sync_counts = Counter()
processed_requests = 0


cursor.execute("SELECT visit_id, url FROM http_requests")
while True:
    rows = cursor.fetchmany(10000)
    if not rows:
        break
    for row in rows:
        visit_id = row['visit_id']
        # Only process requests from successful visits that had cookies set
        if visit_id in cookies_by_visit:
            request_url = row['url']
            if not request_url: # Skip if URL is null/empty
                continue

            # Get the set of potential cookie values for this visit
            possible_values = cookies_by_visit[visit_id]

            for value in possible_values:
                # Check if the raw value is in the URL
                match_found = False
                if value in request_url:
                    sync_counts[visit_id] += 1
                    match_found = True 

                # check for the URL-encoded version ONLY if raw didn't match
                # This avoids double counting if raw and encoded are the same or both present
                if not match_found and CHECK_URL_ENCODED_VALUES:
                     try:
                        encoded_value = quote(value)
                        # Avoid checking if encoding didn't change it OR if encoded is same as raw
                        if encoded_value != value and encoded_value in request_url:
                             sync_counts[visit_id] += 1
                     except Exception:
                         # Ignore potential errors during encoding non-standard values
                         pass

            processed_requests += 1
            if processed_requests % 100000 == 0:
                print(f"  Processed {processed_requests} requests...")


print(f"Finished scanning requests. Found syncs for {len(sync_counts)} visits.")

# Add visits with 0 syncs to the counter for the distribution

all_visit_sync_counts = {visit_id: sync_counts.get(visit_id, 0) for visit_id in successful_visit_ids}


# 4. Analyze results: Find max syncs and corresponding site
max_syncs = 0
visit_id_with_max_syncs = -1

if all_visit_sync_counts: # Check if dictionary is not empty
    # Use max on the items, comparing by value (the count)
    visit_id_with_max_syncs, max_syncs = max(all_visit_sync_counts.items(), key=lambda item: item[1])

    # Get the site URL for the visit with max syncs
    cursor.execute("SELECT site_url FROM site_visits WHERE visit_id = ?", (visit_id_with_max_syncs,))
    result = cursor.fetchone()
    site_url_with_max_syncs = result['site_url'] if result else "Unknown (visit_id not found in site_visits)"

    print("\n--- Analysis Results ---")
    print(f"Maximum number of cookie syncs observed for a single visit: {max_syncs}")
    print(f"Visit ID with max syncs: {visit_id_with_max_syncs}")
    print(f"Site URL for max syncs: {site_url_with_max_syncs}")
else:
     print("\n--- Analysis Results ---")
     print("No syncs found or no successful visits to analyze.")
     site_url_with_max_syncs = "N/A" 

# 5. Prepare data for plotting the distribution
sync_counts_list = list(all_visit_sync_counts.values())

# --- Close DB Connection ---
conn.close()
print("\nDatabase connection closed.")

# 6. Plot the distribution
if sync_counts_list:
    print("Generating distribution plot...")
    plt.figure(figsize=(12, 7))

    max_observed = max(sync_counts_list) if sync_counts_list else 0
    # Create bins up to max_observed+1, maybe step if max is very large
    bin_edge_step = max(1, int(max_observed / 50)) 
    bins = np.arange(0, max_observed + bin_edge_step + 1, bin_edge_step)

    plt.hist(sync_counts_list, bins=bins, edgecolor='black', alpha=0.7)

    plt.xlabel("Number of Cookie Syncs Observed per Visit")
    plt.ylabel("Number of Visits")
    plt.title("Distribution of Cookie Syncs per Successful Site Visit")
    plt.grid(axis='y', linestyle='--', alpha=0.6)

    plt.tight_layout()
    plt.savefig("cookie_sync_distribution.png")
    print("Plot saved as cookie_sync_distribution.png")

else:
    print("No sync data to plot.")

print("\nScript finished.")