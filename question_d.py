import sqlite3
import pandas as pd
import json
from collections import Counter
import logging
import re 


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
DB_FILE = 'crawl-data-177.sqlite'

# --- Helper Function ---
def extract_cookie_name(set_cookie_value):
    """Extracts the cookie name from a Set-Cookie header value."""
    if not isinstance(set_cookie_value, str):
        return None
    # Cookie name is the part before the first '='
    match = re.match(r'^([^=]+)=', set_cookie_value.strip())
    if match:
        return match.group(1).strip() 
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

    # 2. Query http_responses for successful visits
    logging.info("Querying HTTP responses for successful visits...")
    placeholders = ','.join('?' for _ in successful_visit_ids)
    # Fetch only necessary columns: visit_id and headers
    responses_query = f"""
    SELECT headers
    FROM http_responses
    WHERE visit_id IN ({placeholders});
    """
    responses_df = pd.read_sql_query(responses_query, conn, params=successful_visit_ids)
    logging.info(f"Fetched {len(responses_df)} HTTP responses.")

    # 3. Parse headers and count cookie names
    logging.info("Parsing Set-Cookie headers and counting cookie names...")
    cookie_name_counts = Counter()
    processed_responses = 0
    json_errors = 0
    no_header_count = 0
    set_cookie_headers_found = 0

    for headers_json in responses_df['headers']:
        processed_responses += 1
        if headers_json is None:
            no_header_count += 1
            continue
        try:
            # Headers are typically stored as a JSON list of [name, value] lists
            headers_list = json.loads(headers_json)
            if not isinstance(headers_list, list):
                 continue

            # Iterate through [name, value] pairs in the header list
            for header_pair in headers_list:
                # Check if it's a list/tuple with 2 elements: [name, value]
                if isinstance(header_pair, (list, tuple)) and len(header_pair) == 2:
                    header_name, header_value = header_pair
                    # Case-insensitive check for 'Set-Cookie'
                    if isinstance(header_name, str) and header_name.lower() == 'set-cookie':
                        set_cookie_headers_found += 1
                        cookie_name = extract_cookie_name(header_value)
                        if cookie_name:
                            cookie_name_counts[cookie_name] += 1
        except json.JSONDecodeError:
            json_errors += 1
     
        except Exception as e:
             json_errors +=1 



    logging.info(f"Processed {processed_responses} responses.")
    logging.info(f"Found {set_cookie_headers_found} Set-Cookie headers.")
    if no_header_count > 0:
        logging.warning(f"{no_header_count} responses had NULL headers.")
    if json_errors > 0:
        logging.warning(f"Encountered {json_errors} errors decoding/processing JSON headers.")

    # 4. Identify the most common cookie name
    print("\n--- Analysis Results ---")
    if cookie_name_counts:
        most_common_cookie, count = cookie_name_counts.most_common(1)[0]
        print(f"\nMost common cookie name set via HTTP Set-Cookie header:")
        print(f"  Cookie Name: {most_common_cookie}")
        print(f"  Times Set: {count}")

    else:
        print("\nNo Set-Cookie headers were successfully parsed or found in the responses for successful visits.")


    print(f"\nNote: Analysis based on parsing Set-Cookie headers from HTTP responses during successful crawls.")

except sqlite3.Error as e:
    logging.error(f"Database error: {e}")
except FileNotFoundError:
    logging.error(f"Database file not found: {DB_FILE}")
except Exception as e:
    logging.error(f"An unexpected error occurred: {e}", exc_info=True)
finally:
    if conn:
        conn.close()
        logging.info("Database connection closed.")