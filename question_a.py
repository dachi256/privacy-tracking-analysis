import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter

# Connect to the database
conn = sqlite3.connect('crawl-data-177.sqlite')

# First, let's check how many sites were supposed to be crawled
sites_query = "SELECT COUNT(*) FROM site_visits"
total_sites = pd.read_sql_query(sites_query, conn).iloc[0, 0]
print(f"Total sites in site_visits table: {total_sites}")

# Let's look at failed crawls in crawl_history
failed_crawls_query = """
SELECT ch.visit_id, ch.command, ch.command_status, ch.error, sv.site_url
FROM crawl_history ch
LEFT JOIN site_visits sv ON ch.visit_id = sv.visit_id
WHERE ch.command = 'GetCommand' AND ch.command_status != 'ok'
"""
failed_crawls = pd.read_sql_query(failed_crawls_query, conn)
print(f"Failed crawls: {len(failed_crawls)}")

# Let's look at incomplete visits
incomplete_query = "SELECT COUNT(*) FROM incomplete_visits"
incomplete_visits = pd.read_sql_query(incomplete_query, conn).iloc[0, 0]
print(f"Incomplete visits: {incomplete_visits}")

# Analyze reasons for failure
if len(failed_crawls) > 0:
    reasons = Counter(failed_crawls['error'])
    print("\nReasons for crawl failures:")
    for reason, count in reasons.most_common():
        print(f"- {reason}: {count}")