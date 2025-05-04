import sqlite3
from collections import Counter, defaultdict
import tldextract 
from urllib.parse import urlparse

# --- Configuration ---
DB_PATH = 'crawl-data-177.sqlite'
TARGET_API = 'HTMLCanvasElement.toDataURL'

# List of potential fingerprinting API symbols identified from exploration

POTENTIAL_FP_APIS = {
    'window.navigator.userAgent', 'window.navigator.vendor', 'window.navigator.platform',
    'window.navigator.plugins', 'CanvasRenderingContext2D.font', 'window.navigator.language',
    'window.navigator.languages', 'window.navigator.cookieEnabled', 'HTMLCanvasElement.height',
    'window.screen.colorDepth', 'HTMLCanvasElement.width', 'window.navigator.maxTouchPoints',
    'window.navigator.product', 'HTMLCanvasElement.clientHeight', 'HTMLCanvasElement.clientWidth',
    'CanvasRenderingContext2D.measureText', 'window.navigator.doNotTrack',
    'window.navigator.sendBeacon', 'window.navigator.mimeTypes', 'window.navigator.appVersion',
    'window.navigator.hardwareConcurrency', 'window.navigator.onLine', 'window.navigator.webdriver',
    'window.navigator.permissions', 'window.navigator.globalPrivacyControl',
    'HTMLCanvasElement.getContext', 'window.navigator.appName', 'CanvasRenderingContext2D.fillStyle',
    'window.navigator.serviceWorker', 'window.screen.pixelDepth', 'window.navigator.appCodeName',
    'CanvasRenderingContext2D.getImageData', 'window.navigator.mediaCapabilities',
    'HTMLCanvasElement.toDataURL', 'window.navigator.mediaSession', 'CanvasRenderingContext2D.arc',
    'CanvasRenderingContext2D.fillText', 'window.navigator.javaEnabled',
    'window.navigator.productSub', 'HTMLCanvasElement.style', 'CanvasRenderingContext2D.fillRect',
    'window.navigator.userActivation', 'window.navigator.requestMediaKeySystemAccess',
    'CanvasRenderingContext2D.stroke', 'CanvasRenderingContext2D.fill', 'HTMLCanvasElement.tagName',
    'window.navigator.__proto__', 'CanvasRenderingContext2D.lineWidth', 'window.navigator.oscpu',
    'window.navigator.buildID', 'CanvasRenderingContext2D.strokeStyle',
    'CanvasRenderingContext2D.textBaseline', 'window.navigator.vendorSub',
    'window.navigator.credentials', 'window.navigator.mediaDevices', 'HTMLCanvasElement.nodeName',
    'OfflineAudioContext.createOscillator', 'HTMLCanvasElement.nodeType',
    'OfflineAudioContext.currentTime', 'CanvasRenderingContext2D.rect', 'window.navigator.storage',
    'window.navigator.geolocation', 'HTMLCanvasElement.children',
    'CanvasRenderingContext2D.createLinearGradient', 'window.navigator.pdfViewerEnabled',
    'CanvasRenderingContext2D.shadowBlur', 'window.navigator.hasOwnProperty',
    'CanvasRenderingContext2D.globalCompositeOperation', 'CanvasRenderingContext2D.shadowColor',
    'HTMLCanvasElement.setAttribute', 'OfflineAudioContext.destination',
    'OfflineAudioContext.startRendering', 'OfflineAudioContext.createDynamicsCompressor',
    'OfflineAudioContext.hasOwnProperty', 'CanvasRenderingContext2D.createRadialGradient',
    'CanvasRenderingContext2D.isPointInPath', 'OfflineAudioContext.oncomplete',
    'HTMLCanvasElement.transferControlToOffscreen', 'HTMLCanvasElement.classList',
    'CanvasRenderingContext2D.putImageData', 'HTMLCanvasElement.childNodes',
    'CanvasRenderingContext2D.strokeText', 'HTMLCanvasElement.getBoundingClientRect',
    'HTMLCanvasElement.addEventListener', 'HTMLCanvasElement.childElementCount',
    'HTMLCanvasElement.textContent', 'CanvasRenderingContext2D.bezierCurveTo',
    'HTMLCanvasElement.toBlob', 'OfflineAudioContext.state', 'window.navigator.locks',
    'CanvasRenderingContext2D.restore', 'CanvasRenderingContext2D.save',
    'CanvasRenderingContext2D.scale', 'HTMLCanvasElement.captureStream',
    'HTMLCanvasElement.ownerDocument', 'HTMLCanvasElement.parentNode',
    'window.navigator.getGamepads', 'AnalyserNode.frequencyBinCount',
    'OfflineAudioContext.createAnalyser', 'AnalyserNode.context', 'AnalyserNode.fftSize',
    'CanvasRenderingContext2D.arcTo', 'CanvasRenderingContext2D.rotate',
    'HTMLCanvasElement.attributes', 'HTMLCanvasElement.matches', 'HTMLCanvasElement.namespaceURI',
    'HTMLCanvasElement.querySelectorAll', 'window.navigator.toString',
    'AnalyserNode.getFloatFrequencyData', 'window.navigator.clipboard',
    'AnalyserNode.channelCount', 'AnalyserNode.channelCountMode',
    'AnalyserNode.channelInterpretation', 'AnalyserNode.connect',
    'AnalyserNode.getFloatTimeDomainData', 'AnalyserNode.maxDecibels',
    'AnalyserNode.minDecibels', 'AnalyserNode.numberOfInputs', 'AnalyserNode.numberOfOutputs',
    'AnalyserNode.smoothingTimeConstant', 'AudioContext.addEventListener',
    'AudioContext.createOscillator', 'AudioContext.destination', 'AudioContext.resume',
    'AudioContext.state', 'CanvasRenderingContext2D.shadowOffsetX',
    'CanvasRenderingContext2D.shadowOffsetY', 'HTMLCanvasElement.className',
    'HTMLCanvasElement.getElementsByTagName', 'HTMLCanvasElement.nextSibling',
    'HTMLCanvasElement.offsetHeight', 'HTMLCanvasElement.offsetWidth',
    'HTMLCanvasElement.previousSibling', 'OfflineAudioContext.addEventListener',
    'OfflineAudioContext.createBiquadFilter', 'OfflineAudioContext.createBuffer',
    'OfflineAudioContext.createBufferSource', 'OfflineAudioContext.listener',
    'OfflineAudioContext.sampleRate', 'window.navigator.__lookupGetter__',
    'window.navigator.getAutoplayPolicy', 'AnalyserNode.disconnect',
    'AudioContext.createAnalyser', 'AudioContext.createGain',
    'AudioContext.createScriptProcessor', 'AudioContext.sampleRate',
    'CanvasRenderingContext2D.createImageData', 'HTMLCanvasElement.id',
    'HTMLCanvasElement.parentElement', 'HTMLCanvasElement.removeEventListener',
    'window.navigator.valueOf', 'window.navigator.wakeLock',

}

OTHER_FP_APIS = POTENTIAL_FP_APIS - {TARGET_API}


tld_cache = {}

# --- Helper Functions ---

def get_successful_visit_ids(cursor):
    """Fetches visit_ids for successful 'GetCommand' crawls."""
    cursor.execute("""
        SELECT DISTINCT visit_id
        FROM crawl_history
        WHERE command = 'GetCommand' AND command_status = 'ok'
    """)
    return {row['visit_id'] for row in cursor.fetchall()}

def get_etld1(url_string):
    """Extracts the eTLD+1 (registered domain) from a URL string, using a cache."""
    if url_string is None:
        return None
    if url_string in tld_cache:
        return tld_cache[url_string]

    try:
        # Handle potential // prefix or schemeless URLs for tldextract
        if url_string.startswith('//'):
            url_string = 'http:' + url_string
        # tldextract handles missing scheme better
        ext = tldextract.extract(url_string)
        # registered_domain combines suffix and domain (e.g., google.com)
        result = ext.registered_domain 
        if not result: # Handle cases like 'localhost' or IPs where it's empty
             result = ext.domain # Fall back to domain part if registered_domain is empty
        tld_cache[url_string] = result
        return result
    except Exception:
        # Handle potential parsing errors with unusual URLs
        tld_cache[url_string] = None
        return None

def is_third_party(script_url, page_url):
    """Determines if a script URL is third-party relative to the page URL."""
    # Treat None/empty script URLs or data URIs as first-party
    if not script_url or script_url.startswith('data:'):
        return False
    
    if not page_url:
         return True 

    script_domain = get_etld1(script_url)
    page_domain = get_etld1(page_url)

    # If either domain could not be parsed, conservatively assume different
    # unless they are identical strings (e.g. both None)
    if not script_domain or not page_domain:
        return script_domain != page_domain 

    return script_domain != page_domain

# --- Main Analysis Logic ---

print(f"Connecting to database: {DB_PATH}")
conn = None 
try:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    print("Fetching successful visit IDs...")
    successful_visit_ids = get_successful_visit_ids(cursor)
    print(f"Found {len(successful_visit_ids)} successful visits.")

    if not successful_visit_ids:
        print("No successful visits found. Exiting.")
        exit()

    # --- Data Structures ---
    sites_using_target = set() 
    # Stores details for each TARGET_API call for script analysis
    target_api_calls = [] 
    # Maps (visit_id, script_url) -> set of potential FP symbols called in that context
    js_calls_per_script_visit = defaultdict(set) 
    # Counts co-occurrences of OTHER_FP_APIS with TARGET_API in the same script/visit context
    cooccurrence_counts = Counter() 
    
    print(f"Processing javascript table for target API '{TARGET_API}' and potential co-occurring APIs...")
    processed_rows = 0
    cursor.execute("SELECT visit_id, script_url, symbol, top_level_url FROM javascript")

    while True:
        rows = cursor.fetchmany(100000) # Process in chunks
        if not rows:
            break

        for row in rows:
            processed_rows += 1
            visit_id = row['visit_id']

            # Process only data from successful visits
            if visit_id in successful_visit_ids:
                symbol = row['symbol']
                script_url = row['script_url']
                top_level_url = row['top_level_url']
                
                # Check if the symbol is our target or another potential FP API
                is_target = (symbol == TARGET_API)
                is_other_fp = (symbol in OTHER_FP_APIS)

                if is_target:
                    if top_level_url: # Only count sites if we have a top_level_url
                         sites_using_target.add(top_level_url)
                    # Store details even if top_level_url is missing for script analysis consistency
                    target_api_calls.append({
                        'visit_id': visit_id, 
                        'script_url': script_url, 
                        'top_level_url': top_level_url
                    })
                
                    if script_url is not None: # Use only contexts with a script_url
                        js_calls_per_script_visit[(visit_id, script_url)].add(symbol)

                elif is_other_fp:
                    # Add other potential FP APIs to the context map
                     if script_url is not None: # Use only contexts with a script_url
                        js_calls_per_script_visit[(visit_id, script_url)].add(symbol)

        if processed_rows % 500000 == 0:
            print(f"  Processed {processed_rows} javascript entries...")

    print(f"Finished processing {processed_rows} javascript entries.")

    # --- Analysis Part 1: Target API Usage ---
    print("\n--- Target API Analysis ---")
    num_sites_using_target = len(sites_using_target)
    print(f"1. Number of distinct sites using '{TARGET_API}': {num_sites_using_target}")
    
    # Analyze scripts calling the target API
    script_counts = Counter()
    script_party_status = defaultdict(lambda: {'first': 0, 'third': 0}) 

    print("\n2. Analyzing scripts calling the target API:")
    if not target_api_calls:
        print("  No calls to the target API were found in successful visits.")
    else:
        print(f"  Total calls to '{TARGET_API}': {len(target_api_calls)}")
        for call in target_api_calls:
            script = call['script_url']
           
            script_key = script if script is not None else "(Inline/Unknown)" 
            script_counts[script_key] += 1
            
            # Determine party status only if top_level_url is available
            if call['top_level_url']:
                is_third = is_third_party(script, call['top_level_url'])
                if is_third:
                    script_party_status[script_key]['third'] += 1
                else:
                    script_party_status[script_key]['first'] += 1
            else:
                # Cannot determine party status if top_level_url is missing
                pass 
        
        print("\n  Top 10 scripts calling the target API (by frequency):")
        for script, count in script_counts.most_common(10):
            party_info = script_party_status[script]
            print(f"  - Script: {script}")
            print(f"    Count: {count} (First-party contexts: {party_info['first']}, Third-party contexts: {party_info['third']})")
            
        total_first_party_calls = sum(status['first'] for status in script_party_status.values())
        total_third_party_calls = sum(status['third'] for status in script_party_status.values())
        print(f"\n  Overall Contexts (where determinable):")
        print(f"  - First-party contexts: {total_first_party_calls}")
        print(f"  - Third-party contexts: {total_third_party_calls}")


    # --- Analysis Part 2: Co-occurrence ---
    print("\n--- API Co-occurrence Analysis ---")
    print(f"Analyzing co-occurrence of other FP APIs with '{TARGET_API}' within the same script execution context...")

    # Iterate through the contexts where JS calls happened
    for (visit_id, script_url), symbols_called in js_calls_per_script_visit.items():
        # Check if the target API was called by this script in this visit
        if TARGET_API in symbols_called:
            # If yes, iterate through all symbols called by this script in this visit
            for other_symbol in symbols_called:
                # Count if it's a *different* potential FP API
                if other_symbol != TARGET_API and other_symbol in OTHER_FP_APIS:
                    cooccurrence_counts[other_symbol] += 1

    if not cooccurrence_counts:
         print(f"No co-occurrences found with '{TARGET_API}'.")
    else:
        most_common_cooccurring = cooccurrence_counts.most_common(1)
        if most_common_cooccurring:
            api_name, count = most_common_cooccurring[0]
            print(f"Most frequent co-occurring API with '{TARGET_API}':")
            print(f"  - API: {api_name}")
            print(f"  - Co-occurrence Count: {count} (times seen in the same script context as the target)")

            print("\n  Top 5 co-occurring APIs:")
            for api, num in cooccurrence_counts.most_common(5):
                 print(f"  - {api}: {num}")

except sqlite3.Error as e:
    print(f"Database error: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
finally:
    if conn:
        conn.close()
        print("\nDatabase connection closed.")

print("\nScript finished.")