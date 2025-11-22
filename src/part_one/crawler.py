# src/part_one/crawler.py
# A web crawler to download animal pages from a-z-animals.com

import urllib.request
import brotli
import os
import re
import time
import csv
import logging
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
from fake_useragent import UserAgent

# Website to get data from
URL = "https://a-z-animals.com/animals/"
# Directory to save HTML files
SAVE_DIR = "animals_data/"
# Metadata CSV file
METADATA_FILE = "url_metadata.csv"
# Store visited urls
VISITED_URLS = "visited_urls.txt"
# UserAgent
ua = UserAgent()
# Settings for not overloading the server
HEADERS = {
    "User-Agent": ua.random,
    "Accept": "text/html",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://www.google.com/"
}
# Timing settings
SLEEP_ = 2
TIMEOUT_ = 5
# Retry settings
MAX_RETRIES = 3
RETRY_BACKOFF = 2

LOG_FILE = "crawler.log"

# Metrics
T_RESPONSE_TIME = 0.0
TOTAL_COMPRESSED_SIZE = 0
TOTAL_UNCOMPRESSED_SIZE = 0
CRAWLED_COUNT = 0
SKIPPED_COUNT = 0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def load_visited_urls():
    """Load previously visited URLs from .txt"""
    visited = set()
    if os.path.exists(VISITED_URLS):
        try:
            with open(VISITED_URLS, "r", encoding="utf-8") as f:
                for line in f:
                    url = line.strip()
                    if url:
                        visited.add(url)
            logger.info(f"Loaded {len(visited)} previously visited URLs")
        except Exception as e:
            logger.error(f"Error loading visited URLs: {e}")
    return visited


def init_csv_for_metadata():
    """Initialize CSV for metadata if not exists"""
    if not os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "URL", "HTTP Status Code", "Response Time (s)", "Crawl Timestamp",
                "HTML Title", "Canonical URL", "robots Directive",
                "Content Type", "Page Size (Raw/Compressed Bytes)", "Page Size (Uncompressed Bytes)"
            ])


def save_visited_url(url):
    """Append a visited URL to the file"""
    try:
        with open(VISITED_URLS, "a", encoding="utf-8") as f:
            f.write(url + '\n')
    except Exception as e:
        logger.error(f"Error saving visited URL: {e}")


def save_metadata(metadata):
    """Append metadata to CSV file"""
    try:
        with open(METADATA_FILE, "a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                metadata.get("url", ""),
                metadata.get("status_code", ""),
                metadata.get("response_time", ""),
                metadata.get("timestamp", ""),
                metadata.get("title", ""),
                metadata.get("canonical", ""),
                metadata.get("robots", ""),
                metadata.get("content_type", ""),
                metadata.get("page_size_compressed", ""),
                metadata.get("page_size_uncompressed", "")
            ])
    except Exception as e:
        logger.error(f"Error saving metadata for {metadata.get('url')}: {e}")


def extract_metadata(url, html, response, response_time, raw_size, uncompressed_size):
    """Extract metadata from HTML and headers"""
    content_type = response.headers.get("Content-Type", "")
    raw_content_type = content_type.split(';')[0].strip() 
    if ';' in raw_content_type:
        cleaned_content_type = raw_content_type.split(';')[0].strip()
    else:
        cleaned_content_type = raw_content_type

    metadata = {
        "url": url,
        "status_code": getattr(response, "status", ""),
        "response_time": round(response_time, 3),
        "timestamp": datetime.now(timezone.utc),
        "title": "",
        "canonical": "",
        "robots": "",
        "content_type": cleaned_content_type,
        "page_size_compressed": str(raw_size),
        "page_size_uncompressed": str(uncompressed_size)
    }

    # Add to global metrics
    global T_RESPONSE_TIME, TOTAL_COMPRESSED_SIZE, TOTAL_UNCOMPRESSED_SIZE
    T_RESPONSE_TIME += response_time
    TOTAL_COMPRESSED_SIZE += raw_size
    TOTAL_UNCOMPRESSED_SIZE += uncompressed_size

    try:
        # Extract title
        title_match = re.search(r'<title>(.*?)</title>', html, re.IGNORECASE | re.DOTALL)
        if title_match:
            metadata["title"] = title_match.group(1).strip()

        # Canonical URL
        canonical_match = re.search(
            r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\'](.*?)["\']', html, re.IGNORECASE)
        if canonical_match:
            metadata["canonical"] = canonical_match.group(1).strip()

        # Robots meta
        robots_match = re.search(
            r'<meta[^>]+name=["\']robots["\'][^>]+content=["\'](.*?)["\']', html, re.IGNORECASE)
        if robots_match:
            metadata["robots"] = robots_match.group(1).strip()
    except Exception as e:
        logger.error(f"Error extracting metadata from {url}: {e}")

    return metadata


def get_filename_from_url(url):
    """Generate filename from URL"""
    url_path = urlparse(url).path
    filename = url_path.strip("/").replace("/", "_")
    if not filename:
        filename = "index"
    if not filename.endswith(".html"):
        filename += ".html"
    return filename


def file_already_exists(url):
    """Check if file for this URL already exists"""
    filename = get_filename_from_url(url)
    filepath = os.path.join(SAVE_DIR, filename)
    return os.path.exists(filepath)


def get_links(html_content, base_url):
    """Extract all links from HTML content using regex, excluding specific sections"""
    links = []
    try:
        # Remove header navigation
        html_cleaned = re.sub(r'<div[^>]*id=["\']headerNav["\'][^>]*>.*?</div>', 
                             '', html_content, flags=re.DOTALL | re.IGNORECASE)
        
        # Pattern to find href attributes
        pattern = r'<a[^>]+href=[\'"](.*?)[\'"][^>]*>'
        matches = re.findall(pattern, html_cleaned, re.IGNORECASE)
        
        for href in matches:
            full_url = urljoin(base_url, href)
            if urlparse(full_url).netloc == urlparse(base_url).netloc:
                links.append(full_url)
        
        # Remove duplicates
        seen = set()
        unique_links = []
        for link in links:
            if link not in seen:
                seen.add(link)
                unique_links.append(link)
        
        return unique_links
    except Exception as e:
        logger.error(f"Error extracting links from {base_url}: {e}")
        return []


def download_page(url):
    """Download the page and measure metadata with retry logic"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            start_time = time.time()
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=TIMEOUT_) as response:
                status_code = response.status
                if status_code == 200:
                    encoding = response.headers.get("Content-Encoding", "").lower()
                    raw_data = response.read()
                    raw_size = len(raw_data)
                    response_time = time.time() - start_time

                    if encoding == "br":
                        html_content = brotli.decompress(raw_data).decode("utf-8", errors="ignore")
                    else:
                        html_content = raw_data.decode("utf-8", errors="ignore")

                    uncompressed_size = len(html_content)
                    logger.info(
                        f"Downloaded {url} (Status: {status_code}, "
                        f"Compressed: {raw_size} bytes, Uncompressed: {uncompressed_size} bytes)"
                    )

                    metadata = extract_metadata(url, html_content, response, response_time, raw_size, uncompressed_size)
                    save_metadata(metadata)
                    return html_content
                else:
                    logger.warning(f"Non-200 status {status_code} for {url}, attempt {attempt}/{MAX_RETRIES}")

        except Exception as e:
            logger.warning(f"Attempt {attempt}/{MAX_RETRIES} failed for {url}: {e}")

        if attempt < MAX_RETRIES:
            sleep_time = RETRY_BACKOFF * (2 ** (attempt - 1))
            logger.info(f"Retrying {url} in {sleep_time:.1f} seconds...")
            time.sleep(sleep_time)

    logger.error(f"Failed to download {url} after {MAX_RETRIES} attempts.")
    SKIPPED_COUNT += 1
    return None
    

def is_detail_page(url):
    """Check if URL is an animal detail page"""
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    
    # Check if it has route after animals
    if path.startswith("animals/") and path != "animals":
        segments = path.split("/")
        if len(segments) > 1 and segments[1]:
            return True
    return False


def is_valid_animal_link(url):
    """Check if the link is a valid animal page"""
    parsed = urlparse(url)
    path = parsed.path.lower()
    
    # Patterns
    exclude_patterns = [
        "/lists/",
    ]
    
    # Check if any exclude pattern is in the path
    for pattern in exclude_patterns:
        if pattern in path:
            return False
    
    return True
        

def use_crawler():
    """Crawl the website using urllib"""
    os.makedirs(SAVE_DIR, exist_ok=True)
    init_csv_for_metadata()
    visited_set = load_visited_urls()
    urls = [URL]
    global CRAWLED_COUNT, SKIPPED_COUNT

    while urls:
        curr_url = urls.pop(0)

        if curr_url in visited_set:
            logger.info(f"Skipping visited: {curr_url}")
            SKIPPED_COUNT += 1
            continue

        if not is_valid_animal_link(curr_url):
            logger.info(f"Skipping invalid: {curr_url}")
            visited_set.add(curr_url)
            save_visited_url(curr_url)
            SKIPPED_COUNT += 1
            continue

        if file_already_exists(curr_url):
            logger.info(f"File already exists: {curr_url}")
            SKIPPED_COUNT += 1
            try:
                filename = get_filename_from_url(curr_url)
                filepath = os.path.join(SAVE_DIR, filename)
                with open(filepath, "r", encoding="utf-8") as f:
                    html_page = f.read()
                    new_links = get_links(html_page, curr_url)
                    for link in new_links:
                        if link not in visited_set and link not in urls:
                            if "/animals/" in link:
                                urls.append(link)
            except Exception as e:
                logger.error(f"Error reading cached file: {e}")
            continue

        html_page = download_page(curr_url)

        if html_page:
            filename = get_filename_from_url(curr_url)
            filepath = os.path.join(SAVE_DIR, filename)
            try:
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(html_page)
                logger.info(f"Saved {filepath} (#{CRAWLED_COUNT})")
            except Exception as e:
                logger.error(f"Error saving file {filepath}: {e}")

            if not is_detail_page(curr_url):
                new_links = get_links(html_page, curr_url)
                for link in new_links:
                    if link not in visited_set and link not in urls:
                        if "/animals/" in link:
                            urls.append(link)
                logger.info(f"Extracted {len(new_links)} new links from {curr_url}")

            visited_set.add(curr_url)
            save_visited_url(curr_url)
            CRAWLED_COUNT += 1
            time.sleep(SLEEP_)


def generate_report(runtime):
    """Calculates and prints statistics.""" 
    crawl_rate = CRAWLED_COUNT / runtime if runtime > 0 else 0
    avg_response_time = T_RESPONSE_TIME / CRAWLED_COUNT if CRAWLED_COUNT > 0 else 0

    logger.info("-" * 20)
    logger.info("Crawl Summary Report")
    logger.info("-" * 20)
    
    logger.info(f"{'Number of Crawled Pages:':<30} {CRAWLED_COUNT}")
    logger.info(f"{'Skipped URLs:':<30} {SKIPPED_COUNT}")
    
    logger.info(f"{'Total Runtime:':<30} {runtime:.2f} seconds")
    logger.info(f"{'Crawl Rate:':<30} {crawl_rate:.2f} pages/second")
    logger.info(f"{'Average Response Time:':<30} {avg_response_time:.3f} seconds")

    logger.info(f"{'Total Compressed Size:':<30} {TOTAL_COMPRESSED_SIZE} bytes")
    logger.info(f"{'Total Uncompressed Size:':<30} {TOTAL_UNCOMPRESSED_SIZE} bytes")
    logger.info("-" * 20)


if __name__ == "__main__":
    start_time = time.time()
    logger.info(f"Starting web crawler for {URL}")
    
    try:
        use_crawler()
        
    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
        pass 
    
    finally:
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info("Web crawling ended...")
        
        generate_report(duration)