import os
import requests
from urllib.parse import urljoin, urlparse, urldefrag
from bs4 import BeautifulSoup

# === CONFIGURATION ===
BASE_URL = "https://dfm.idaho.gov/"
OUTPUT_DIR = "downloaded_files"
VALID_EXTENSIONS = [
    ".pdf", ".doc", ".docx", ".xls", ".xlsx",
    ".odt", ".ods", ".csv", ".tsv",
    ".ppt", ".pptx", ".odp",
    ".rtf", ".txt"
    # optionally: ".zip", ".rar", ".7z"
]

visited_links = set()
downloaded_files = set()

os.makedirs(OUTPUT_DIR, exist_ok=True)


def normalize_url(url):
    # Strip fragment like #main
    return urldefrag(url)[0]


def is_valid_file(url):
    return any(url.lower().endswith(ext) for ext in VALID_EXTENSIONS)


def get_filename_from_url(url):
    return os.path.basename(urlparse(url).path)


def download_file(file_url):
    parsed_url = urlparse(file_url)
    file_path = parsed_url.path.lstrip("/")  # Remove leading /
    local_path = os.path.join(OUTPUT_DIR, file_path)
    local_dir = os.path.dirname(local_path)

    if os.path.exists(local_path):
        print(f"🟡 Already downloaded: {file_path}")
        downloaded_files.add(file_url)
        return

    os.makedirs(local_dir, exist_ok=True)

    try:
        print(f"⬇️ Downloading: {file_path}")
        response = requests.get(file_url, stream=True, timeout=10)
        response.raise_for_status()

        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        downloaded_files.add(file_url)
    except Exception as e:
        print(f"❌ Failed to download {file_url}: {e}")

def crawl(url):
    url = normalize_url(url)
    if url in visited_links:
        return
    visited_links.add(url)

    try:
        print(f"🔍 Visiting: {url}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        for link_tag in soup.find_all("a", href=True):
            href = link_tag["href"]
            full_url = normalize_url(urljoin(url, href))

            if is_valid_file(full_url):
                if full_url not in downloaded_files:
                    download_file(full_url)
            else:
                parsed_url = urlparse(full_url)
                if parsed_url.netloc == urlparse(BASE_URL).netloc:
                    crawl(full_url)

    except Exception as e:
        print(f"⚠️ Error visiting {url}: {e}")


if __name__ == "__main__":
    crawl(BASE_URL)
    print("✅ Crawl complete.")
