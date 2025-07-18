import os
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from azure.storage.blob import BlobServiceClient, ContentSettings

# === CONFIGURATION ===
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=dfm4512768544;AccountKey=cMRgM2an2ssceMNmlCyqPDL+YH1uqGTqveUG7aHlIiwyjsWWhn1XzCNbgsrTAn7WPrZO5HK54m2q+AStq5GF0w==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "dfm-main-website"
TARGET_URL = "https://dfm.idaho.gov/federal-funds-inventory/"
DOCUMENT_TYPE = "Budget Activities Summary"
DOWNLOAD_DIR = "downloaded_files/federal-funds-inventory"

VALID_EXTENSIONS = [".pdf", ".docx", ".xlsx", ".doc"]

FISCAL_YEARS = [
    "Fiscal Year 2025", "Fiscal Year 2024", "Fiscal Year 2023",
    "Fiscal Year 2022", "Fiscal Year 2021", "Fiscal Year 2020",
    "Fiscal Year 2019", "Fiscal Year 2018", "Fiscal Year 2017"
]

blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

def normalize_blob_name(file_url):
    parsed = urlparse(file_url)
    return parsed.path.lstrip("/")

def get_content_type(filename):
    ext = filename.lower()
    if ext.endswith(".pdf"):
        return "application/pdf"
    if ext.endswith(".docx"):
        return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    if ext.endswith(".xlsx"):
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if ext.endswith(".doc"):
        return "application/msword"
    return "application/octet-stream"

def download_file(file_url, dest_path):
    try:
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        r = requests.get(file_url, stream=True, timeout=15)
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"⬇️ Downloaded: {file_url}")
        return True
    except Exception as e:
        print(f"❌ Failed to download {file_url}: {e}")
        return False

def upload_to_azure(file_path, blob_name, document_type):
    try:
        blob_client = container_client.get_blob_client(blob_name)
        content_type = get_content_type(file_path)
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True, content_settings=ContentSettings(content_type=content_type))
        blob_client.set_blob_metadata({"documentType": document_type})
        blob_client.set_blob_tags({"index": blob_name})
        print(f"✅ Uploaded & tagged: {blob_name}")
    except Exception as e:
        print(f"❌ Failed to upload {blob_name} to Azure: {e}")

def process_download_links(soup, base_url):
    files_found = 0
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if any(href.lower().endswith(ext) for ext in VALID_EXTENSIONS):
            file_url = urljoin(base_url, href)
            blob_name = normalize_blob_name(file_url)
            local_path = os.path.join(DOWNLOAD_DIR, blob_name)
            if download_file(file_url, local_path):
                upload_to_azure(local_path, blob_name, DOCUMENT_TYPE)
                files_found += 1
    return files_found

def crawl_and_process_fiscal_links():
    try:
        response = requests.get(TARGET_URL, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        print(f"⚠️ Failed to crawl base URL: {TARGET_URL} → {e}")
        return

    year_links = {}

    # Step 1: Find each fiscal year link
    for link in soup.find_all("a", href=True):
        text = link.get_text(strip=True)
        if text in FISCAL_YEARS:
            year_links[text] = urljoin(TARGET_URL, link["href"])

    total_files = 0

    for year_label, year_url in year_links.items():
        print(f"\n📂 Processing: {year_label} → {year_url}")
        try:
            year_response = requests.get(year_url, timeout=15)
            year_response.raise_for_status()
            year_soup = BeautifulSoup(year_response.text, "html.parser")
        except Exception as e:
            print(f"❌ Failed to fetch {year_url}: {e}")
            continue

        # 1️⃣ Try downloading files directly from the page
        total_files += process_download_links(year_soup, year_url)

        # 2️⃣ Go one level deeper: follow each link and check for documents
        for a_tag in year_soup.find_all("a", href=True):
            sub_href = a_tag["href"]
            sub_url = urljoin(year_url, sub_href)
            try:
                sub_resp = requests.get(sub_url, timeout=15)
                sub_resp.raise_for_status()
                sub_soup = BeautifulSoup(sub_resp.text, "html.parser")
                total_files += process_download_links(sub_soup, sub_url)
            except Exception:
                continue  # silently skip failed sub-pages

    print(f"\n🔍 Finished. Total files processed: {total_files}")

# === MAIN ===
if __name__ == "__main__":
    print(f"🚀 Starting crawl for hardcoded fiscal year links...")
    crawl_and_process_fiscal_links()
    print("🎉 All done.")
