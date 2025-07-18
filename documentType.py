import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from azure.storage.blob import BlobServiceClient, ContentSettings
import requests
from bs4 import BeautifulSoup

# === CONFIGURATION ===
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=dfm4512768544;AccountKey=cMRgM2an2ssceMNmlCyqPDL+YH1uqGTqveUG7aHlIiwyjsWWhn1XzCNbgsrTAn7WPrZO5HK54m2q+AStq5GF0w==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "dfm-main-website"
TARGET_URL = "https://dfm.idaho.gov/publication/?type=budget&level=summary"


VALID_EXTENSIONS = [".pdf", ".docx", ".xlsx", ".doc"]

def crawl(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        print(f"❌ Failed to fetch page: {url} → {e}")
        return {}

    doc_type_map = {}

    for link in soup.find_all("a", href=True):
        href = link["href"].lower()
        if any(href.endswith(ext) for ext in VALID_EXTENSIONS):
            h3 = link.find_previous("h3")
            doc_type = h3.get_text(strip=True) if h3 else "Unknown"
            full_url = requests.compat.urljoin(url, href)
            doc_type_map[full_url] = doc_type

    return doc_type


link_to_document_type = {
    "https://dfm.idaho.gov/budget-development-manual/": "Budget Development Manual",
    "https://dfm.idaho.gov/publication/fy-2025-executive-budget-publications/": "Executive Budget Publications",
    "https://dfm.idaho.gov/publication/budgetrequests/?fy=2026": "Agency Budget Request",
    "https://dfm.idaho.gov/budgetactivities/": "Budget Activities Summary",
    "https://dfm.idaho.gov/publication/performance-report-templates/": "Trainings Materials",
    "https://dfm.idaho.gov/strategic-plans-and-performance-reports/": "Performance Report Templates",
    "https://dfm.idaho.gov/publication/?type=budget&level=performance": "Performance Report",
    "https://dfm.idaho.gov/publication/?type=budget&level=detail": "Executive Budget Details",
    "https://dfm.idaho.gov/statewide-cost-allocation-program-swcap/": "Summary of Fixed Costs Schedule H",
    "https://dfm.idaho.gov/publication/?type=budget&level=summary": "Executive Budget Summary",
    "https://dfm.idaho.gov/federal-funds-inventory/": "Budget Activities Summary"
    #"https://dfm.idaho.gov/publication/economicpublications/": crawl("https://dfm.idaho.gov/publication/economicpublications/")
}

DOCUMENT_TYPE = link_to_document_type.get(TARGET_URL, "Unknown")

blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Supported extensions and their MIME types
EXTENSION_CONTENT_TYPES = {
    '.pdf': "application/pdf",
    '.docx': "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    '.doc': "application/msword",
    '.xlsx': "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
}

def normalize_blob_name(file_url):
    parsed = urlparse(file_url)
    return parsed.path.lstrip("/")


def get_content_type(file_url):
    for ext, mime in EXTENSION_CONTENT_TYPES.items():
        if file_url.lower().endswith(ext):
            return mime
    return None

def update_blob(blob_name, document_type, content_type):
    try:
        print(f"📦 Attempting to update blob: {blob_name}")
        blob_client = container_client.get_blob_client(blob_name)

        props = blob_client.get_blob_properties()
        current_metadata = props.metadata or {}
        document_type
        tags = {
            "documentType" : document_type
        }
        # Update metadata
        current_metadata["documentType"] = document_type
        print(f"🔁 Existing metadata: {props.metadata}")
        print(f"📝 Setting new metadata: {current_metadata}")
        print("Tags: ", tags.get("documentType"))
        # blob_client.set_blob_metadata(current_metadata)
        # blob_client.set_blob_tags(tags)

        # Update content-type
        blob_client.set_http_headers(content_settings=ContentSettings(content_type=content_type))
        print(f"✅ Updated: {blob_name} | Type: {content_type} | documentType: {document_type}")

    except Exception as e:
        print(f"❌ Failed to update blob: {blob_name} → {e}")

def crawl_single_page_and_update(url, document_type):
    print(f"🌐 Visiting: {url}")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        print(f"⚠️ Failed to fetch page: {url} → {e}")
        return

    files_updated = 0

    for link in soup.find_all("a", href=True):
        href = link["href"]
        full_url = urljoin(url, href)
        content_type = get_content_type(full_url)

        if content_type:
            blob_name = normalize_blob_name(full_url)
            print(f"📄 Found file: {full_url}")
            update_blob(blob_name, document_type, content_type)
            files_updated += 1

    print(f"🔍 Done with: {url} | Files updated: {files_updated}")

def crawl_single_page_and_update_publications(url):
    print(f"🌐 Visiting: {url}")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        print(f"⚠️ Failed to fetch page: {url} → {e}")
        return

    files_updated = 0

    for link in soup.find_all("a", href=True):
        href = link["href"]
        full_url = urljoin(url, href)
        content_type = get_content_type(full_url)

        if content_type:
            h3 = link.find_previous("h3")
            document_type = h3.get_text(strip=True) if h3 else "Unknown"

            blob_name = normalize_blob_name(full_url)
            print(f"📄 Found file: {full_url}")
            update_blob(blob_name, document_type, content_type)
            files_updated += 1

    print(f"🔍 Done with: {url} | Files updated: {files_updated}")

def crawl_with_years(url, base_document_type):
    print(f"📆 Crawling all years from: {url}")
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        print(f"❌ Failed to fetch base page for dropdown: {url} → {e}")
        return

    # Find year dropdown
    select_tag = soup.find("select", {"id": "page-year"})
    if not select_tag:
        print("⚠️ No year dropdown found on page.")
        return

    year_options = [option.get("value") for option in select_tag.find_all("option") if option.get("value")]
    
    if not year_options:
        print("⚠️ No year options found.")
        return

    print(f"📅 Found years: {year_options}")
    
    for year in year_options:
        year_url = f"{url}&fy={year}"
        print(f"\n➡️ Crawling year: {year} → {year_url}")
        crawl_single_page_and_update(year_url, base_document_type)

# === MAIN ===
if __name__ == "__main__":
    print(f"\n🚀 Starting crawl from: {TARGET_URL}")

    if TARGET_URL == "https://dfm.idaho.gov/publication/economicpublications/":
        print(f"🔍 Using dynamic h3 extraction for documentType...")
        crawl_single_page_and_update_publications(TARGET_URL)
    else:
        DOCUMENT_TYPE = link_to_document_type.get(TARGET_URL, "Unknown")
        print(f"🧾 Document Type to set: {DOCUMENT_TYPE}")
        
        if TARGET_URL in [
        "https://dfm.idaho.gov/publication/?type=budget&level=detail",
        "https://dfm.idaho.gov/publication/?type=budget&level=summary",
        "https://dfm.idaho.gov/publication/?type=budget&level=performance",
            ]: 
            crawl_with_years(TARGET_URL, DOCUMENT_TYPE)
        else:
            crawl_single_page_and_update(TARGET_URL, DOCUMENT_TYPE)


    print("\n✅ All matching blobs updated.\n")

