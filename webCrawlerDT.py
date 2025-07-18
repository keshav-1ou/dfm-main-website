import os
import re
import requests
from urllib.parse import urljoin, urlparse, parse_qs, urlunparse
from bs4 import BeautifulSoup
from azure.storage.blob import BlobServiceClient, ContentSettings

# Azure Blob Storage Configuration
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=dfm4512768544;AccountKey=cMRgM2an2ssceMNmlCyqPDL+YH1uqGTqveUG7aHlIiwyjsWWhn1XzCNbgsrTAn7WPrZO5HK54m2q+AStq5GF0w==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "dfm-main-website"

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
}

VALID_EXTENSIONS = [
    ".pdf", ".doc", ".docx", ".xls", ".xlsx",
    ".odt", ".ods", ".csv", ".tsv",
    ".ppt", ".pptx", ".odp",
    ".rtf", ".txt"
]

EXTENSION_CONTENT_TYPES = {
    '.pdf': "application/pdf",
    '.docx': "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    '.doc': "application/msword",
    '.xlsx': "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    '.xls': "application/vnd.ms-excel",
    '.ppt': "application/vnd.ms-powerpoint",
    '.pptx': "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    '.csv': "text/csv",
    '.tsv': "text/tab-separated-values",
    '.txt': "text/plain",
    '.odt': "application/vnd.oasis.opendocument.text",
    '.ods': "application/vnd.oasis.opendocument.spreadsheet",
    '.odp': "application/vnd.oasis.opendocument.presentation",
    '.rtf': "application/rtf"
}

blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

visited_urls = set()

def normalize_year(year_str):
    if not year_str:
        return None
    match = re.search(r'20\d{2}', year_str)
    return match.group(0) if match else None

def extract_year_from_url(url):
    query_params = parse_qs(urlparse(url).query)
    raw_year = query_params.get('fy', [None])[0]
    return normalize_year(raw_year)

def extract_year_from_text(text):
    if not text:
        return None
    match = re.search(r'(FY[\s\-]?20\d{2}|20\d{2})', text, re.IGNORECASE)
    return normalize_year(match.group(0)) if match else None

def strip_fragment(url):
    return urlunparse(urlparse(url)._replace(fragment=''))

def get_content_type(file_url):
    ext = os.path.splitext(file_url)[1].lower()
    return EXTENSION_CONTENT_TYPES.get(ext, "application/octet-stream")

def process_file(file_url, current_page_url, goal, agency, year):
    print(f"Processing: {file_url}")
    parsed_url = urlparse(file_url)
    blob_name = parsed_url.path.lstrip('/')
    blob_client = container_client.get_blob_client(blob_name)

    content_type = get_content_type(file_url)

    # Try to get existing metadata
    try:
        props = blob_client.get_blob_properties()
        existing_metadata = props.metadata or {}
        print(f"✔️ Already exists: {blob_name}")
    except Exception:
        existing_metadata = {}
        print(f"⬆️ Uploading: {blob_name}")
        try:
            response = requests.get(file_url)
            response.raise_for_status()
            # blob_client.upload_blob(
            #     response.content,
            #     overwrite=True,
            #     content_settings=ContentSettings(content_type=content_type)
            # )
        except Exception as e:
            print(f"❌ Failed to upload: {file_url} → {e}")
            return None

    existing_doc_type = existing_metadata.get("documentType")
    final_doc_type = existing_doc_type if existing_doc_type else "N/A"

    metadata = {
        'file': blob_client.url,
        'currentLink': strip_fragment(current_page_url),
        'year': year or 'N/A',
        'goal': goal or 'N/A',
        'agency': agency or 'N/A',
        'documentType': final_doc_type
    }

    tags = {
        'documentType': final_doc_type
    }

    # blob_client.set_blob_metadata(metadata)
    # blob_client.set_blob_tags(tags)

    print(f"✅ Metadata set for {blob_name} with metadata {metadata} and tags {tags}")
    return metadata

def crawl(url, inherited_year=None):
    if url in visited_urls:
        return []
    visited_urls.add(url)

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
    except Exception as e:
        print(f"⚠️ Failed to fetch: {url} → {e}")
        return []
        # Check for year dropdown and crawl each year separately

    metadata_list = []
    year_from_url = extract_year_from_url(url)
    page_year = inherited_year or year_from_url

    sections = soup.find_all('div', class_='col-6')

    for section in sections:
        goal_div = section.find_previous('div', class_='top-section-title')
        goal_text = goal_div.get_text(strip=True) if goal_div else 'N/A'
        current_agency = None

        for elem in section.find_all(['p', 'a']):
            if 'sub-section-title' in elem.get('class', []):
                current_agency = elem.get_text(strip=True)
            elif elem.name == 'a':
                href = elem.get('href', '')
                if any(href.lower().endswith(ext) for ext in VALID_EXTENSIONS):
                    file_url = urljoin(url, href)
                    link_text = elem.get_text(strip=True)
                    year = extract_year_from_text(link_text) or extract_year_from_text(href) or page_year
                    metadata = process_file(file_url, url, goal_text, current_agency, year)
                    if metadata:
                        metadata_list.append(metadata)

    if not sections:
        goal_text = 'N/A'
        current_agency = None
        for elem in soup.find_all('a', href=True):
            href = elem['href']
            if any(href.lower().endswith(ext) for ext in VALID_EXTENSIONS):
                file_url = urljoin(url, href)
                link_text = elem.get_text(strip=True)
                year = extract_year_from_text(link_text) or extract_year_from_text(href) or page_year
                metadata = process_file(file_url, url, goal_text, current_agency, year)
                if metadata:
                    metadata_list.append(metadata)

    for link in soup.find_all('a', href=True):
        href = link['href']
        next_url = urljoin(url, href)
        if urlparse(next_url).netloc == urlparse(url).netloc and not any(next_url.lower().endswith(ext) for ext in VALID_EXTENSIONS):
            metadata_list.extend(crawl(next_url, inherited_year=page_year))

    return metadata_list

# === MAIN ===
if __name__ == "__main__":
    homepage = 'https://dfm.idaho.gov/publication/?type=budget&level=detail'
    all_metadata = crawl(homepage)
    for m in all_metadata:
        print(m)
