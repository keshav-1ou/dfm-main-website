import os
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from azure.storage.blob import BlobServiceClient, ContentSettings
import re
# === CONFIGURATION ===
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=dfm4512768544;AccountKey=cMRgM2an2ssceMNmlCyqPDL+YH1uqGTqveUG7aHlIiwyjsWWhn1XzCNbgsrTAn7WPrZO5HK54m2q+AStq5GF0w==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "dfm-main-website"
TARGET_URL = "https://dfm.idaho.gov/publication/?type=budget&level=summary"
DOWNLOAD_DIR = r"C:\Temp\downloaded_files"

VALID_EXTENSIONS = [".pdf", ".docx", ".xlsx", ".doc"]
EXTENSION_CONTENT_TYPES = {
    '.pdf': "application/pdf",
    '.docx': "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    '.doc': "application/msword",
    '.xlsx': "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
}

link_to_document_type = {
    "https://dfm.idaho.gov/budget-development-manual/": "Budget Development Manual",
    "https://dfm.idaho.gov/publication/fy-2025-executive-budget-publications/": "Executive Budget Publications",
    "https://dfm.idaho.gov/publication/budgetrequests/?fy=2026": "Agency Budget Request",
    "https://dfm.idaho.gov/publication/?type=budget&level=strategic-plans": "Strategic Plans",
    "https://dfm.idaho.gov/budgetactivities/": "Budget Activities Summary",
    "https://dfm.idaho.gov/publication/performance-report-templates/": "Trainings Materials",
    "https://dfm.idaho.gov/strategic-plans-and-performance-reports/": "Performance Report Templates",
    "https://dfm.idaho.gov/publication/?type=budget&level=performance": "Performance Report",
    "https://dfm.idaho.gov/publication/?type=budget&level=detail": "Executive Budget Details",
    "https://dfm.idaho.gov/statewide-cost-allocation-program-swcap/": "Summary of Fixed Costs Schedule H",
    "https://dfm.idaho.gov/publication/?type=budget&level=summary": "Executive Budget Summary",
    "https://dfm.idaho.gov/federal-funds-inventory/": "Budget Activities Summary"
}

blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

def strip_fragment(url):
    parsed = urlparse(url)
    return parsed._replace(fragment="").geturl()


def extract_year_from_text(text):
    match = re.search(r"(20\d{2})", text)
    return match.group(1) if match else None


def get_content_type(file_url):
    for ext, mime in EXTENSION_CONTENT_TYPES.items():
        if file_url.lower().endswith(ext):
            return mime
    return None


def normalize_blob_name(file_url):
    return urlparse(file_url).path.lstrip("/")


def save_file_locally(file_url):
    parsed = urlparse(file_url)
    relative_path = parsed.path.lstrip("/")
    local_path = os.path.join(DOWNLOAD_DIR, relative_path)

    if os.path.exists(local_path):
        print(f"🟡 Already downloaded: {file_url}")
        return local_path

    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        response = requests.get(file_url, stream=True, timeout=10)
        response.raise_for_status()

        with open(local_path, "wb") as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)

        print(f"✅ Downloaded: {file_url}")
        return local_path

    except Exception as e:
        print(f"❌ Failed to download {file_url}: {e}")
        return None


def upload_file_to_blob(local_path, blob_path, metadata, tags):
    try:
        blob_client = container_client.get_blob_client(blob_path)
        ext = os.path.splitext(local_path)[1].lower()
        content_type = EXTENSION_CONTENT_TYPES.get(ext, "application/octet-stream")

        # with open(local_path, "rb") as data:
        #     blob_client.upload_blob(
        #         data,
        #         overwrite=True,
        #         content_settings=ContentSettings(content_type=content_type),
        #         metadata=metadata
        #     )
        # blob_client.set_blob_tags(tags)
        
        metadata['file'] = blob_client.url
        print(f"☁️ Uploaded: {blob_path} | Metadata: {metadata} | Tags: {tags}")
        print(" ")
        print(" ")
    except Exception as e:
        print(f"❌ Upload failed: {blob_path} → {e}")



def crawl_single_page_and_update(url, document_type, page_year=None):
    print(f"🌐 Visiting: {url}")
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        print(f"⚠️ Failed to fetch page: {url} → {e}")
        return

    sections = soup.find_all('div', class_='col-6')
    processed_files = set()

    if sections:
        for section in sections:
            goal_div = section.find_previous('div', class_='top-section-title')
            goal_text = goal_div.get_text(strip=True) if goal_div else 'N/A'
            current_agency = None

            for elem in section.find_all(['p', 'a']):
                if 'sub-section-title' in elem.get('class', []):
                    current_agency = elem.get_text(strip=True)
                elif elem.name == 'a':
                    href = elem.get('href', '')
                    if get_content_type(href):
                        file_url = urljoin(url, href)
                        if file_url in processed_files:
                            continue
                        processed_files.add(file_url)

                        # ✅ Extract <strong> within the same <a>
                        doc_name_tag = elem.find("strong")
                        doc_name = doc_name_tag.get_text(strip=True) if doc_name_tag else "N/A"

                        link_text = elem.get_text(strip=True)
                        year = extract_year_from_text(link_text) or extract_year_from_text(href) or page_year
                        local_path = save_file_locally(file_url)

                        if local_path:
                            blob_path = normalize_blob_name(file_url)
                            metadata = {
                                'documentType': document_type,
                                'year': year or 'N/A',
                                'goal': goal_text,
                                'agency': current_agency or 'N/A',
                                'currentLink': strip_fragment(url),
                                'docName': doc_name  # ✅ Added here
                            }
                            tags = {
                                'year': year,
                                'goal': goal_text,
                                'agency': current_agency or 'N/A',
                                'documentType': document_type
                            }
                            upload_file_to_blob(local_path, blob_path, metadata, tags)
    else:
        goal_text = 'N/A'
        current_agency = None
        for link in soup.find_all('a', href=True):
            href = link['href']
            if get_content_type(href):
                file_url = urljoin(url, href)
                if file_url in processed_files:
                    continue
                processed_files.add(file_url)

                link_text = link.get_text(strip=True)
                year = extract_year_from_text(link_text) or extract_year_from_text(href) or page_year
                local_path = save_file_locally(file_url)
                header = "Unknown"
                current = link
                while current:
                    current = current.find_previous_sibling()
                    if current and current.name == "h2":
                        header = current.get_text(strip=True)
                        break
                if local_path:
                    blob_path = normalize_blob_name(file_url)
                    metadata = {
                            'documentType': document_type,
                            'year': year or 'N/A',
                            'header': header or 'N/A',
                            'goal': goal_text,
                            'agency': current_agency or 'N/A',
                            'currentLink': strip_fragment(url),
                            'docName': doc_name
                        }
                    tags = {
                            'documentType': document_type,
                            'year': year or 'N/A',
                            'header': header or 'N/A'
                        }
                    upload_file_to_blob(local_path, blob_path, metadata, tags)




def crawl_single_page_and_update_publications(url):
    print(f"🌐 Visiting: {url}")
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        print(f"⚠️ Failed to fetch page: {url} → {e}")
        return

    for link in soup.find_all("a", href=True):
        href = link["href"]
        full_url = urljoin(url, href)
        if get_content_type(full_url):
            h3 = link.find_previous("h3")
            document_type = h3.get_text(strip=True) if h3 else "Unknown"
            local_path = save_file_locally(full_url)
            if local_path:
                blob_path = normalize_blob_name(full_url)
                upload_file_to_blob(local_path, blob_path, document_type)


def crawl_with_years(url, base_document_type):
    print(f"📆 Crawling all years from: {url}")
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        print(f"❌ Failed to fetch base page for dropdown: {url} → {e}")
        return

    select_tag = soup.find("select", {"id": "page-year"})
    if not select_tag:
        print("⚠️ No year dropdown found on page.")
        return

    year_options = [option.get("value") for option in select_tag.find_all("option") if option.get("value")]
    print(f"📅 Found years: {year_options}")
    for year in year_options:
        year_url = f"{url}&fy={year}"
        print(f"\n➡️ Crawling year: {year} → {year_url}")
        crawl_single_page_and_update(year_url, base_document_type, year)


# === MAIN ===
if __name__ == "__main__":
    print(f"\n🚀 Starting crawl from: {TARGET_URL}")

    if TARGET_URL == "https://dfm.idaho.gov/publication/economicpublications/":
        crawl_single_page_and_update_publications(TARGET_URL)
    else:
        DOCUMENT_TYPE = link_to_document_type.get(TARGET_URL, "Unknown")
        print(f"🧾 Document Type to set: {DOCUMENT_TYPE}")
        if TARGET_URL in [
            "https://dfm.idaho.gov/publication/?type=budget&level=detail",
            "https://dfm.idaho.gov/publication/?type=budget&level=summary",
            "https://dfm.idaho.gov/publication/?type=budget&level=performance",
            "https://dfm.idaho.gov/publication/?type=budget&level=strategic-plans"
        ]:
            crawl_with_years(TARGET_URL, DOCUMENT_TYPE)
        else:
            crawl_single_page_and_update(TARGET_URL, DOCUMENT_TYPE)

    print("\n✅ Crawl complete. All files downloaded and uploaded.\n")
