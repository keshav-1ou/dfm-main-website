import os
import re
import requests
from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup
from azure.storage.blob import BlobServiceClient

# Azure Blob Storage Configuration
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=dfm4512768544;AccountKey=cMRgM2an2ssceMNmlCyqPDL+YH1uqGTqveUG7aHlIiwyjsWWhn1XzCNbgsrTAn7WPrZO5HK54m2q+AStq5GF0w==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "dfm-main-website-docs"

# Initialize Azure Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

visited_urls = set()

def normalize_year(year_str):
    """Extract and return only the 4-digit year."""
    if not year_str:
        return None
    match = re.search(r'20\d{2}', year_str)
    return match.group(0) if match else None

def extract_year_from_url(url):
    """Extract year from URL query parameter like ?fy=2026 or ?fy=FY-2026."""
    query_params = parse_qs(urlparse(url).query)
    raw_year = query_params.get('fy', [None])[0]
    return normalize_year(raw_year)

def extract_year_from_text(text):
    """Extract year from PDF link text or href like 'FY-2025', 'FY 2024', or '2023'."""
    if not text:
        return None
    match = re.search(r'(FY[\s\-]?20\d{2}|20\d{2})', text, re.IGNORECASE)
    return normalize_year(match.group(0)) if match else None

def process_pdf(pdf_url, current_page_url, parent_url, level, goal, group, year):
    print(f"Processing PDF: {pdf_url}")

    parsed_url = urlparse(pdf_url)
    pdf_path = parsed_url.path.lstrip('/')
    folder_structure = os.path.dirname(pdf_path)
    pdf_name = os.path.basename(pdf_path)
    blob_name = f"{folder_structure}/{pdf_name}"
    blob_client = container_client.get_blob_client(blob_name)

    try:
        blob_client.get_blob_properties()
        print(f"PDF already exists in Azure Blob Storage: {blob_name}")
    except Exception:
        print(f"Uploading PDF: {blob_name}")
        try:
            response = requests.get(pdf_url)
            response.raise_for_status()
            blob_client.upload_blob(response.content, overwrite=True)
        except Exception as e:
            print(f"Failed to download/upload PDF: {pdf_url} | Error: {e}")
            return None

    metadata = {
        'currentLink': current_page_url,
        'parentLink': parent_url,
        'level': str(level),
        'year': year or 'N/A',
        'goal': goal or 'N/A',
        'group': group or 'N/A'
    }

    blob_client.set_blob_metadata(metadata)

    return {
        'pdf_url': pdf_url,
        'currentLink': current_page_url,
        'parentLink': parent_url,
        'level': level,
        'year': year,
        'goal': goal,
        'group': group
    }

def crawl(url, parent_url=None, level=0, inherited_year=None):
    if url in visited_urls:
        return []

    visited_urls.add(url)
    metadata_list = []

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
    except Exception as e:
        print(f"Failed to fetch page: {url} | Error: {e}")
        return []

    # Extract or inherit year from URL
    year_from_url = extract_year_from_url(url)
    page_year = inherited_year or year_from_url

    # Find all section containers
    sections = soup.find_all('div', class_='col-6')

    for section in sections:
        goal_div = section.find_previous('div', class_='top-section-title')
        goal_text = goal_div.get_text(strip=True) if goal_div else 'Unknown'

        current_group = None

        # Walk through children in order to associate group titles properly
        for elem in section.find_all(['p', 'a']):
            if 'sub-section-title' in elem.get('class', []):
                current_group = elem.get_text(strip=True)
            elif elem.name == 'a' and elem.get('href', '').lower().endswith('.pdf'):
                href = elem['href']
                pdf_url = urljoin(url, href)

                link_text = elem.get_text(strip=True)
                year_from_pdf = extract_year_from_text(link_text) or extract_year_from_text(href)
                final_year = year_from_pdf or page_year

                metadata = process_pdf(
                    pdf_url=pdf_url,
                    current_page_url=url,
                    parent_url=parent_url,
                    level=level,
                    goal=goal_text,
                    group=current_group,
                    year=final_year
                )

                if metadata:
                    metadata_list.append(metadata)

    # Fallback: If no sections (e.g., homepage), scan all PDF links
    if not sections:
        goal_text = 'Unknown'
        current_group = None
        for elem in soup.find_all('a', href=True):
            href = elem['href']
            if href.lower().endswith('.pdf'):
                pdf_url = urljoin(url, href)
                link_text = elem.get_text(strip=True)
                year_from_pdf = extract_year_from_text(link_text) or extract_year_from_text(href)
                final_year = year_from_pdf or page_year

                metadata = process_pdf(
                    pdf_url=pdf_url,
                    current_page_url=url,
                    parent_url=parent_url,
                    level=level,
                    goal=goal_text,
                    group=current_group,
                    year=final_year
                )

                if metadata:
                    metadata_list.append(metadata)

    # Crawl internal non-PDF links
    for link in soup.find_all('a', href=True):
        href = link['href']
        next_url = urljoin(url, href)
        if urlparse(next_url).netloc == urlparse(url).netloc and not next_url.lower().endswith('.pdf'):
            metadata_list.extend(crawl(next_url, url, level + 1, inherited_year=page_year))

    return metadata_list

# Start crawling
homepage = 'https://dfm.idaho.gov/'
pdf_metadata = crawl(homepage)

# Output result
for metadata in pdf_metadata:
    print(metadata)
