import os
import re
import requests
from urllib.parse import urljoin, urlparse, parse_qs, urlunparse
from bs4 import BeautifulSoup
from azure.storage.blob import BlobServiceClient, ContentSettings

# Azure Blob Storage Configuration
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=dfm4512768544;AccountKey=cMRgM2an2ssceMNmlCyqPDL+YH1uqGTqveUG7aHlIiwyjsWWhn1XzCNbgsrTAn7WPrZO5HK54m2q+AStq5GF0w==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "dfm-main-website-docs"

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

def process_pdf(pdf_url, current_page_url, parent_url, level, goal, group, year, documentType):
    print(f"Processing PDF: {pdf_url}")

    parsed_url = urlparse(pdf_url)
    blob_name = parsed_url.path.lstrip('/')
    blob_client = container_client.get_blob_client(blob_name)

    try:
        blob_client.get_blob_properties()
        print(f"PDF already exists in Azure Blob Storage: {blob_name}")
    except Exception:
        print(f"Uploading PDF: {blob_name}")
        try:
            response = requests.get(pdf_url)
            response.raise_for_status()
            blob_client.upload_blob(
                response.content,
                overwrite=True,
                content_settings=ContentSettings(content_type='application/pdf')
            )
        except Exception as e:
            print(f"Failed to download/upload PDF: {pdf_url} | Error: {e}")
            return None

    documentType = link_to_document_type.get(current_page_url)
    doc_type = documentType or 'N/A'
    print(documentType, doc_type)
    metadata = {
        'pfd_url': blob_client.url,
        'currentLink': strip_fragment(current_page_url),
        'parentLink': strip_fragment(parent_url) if parent_url else 'N/A',
        'level': str(level),
        'year': year or 'N/A',
        'goal': goal or 'N/A',
        'group': group or 'N/A',
        'documentType': doc_type
    }

    tags = {
        'documentType': doc_type
    }

    # Apply metadata and tags
    blob_client.set_blob_metadata(metadata)
    blob_client.set_blob_tags(tags)

    print(blob_name, metadata, tags)
    return metadata

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

    year_from_url = extract_year_from_url(url)
    page_year = inherited_year or year_from_url

    # Default document type
    document_type = link_to_document_type.get(url, None)

    # Section-style layout
    sections = soup.find_all('div', class_='col-6')

    for section in sections:
        goal_div = section.find_previous('div', class_='top-section-title')
        goal_text = goal_div.get_text(strip=True) if goal_div else 'Unknown'
        current_group = None

        for elem in section.find_all(['p', 'a']):
            if 'sub-section-title' in elem.get('class', []):
                current_group = elem.get_text(strip=True)
            elif elem.name == 'a' and elem.get('href', '').lower().endswith('.pdf'):
                href = elem['href']
                pdf_url = urljoin(url, href)
                link_text = elem.get_text(strip=True)
                final_year = extract_year_from_text(link_text) or extract_year_from_text(href) or page_year

                metadata = process_pdf(
                    pdf_url=pdf_url,
                    current_page_url=url,
                    parent_url=parent_url,
                    level=level,
                    goal=goal_text,
                    group=current_group,
                    year=final_year,
                    documentType=document_type
                )
                if metadata:
                    metadata_list.append(metadata)

    # Fallback: generic layout (e.g., economicpublications)
    if not sections:
        goal_text = 'Unknown'
        current_group = None

        for elem in soup.find_all('a', href=True):
            href = elem['href']
            if href.lower().endswith('.pdf'):
                pdf_url = urljoin(url, href)
                link_text = elem.get_text(strip=True)
                final_year = extract_year_from_text(link_text) or extract_year_from_text(href) or page_year

                # Special case for economicpublications
                doc_type = document_type
                if url == "https://dfm.idaho.gov/publication/economicpublications/":
                    h3 = elem.find_previous('h3')
                    doc_type = h3.get_text(strip=True) if h3 else None

                metadata = process_pdf(
                    pdf_url=pdf_url,
                    current_page_url=url,
                    parent_url=parent_url,
                    level=level,
                    goal=goal_text,
                    group=current_group,
                    year=final_year,
                    documentType=doc_type
                )
                if metadata:
                    metadata_list.append(metadata)

    # Recursively crawl subpages
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
