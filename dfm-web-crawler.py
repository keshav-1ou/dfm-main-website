import os
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from azure.storage.blob import BlobServiceClient, ContentSettings

# Azure setup
AZURE_CONNECTION_STRING = "<your-azure-connection-string>"
CONTAINER_NAME = "your-container-name"

blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Base URL
BASE_URL = "https://dfm.idaho.gov/"

def sanitize_text(text):
    return re.sub(r'\s+', ' ', text.strip())

def upload_to_azure(local_path, blob_name, metadata):
    with open(local_path, "rb") as data:
        print(f"⬆️ Uploading {blob_name} with metadata: {metadata}")
        container_client.upload_blob(
            name=blob_name,
            data=data,
            overwrite=True,
            content_settings=ContentSettings(content_type='application/pdf'),
            metadata={k: str(v) for k, v in metadata.items() if v}
        )

def crawl_and_upload_pdfs():
    response = requests.get(BASE_URL)
    soup = BeautifulSoup(response.content, "html.parser")

    for year_section in soup.select("div.accordion-item"):
        year_heading = year_section.select_one(".accordion-button")
        if not year_heading:
            continue
        year = sanitize_text(year_heading.text)
        print(f"\n📅 Year: {year}")

        # Reveal nested HTML
        goal_sections = year_section.select(".accordion-body .accordion-item")
        for goal_sec in goal_sections:
            goal = sanitize_text(goal_sec.select_one(".accordion-button").text)
            print(f"  🎯 Goal: {goal}")

            group_sections = goal_sec.select(".accordion-body ul li")
            for group_li in group_sections:
                link_tag = group_li.find("a", href=True)
                if not link_tag:
                    continue

                group = sanitize_text(link_tag.text)
                file_url = urljoin(BASE_URL, link_tag["href"])
                filename = os.path.basename(link_tag["href"])

                if not filename.lower().endswith(".pdf"):
                    continue

                print(f"    📄 Group: {group} → {filename}")

                # Download the file
                try:
                    pdf_bytes = requests.get(file_url).content
                    local_path = os.path.join("downloads", filename)
                    os.makedirs("downloads", exist_ok=True)
                    with open(local_path, "wb") as f:
                        f.write(pdf_bytes)

                    # Metadata
                    metadata = {
                        "year": year,
                        "goal": goal,
                        "group": group,
                        "filename": filename,
                        "url": file_url,
                        "level": "dfm"
                    }

                    # Upload
                    blob_path = f"dfm_documents/{year}/{filename}"
                    upload_to_azure(local_path, blob_path, metadata)

                except Exception as e:
                    print(f"❌ Failed to process {file_url}: {e}")

if __name__ == "__main__":
    crawl_and_upload_pdfs()
