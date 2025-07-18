import os
from azure.storage.blob import BlobServiceClient
from urllib.parse import unquote

# === CONFIGURATION ===
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=dfm4512768544;AccountKey=cMRgM2an2ssceMNmlCyqPDL+YH1uqGTqveUG7aHlIiwyjsWWhn1XzCNbgsrTAn7WPrZO5HK54m2q+AStq5GF0w==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "dfm-main-website-docs"
LOCAL_DOWNLOAD_DIR = "downloaded_pdfs"  # change as needed

# === INITIALIZE CLIENT ===
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

def download_all_pdfs():
    print(f"📥 Downloading PDFs from container '{CONTAINER_NAME}'...")

    blobs = container_client.list_blobs()
    total = 0

    for blob in blobs:
        blob_name = unquote(blob.name)

        if not blob_name.lower().endswith(".pdf"):
            continue

        local_path = os.path.join(LOCAL_DOWNLOAD_DIR, blob_name.replace("/", os.sep))
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        print(f"🔽 {blob_name} → {local_path}")

        with open(local_path, "wb") as file:
            download_stream = container_client.download_blob(blob.name)
            file.write(download_stream.readall())

        total += 1

    print(f"\n✅ Download complete: {total} PDFs downloaded.")

if __name__ == "__main__":
    download_all_pdfs()
