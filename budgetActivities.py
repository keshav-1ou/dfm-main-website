import os
from azure.storage.blob import BlobServiceClient

# === CONFIGURATION ===
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=dfm4512768544;AccountKey=cMRgM2an2ssceMNmlCyqPDL+YH1uqGTqveUG7aHlIiwyjsWWhn1XzCNbgsrTAn7WPrZO5HK54m2q+AStq5GF0w==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "dfm-main-website"
FOLDER_PATH_PREFIX = "federal-funds"  # 👈 Set your Azure folder path here
DOCUMENT_TYPE = "Budget Activities Summary"

# === Initialize Blob Service ===
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

def update_metadata_and_tags(blob_name):
    try:
        blob_client = container_client.get_blob_client(blob_name)

        # Update metadata
        metadata = blob_client.get_blob_properties().metadata or {}
        metadata["documentType"] = DOCUMENT_TYPE
        blob_client.set_blob_metadata(metadata)

        # Update tags
        blob_client.set_blob_tags({"index": blob_name})

        print(f"✅ Updated: {blob_name}")
    except Exception as e:
        print(f"❌ Failed to update {blob_name}: {e}")

def main():
    print(f"🚀 Updating blobs in: {FOLDER_PATH_PREFIX}")
    updated_count = 0

    blobs = container_client.list_blobs(name_starts_with=FOLDER_PATH_PREFIX)
    for blob in blobs:
        update_metadata_and_tags(blob.name)
        updated_count += 1

    print(f"\n🎉 Done. Total blobs updated: {updated_count}")

if __name__ == "__main__":
    main()
