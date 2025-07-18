import os
from azure.storage.blob import BlobServiceClient, ContentSettings
from pathlib import Path

# === CONFIGURATION ===
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=dfm4512768544;AccountKey=cMRgM2an2ssceMNmlCyqPDL+YH1uqGTqveUG7aHlIiwyjsWWhn1XzCNbgsrTAn7WPrZO5HK54m2q+AStq5GF0w==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "dfm-main-website"
LOCAL_FOLDER = "downloaded_files"

EXTENSION_CONTENT_TYPES = {
    '.pdf': "application/pdf",
    '.docx': "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    '.doc': "application/msword",
    '.xlsx': "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
}

def get_content_type(file_path):
    ext = Path(file_path).suffix.lower()
    return EXTENSION_CONTENT_TYPES.get(ext, "application/octet-stream")


def upload_files_to_azure():
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    for root, dirs, files in os.walk(LOCAL_FOLDER):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, LOCAL_FOLDER).replace("\\", "/")
            blob_path = relative_path  # Maintain directory structure in blob

            content_type = get_content_type(local_file_path)

            print(f"⬆️ Uploading: {blob_path} ({content_type})")

            with open(local_file_path, "rb") as data:
                container_client.upload_blob(
                    name=blob_path,
                    data=data,
                    overwrite=True,
                    content_settings=ContentSettings(content_type=content_type)
                )

    print("✅ Upload complete.")


if __name__ == "__main__":
    upload_files_to_azure()
