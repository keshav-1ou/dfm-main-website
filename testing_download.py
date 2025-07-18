from azure.storage.blob import BlobServiceClient

AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=dfm4512768544;AccountKey=cMRgM2an2ssceMNmlCyqPDL+YH1uqGTqveUG7aHlIiwyjsWWhn1XzCNbgsrTAn7WPrZO5HK54m2q+AStq5GF0w==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "dfm-main-website-docs"

blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

print("📦 Listing blobs:")
for blob in container_client.list_blobs():
    print(blob.name)
