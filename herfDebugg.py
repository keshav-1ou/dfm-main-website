
from bs4 import BeautifulSoup
import requests


url = "https://dfm.idaho.gov/publication/?type=budget&level=summary"
response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")
print(f"URL: {url}")
print("🔍 Preview HTML snippet:")
print(soup.prettify()[:1000])
