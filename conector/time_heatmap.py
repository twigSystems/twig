import requests
from requests.auth import HTTPBasicAuth
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

stores = {
    "OML01-Omnia GuimarãesShopping": ["http://93.108.96.96:21001/"],
    "ONL01-Only UBBO Amadora": ["http://93.108.245.76:21002/", "http://93.108.245.76:21003/"],
    "OML02-Omnia Fórum Almada": ["http://188.37.190.134:2201/"],
    "OML03-Omnia Norteshopping": ["http://188.37.124.33:21002/"]
}

username = "admin"
password = "grnl.2024"

def fetch_heatmap_data(url: str):
    try:
        logging.info(f"Fetching heatmap data from URL: {url}")
        response = requests.get(url, auth=HTTPBasicAuth(username, password))
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Error fetching heatmap data: {e}")
        raise Exception(f"Error fetching heatmap data: {e}")

def get_heatmap_permanence(loja: str, start_time: str):
    try:
        urls = stores[loja]
        total_permanence = 0
        for url in urls:
            try:
                data = fetch_heatmap_data(f"{url}vb.htm?page=timeheatmapreport&sub_type=0&time_start={start_time}")
                # Calculate permanence in minutes and sum them up
                permanence = sum(data['data']) / 60
                total_permanence += permanence
            except requests.RequestException as e:
                logging.error(f"Error fetching data from {url}: {e}")
                raise Exception(f"Error fetching data from {url}: {e}")
        return total_permanence
    except Exception as e:
        logging.error(f"Error in get_heatmap_permanence: {e}")
        raise Exception(f"Error in get_heatmap_permanence: {e}")