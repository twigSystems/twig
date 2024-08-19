import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from io import StringIO
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

def fetch_data(url: str, report_type: int):
    try:
        full_url = f"{url}&report_type={report_type}&linetype=31&statistics_type=2"
        logging.info(f"Fetching data from URL: {full_url}")
        response = requests.get(full_url, auth=HTTPBasicAuth(username, password))
        response.raise_for_status()
        data = StringIO(response.text)
        df = pd.read_csv(data)
        df.columns = df.columns.str.strip()
        relevant_sum = df[['Line1 - In', 'Line2 - In', 'Line3 - In']].sum(axis=1).sum()
        corridor_count = df['Line4 - In'].sum()
        return relevant_sum, corridor_count
    except requests.RequestException as e:
        logging.error(f"Error fetching data: {e}")
        raise Exception(f"Error fetching data: {e}")

def get_people_counting(loja: str, start_time: str, report_type: int):
    try:
        urls = stores[loja]
        relevant_sum_total = 0
        corridor_count_total = 0
        for url in urls:
            try:
                relevant_sum, corridor_count = fetch_data(f"{url}dataloader.cgi?dw=vcalogcsv&time_start={start_time}", report_type)
                relevant_sum_total += relevant_sum
                corridor_count_total += corridor_count
            except requests.RequestException as e:
                logging.error(f"Error fetching data from {url}: {e}")
                raise Exception(f"Error fetching data from {url}: {e}")
        return relevant_sum_total, corridor_count_total
    except Exception as e:
        logging.error(f"Error in get_people_counting: {e}")
        raise Exception(f"Error in get_people_counting: {e}")