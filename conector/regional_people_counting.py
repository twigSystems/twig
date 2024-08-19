import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import logging
from io import StringIO

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

def fetch_regional_data(url: str, report_type: int):
    try:
        full_url = f"{url}&report_type={report_type}&lengthtype=0&length=0&region1=1&region2=1&region3=1&region4=1"
        logging.info(f"Fetching data from URL: {full_url}")
        response = requests.get(full_url, auth=HTTPBasicAuth(username, password))
        response.raise_for_status()
        data = StringIO(response.text)
        df = pd.read_csv(data)
        df.columns = df.columns.str.strip()
        return df
    except requests.RequestException as e:
        logging.error(f"Error fetching data: {e}")
        raise Exception(f"Error fetching data: {e}")

def get_regional_people_counting(loja: str, start_time: str, report_type: int):
    try:
        urls = stores[loja]
        combined_df = pd.DataFrame()
        region_offset = 0
        for url in urls:
            try:
                df = fetch_regional_data(f"{url}dataloader.cgi?dw=regionalcountlogcsv&time_start={start_time}", report_type)
                if not df.empty:
                    df = df.rename(columns={
                        'region1': f'Região {1 + region_offset}',
                        'region2': f'Região {2 + region_offset}',
                        'region3': f'Região {3 + region_offset}',
                        'region4': f'Região {4 + region_offset}'
                    })
                    combined_df = pd.concat([combined_df, df], ignore_index=True)
                    region_offset += 4
            except requests.RequestException as e:
                logging.error(f"Error fetching data from {url}: {e}")
                raise Exception(f"Error fetching data from {url}: {e}")

        if 'Sum' in combined_df.columns:
            for col in combined_df.columns:
                if col.startswith('Região'):
                    combined_df[col] = combined_df[col] / combined_df['Sum'] * 100
        return combined_df
    except Exception as e:
        logging.error(f"Error in get_regional_people_counting: {e}")
        raise Exception(f"Error in get_regional_people_counting: {e}")