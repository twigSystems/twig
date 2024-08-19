import sys
import os

# Adicionar o diretório 'core' e 'config' ao PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import requests
from requests.auth import HTTPDigestAuth
import json
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
import logging
from tenacity import retry, wait_fixed, stop_after_attempt
from core.models import HeatmapData, RegionalPeopleCountingData
from core.config import DATABASE_URL

# Configuração do logger
logging.basicConfig(level=logging.INFO, handlers=[
    logging.FileHandler("sensor_data_collection.log"),
    logging.StreamHandler(sys.stdout)
])
logger = logging.getLogger(__name__)

# Configuração do SQLAlchemy
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

# Configurações do sensor
BASE_URL = "http://93.108.96.96:21001"
USERNAME = "admin"
PASSWORD = "grnl.2024"

HEADERS = {
    "Content-Type": "application/json"
}

LOJA = "OML01-Omnia GuimarãesShopping"
IP = "93.108.96.96:21001"

def format_time(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000")

@retry(wait=wait_fixed(10), stop=stop_after_attempt(5))
def initiate_regional_people_counting_query(uuid, start_time, end_time, region_id):
    url = f"{BASE_URL}/api/v1/counting/getDatabseRecords"
    payload = {
        "uuid": uuid,
        "event": 1,
        "timeStart": format_time(start_time),
        "timeEnd": format_time(end_time),
        "regionCounting": {"regionID": region_id, "mode": 0}
    }
    logger.debug(f"Payload for regional query: {json.dumps(payload)}")
    response = requests.post(url, headers=HEADERS, auth=HTTPDigestAuth(USERNAME, PASSWORD), data=json.dumps(payload))
    response.raise_for_status()
    return response.json()

@retry(wait=wait_fixed(10), stop=stop_after_attempt(5))
def get_people_counting(uuid, event_type):
    url = f"{BASE_URL}/api/v1/counting/getRecordsResult"
    payload = {
        "uuid": uuid,
        "event": event_type
    }
    logger.debug(f"Payload for get records result: {json.dumps(payload)}")
    response = requests.post(url, headers=HEADERS, auth=HTTPDigestAuth(USERNAME, PASSWORD), data=json.dumps(payload))
    response.raise_for_status()
    return response.json()

def calculate_occupancy_time(records):
    total_occupancy_time = timedelta()
    entry_time = None
    
    for record in records:
        time = datetime.strptime(record["time"], "%Y-%m-%dT%H:%M:%S.%f")
        if record["total"] > 0:
            if entry_time is None:
                entry_time = time
        else:
            if entry_time is not None:
                total_occupancy_time += time - entry_time
                entry_time = None
    
    if entry_time is not None:
        total_occupancy_time += datetime.strptime(records[-1]["time"], "%Y-%m-%dT%H:%M:%S.%f") - entry_time

    return total_occupancy_time

def get_counts_for_uuid(uuid, region_id, start_time, end_time):
    logger.info(f"Initiating regional people counting query for UUID {uuid}")
    regional_response = initiate_regional_people_counting_query(uuid, start_time, end_time, region_id)
    time.sleep(3)
    regional_data = get_people_counting(uuid, 1)

    total_count = 0
    occupancy_time = timedelta()
    if regional_data and "data" in regional_data and "regionRecords" in regional_data["data"]:
        records = regional_data["data"]["regionRecords"]["records"]
        total_count = regional_data["data"]["regionRecords"].get("totalCount", 0)
        occupancy_time = calculate_occupancy_time(records)

    return total_count, str(occupancy_time)

def remove_existing_records(session, start_time):
    try:
        session.execute(text("""
            DELETE FROM regional_people_counting_data
            WHERE loja = :loja AND start_time = :start_time
        """), {'loja': LOJA, 'start_time': start_time})
        
        session.execute(text("""
            DELETE FROM heatmap_data
            WHERE loja = :loja AND start_time = :start_time
        """), {'loja': LOJA, 'start_time': start_time})

        session.commit()
    except Exception as e:
        logger.error(f"Erro ao remover registros existentes: {str(e)}", exc_info=True)
        session.rollback()

def get_session():
    return SessionLocal()

def store_data_in_db(region_data, start_time, end_time):
    with get_session() as session:
        try:
            # Remover registros existentes antes de adicionar novos
            remove_existing_records(session, start_time)

            # Calcular média para heatmap
            total_counts = [data['total_count'] for data in region_data.values()]
            average_heatmap_value = sum(total_counts) / len(total_counts)

            # Armazenar dados de RegionalPeopleCountingData
            regional_people_counting_entry = RegionalPeopleCountingData(
                loja=LOJA,
                ip=IP,
                start_time=start_time,
                end_time=end_time,
                region1=region_data['Parede direita']['total_count'],
                region2=region_data['Mesa central']['total_count'],
                region3=region_data['Parede esquerda']['total_count'],
                region4=region_data['Balcão']['total_count'],
                total=sum(total_counts)
            )
            session.add(regional_people_counting_entry)

            # Armazenar dados de HeatmapData (média)
            heatmap_entry = HeatmapData(
                loja=LOJA,
                ip=IP,
                start_time=start_time,
                end_time=end_time,
                value=average_heatmap_value
            )
            session.add(heatmap_entry)

            session.commit()
        except Exception as e:
            logger.error(f"Erro ao armazenar dados no banco de dados: {str(e)}", exc_info=True)
            session.rollback()

def coletar_dados_sensor_OML01(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d %H:%M:%S")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d %H:%M:%S")
    
    uuids = {
        "Parede direita": {"uuid": "f3eaf2d2-8f89-4c26-914b-3b65a6d601c7", "region_id": 1},
        "Mesa central": {"uuid": "31d56e1f-75b6-4ad5-91d8-0a4cb8f5dc3d", "region_id": 2},
        "Parede esquerda": {"uuid": "70e26e9f-0bb7-4ad4-8e49-97c1c7e70363", "region_id": 3},
        "Balcão": {"uuid": "e24d7e3f-2b6a-465f-87b1-b1d3c7b229de", "region_id": 4}
    }

    current_time = start_date.replace(minute=0, second=0, microsecond=0)
    while current_time < end_date:
        next_time = current_time + timedelta(minutes=59, seconds=59)
        region_data = {}
        logger.info(f"Processing data for {current_time.strftime('%Y-%m-%d %H:%M:%S')} to {next_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        for name, details in uuids.items():
            uuid = details["uuid"]
            region_id = details["region_id"]
            logger.info(f"Processing UUID {uuid} for region {name}")
            try:
                total_count, occupancy_time = get_counts_for_uuid(uuid, region_id, current_time, next_time)
                region_data[name] = {
                    "total_count": total_count,
                    "occupancy_time": occupancy_time
                }
            except Exception as e:
                logger.error(f"Erro ao processar dados para UUID {uuid} e região {name}: {str(e)}", exc_info=True)
        
        store_data_in_db(region_data, current_time, next_time)
        current_time = next_time + timedelta(seconds=1)

    # Processar o período final incompleto, se houver
    if current_time < end_date:
        region_data = {}
        next_time = end_date
        logger.info(f"Processing data for {current_time.strftime('%Y-%m-%d %H:%M:%S')} to {next_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        for name, details in uuids.items():
            uuid = details["uuid"]
            region_id = details["region_id"]
            logger.info(f"Processing UUID {uuid} for region {name}")
            try:
                total_count, occupancy_time = get_counts_for_uuid(uuid, region_id, current_time, next_time)
                region_data[name] = {
                    "total_count": total_count,
                    "occupancy_time": occupancy_time
                }
            except Exception as e:
                logger.error(f"Erro ao processar dados para UUID {uuid} e região {name}: {str(e)}", exc_info=True)
        
        store_data_in_db(region_data, current_time, next_time)

    logger.info("Process completed successfully.")

if __name__ == "__main__":
    start_date_str = input("Digite a data de início (YYYY-MM-DD HH:MM:SS): ")
    end_date_str = input("Digite a data de término (YYYY-MM-DD HH:MM:SS): ")
    coletar_dados_sensor_OML01(start_date_str, end_date_str)
