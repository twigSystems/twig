import sys
import os

# Adiciona o diretório 'c:/projetos/grnl_platform' ao PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import requests
from requests.auth import HTTPDigestAuth
import json
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
import logging
from tenacity import retry, wait_fixed, stop_after_attempt

# Adicionar o diretório base do projeto ao PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from core.models import PeopleCountingData
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
def initiate_line_people_counting_query(uuid, start_time, end_time, line_id):
    url = f"{BASE_URL}/api/v1/counting/getDatabseRecords"
    payload = {
        "uuid": uuid,
        "event": 0,
        "timeStart": format_time(start_time),
        "timeEnd": format_time(end_time),
        "lineParam": {"line": line_id, "timeUnit": 0, "mode": 0}
    }
    logger.debug(f"Payload for line query: {json.dumps(payload)}")
    response = requests.post(url, headers=HEADERS, auth=HTTPDigestAuth(USERNAME, PASSWORD), data=json.dumps(payload))
    response.raise_for_status()
    return response.json()

@retry(wait=wait_fixed(10), stop=stop_after_attempt(5))
def get_line_people_counting(uuid):
    url = f"{BASE_URL}/api/v1/counting/getRecordsResult"
    payload = {
        "uuid": uuid,
        "event": 0
    }
    logger.debug(f"Payload for get line records result: {json.dumps(payload)}")
    response = requests.post(url, headers=HEADERS, auth=HTTPDigestAuth(USERNAME, PASSWORD), data=json.dumps(payload))
    response.raise_for_status()
    return response.json()

def get_line_counts_for_uuid(uuid, line_id, start_time, end_time):
    logger.info(f"Initiating line people counting query for UUID {uuid} and line ID {line_id}")
    line_response = initiate_line_people_counting_query(uuid, start_time, end_time, line_id)
    time.sleep(3)
    line_data = get_line_people_counting(uuid)

    enter_count = 0
    if line_data and "data" in line_data and "lineRecords" in line_data["data"]:
        records = line_data["data"]["lineRecords"]["records"]
        for record in records:
            record_time = datetime.strptime(record["time"], "%Y-%m-%dT%H:%M:%S.%f")
            if start_time <= record_time <= end_time:
                enter_count = record.get("enter", 0)
                break  # Encontramos o valor para o período, podemos parar

    logger.info(f"Total enter count for UUID {uuid} and line ID {line_id} is {enter_count}")
    return enter_count

def remove_existing_records(session, start_time):
    try:
        session.execute(text("""
            DELETE FROM people_counting_data
            WHERE loja = :loja AND start_time = :start_time
        """), {'loja': LOJA, 'start_time': start_time})

        session.commit()
    except Exception as e:
        logger.error(f"Erro ao remover registros existentes: {str(e)}", exc_info=True)
        session.rollback()

def store_data_in_db(line_data, start_time, end_time):
    session = SessionLocal()
    try:
        # Remover registros existentes antes de adicionar novos
        remove_existing_records(session, start_time)

        # Armazenar dados de PeopleCountingData
        people_counting_entry = PeopleCountingData(
            loja=LOJA,
            ip=IP,
            start_time=start_time,
            end_time=end_time,
            total_in=line_data["total_in"],
            line1_in=line_data["line1_in"],
            line2_in=line_data["line2_in"],
            line3_in=line_data["line3_in"],
            line4_in=line_data["line4_in"]
        )
        session.add(people_counting_entry)

        session.commit()
    except Exception as e:
        logger.error(f"Erro ao armazenar dados no banco de dados: {str(e)}", exc_info=True)
        session.rollback()
    finally:
        session.close()

def coletar_dados_sensor_OML01(start_date_str, end_date_str):
    # Processar datas de entrada para incluir horários se não fornecidos
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        start_date = start_date.replace(hour=0, minute=0, second=0)
    
    try:
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        end_date = end_date.replace(hour=23, minute=59, second=59)
    
    # Garantir que a data final não é no futuro
    now = datetime.now()
    if end_date > now:
        end_date = now
    
    uuids = {
        "line1_in": {"uuid": "0e5b2c16-4d15-4ff4-b5a6-45b688e5d4d9", "line_id": 1},
        "line2_in": {"uuid": "1f4b2c16-4d15-4ff4-b5a6-45b688e5d4d9", "line_id": 2},
        "line3_in": {"uuid": "2e5b2c16-4d15-4ff4-b5a6-45b688e5d4d9", "line_id": 3},
        "line4_in": {"uuid": "3f4b2c16-4d15-4ff4-b5a6-45b688e5d4d9", "line_id": 4}
    }

    current_time = start_date.replace(minute=0, second=0, microsecond=0)
    
    try:
        while current_time < end_date:
            next_time = current_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1) - timedelta(seconds=1)
            if next_time > end_date:
                next_time = end_date
            
            logger.info(f"Processing line data for {current_time.strftime('%Y-%m-%d %H:%M:%S')} to {next_time.strftime('%Y-%m-%d %H:%M:%S')}")
            line_data = {
                "total_in": 0,
                "line1_in": 0,
                "line2_in": 0,
                "line3_in": 0,
                "line4_in": 0
            }

            for line_name, details in uuids.items():
                try:
                    line_total_count = get_line_counts_for_uuid(details["uuid"], details["line_id"], current_time, next_time)
                    line_data[line_name] = line_total_count
                    line_data["total_in"] += line_total_count
                except Exception as e:
                    logger.error(f"Erro ao processar dados de linha para UUID {details['uuid']} ({line_name}): {str(e)}", exc_info=True)

            logger.info(f"Line data: {line_data}")
            store_data_in_db(line_data, current_time, next_time)
            current_time = next_time + timedelta(seconds=1)
    
    except KeyboardInterrupt:
        logger.info("Process interrupted by user.")
    
    logger.info("Process completed successfully.")

if __name__ == "__main__":
    start_date_str = input("Digite a data de início (YYYY-MM-DD HH:MM:SS) ou (YYYY-MM-DD): ")
    end_date_str = input("Digite a data de término (YYYY-MM-DD HH:MM:SS) ou (YYYY-MM-DD): ")
    coletar_dados_sensor_OML01(start_date_str, end_date_str)
