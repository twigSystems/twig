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
import schedule
from core.models import HeatmapData, RegionalPeopleCountingData, PeopleCountingData
from core.config import DATABASE_URL
from concurrent.futures import ThreadPoolExecutor, as_completed

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
                enter_count += record.get("enter", 0)

    logger.info(f"Total enter count for UUID {uuid} and line ID {line_id} is {enter_count}")
    return enter_count

def remove_existing_records(session, start_time, end_time):
    try:
        session.execute(text("""
            DELETE FROM regional_people_counting_data
            WHERE loja = :loja AND start_time >= :start_time AND start_time < :end_time
        """), {'loja': LOJA, 'start_time': start_time, 'end_time': end_time})
        
        session.execute(text("""
            DELETE FROM heatmap_data
            WHERE loja = :loja AND start_time >= :start_time AND start_time < :end_time
        """), {'loja': LOJA, 'start_time': start_time, 'end_time': end_time})

        session.execute(text("""
            DELETE FROM people_counting_data
            WHERE loja = :loja AND start_time >= :start_time AND start_time < :end_time
        """), {'loja': LOJA, 'start_time': start_time, 'end_time': end_time})

        session.commit()
    except Exception as e:
        logger.error(f"Erro ao remover registros existentes: {str(e)}", exc_info=True)
        session.rollback()

def get_session():
    return SessionLocal()

def store_data_in_db(region_data, people_counting_data, start_time, end_time):
    with get_session() as session:
        try:
            # Remover registros existentes antes de adicionar novos
            remove_existing_records(session, start_time, end_time)

            # Calcular média para heatmap
            total_counts = [data['total_count'] for data in region_data.values()]
            average_heatmap_value = sum(total_counts) / len(total_counts) if total_counts else 0

            # Log dos dados que serão armazenados
            logger.info(f"Storing regional people counting data: {region_data}")
            logger.info(f"Storing heatmap data: {average_heatmap_value}")
            logger.info(f"Storing people counting data: {people_counting_data}")

            # Armazenar dados de RegionalPeopleCountingData
            regional_people_counting_entry = RegionalPeopleCountingData(
                loja=LOJA,
                ip=IP,
                start_time=start_time,
                end_time=end_time,
                region1=region_data.get('Parede direita', {}).get('total_count', 0),
                region2=region_data.get('Mesa central', {}).get('total_count', 0),
                region3=region_data.get('Parede esquerda', {}).get('total_count', 0),
                region4=region_data.get('Balcão', {}).get('total_count', 0),
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

            # Armazenar dados de PeopleCountingData
            people_counting_entry = PeopleCountingData(
                loja=LOJA,
                ip=IP,
                start_time=start_time,
                end_time=end_time,
                total_in=people_counting_data["total_in"],
                line1_in=people_counting_data["line1_in"],
                line2_in=people_counting_data["line2_in"],
                line3_in=people_counting_data["line3_in"],
                line4_in=people_counting_data["line4_in"],
                line4_out=people_counting_data["line4_out"]
             )
            session.add(people_counting_entry)

            session.commit()
        except Exception as e:
            logger.error(f"Erro ao armazenar dados no banco de dados: {str(e)}", exc_info=True)
            session.rollback()

def coletar_dados_sensor_OML01():
    logger.info("Iniciando o processo de coleta de dados")
    now = datetime.now().replace(second=0, microsecond=0)

    if now.minute % 20 == 0:
        if now.minute == 0:
            start_date = (now - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            end_date = now.replace(minute=59, second=59, microsecond=0)
        elif now.minute == 20:
            start_date = now.replace(minute=0, second=0, microsecond=0)
            end_date = now.replace(minute=19, second=59, microsecond=0)
        elif now.minute == 40:
            start_date = now.replace(minute=0, second=0, microsecond=0)
            end_date = now.replace(minute=39, second=59, microsecond=0)
    else:
        start_date = now.replace(minute=0, second=0, microsecond=0)
        end_date = now

    # Definir o período de registro no banco de dados
    db_start_time = start_date.replace(minute=0, second=0, microsecond=0)
    db_end_time = db_start_time + timedelta(hours=1, seconds=-1)

    # Coleta de dados para regional people counting
    uuids = {
        "Parede direita": {"uuid": "f3eaf2d2-8f89-4c26-914b-3b65a6d601c7", "region_id": 1},
        "Mesa central": {"uuid": "31d56e1f-75b6-4ad5-91d8-0a4cb8f5dc3d", "region_id": 2},
        "Parede esquerda": {"uuid": "70e26e9f-0bb7-4ad4-8e49-97c1c7e70363", "region_id": 3},
        "Balcão": {"uuid": "e24d7e3f-2b6a-465f-87b1-b1d3c7b229de", "region_id": 4}
    }

    region_data = {}
    logger.info(f"Coletando dados para {start_date.strftime('%Y-%m-%d %H:%M:%S')} até {end_date.strftime('%Y-%m-%d %H:%M:%S')}")

    for name, details in uuids.items():
        uuid = details["uuid"]
        region_id = details["region_id"]
        logger.info(f"Processando UUID {uuid} para a região {name}")
        try:
            total_count, occupancy_time = get_counts_for_uuid(uuid, region_id, start_date, end_date)
            if name in region_data:
                region_data[name]["total_count"] += total_count
                # Atualizar o tempo de ocupação
                region_data[name]["occupancy_time"] = str(
                    timedelta(seconds=(
                        timedelta(seconds=int(region_data[name]["occupancy_time"].split(':')[-1])).total_seconds() +
                        timedelta(seconds=int(occupancy_time.split(':')[-1])).total_seconds()
                    ))
                )
            else:
                region_data[name] = {
                    "total_count": total_count,
                    "occupancy_time": occupancy_time
                }
        except Exception as e:
            logger.error(f"Erro ao processar dados para UUID {uuid} e região {name}: {str(e)}", exc_info=True)

    # Coleta de dados de people counting
    people_counting_data = collect_line_data(start_date, end_date)
    logger.info(f"People counting data collected: {people_counting_data}")

    # Armazenar dados no banco de dados
    store_data_in_db(region_data, people_counting_data, db_start_time, db_end_time)
    logger.info("Processo concluído com sucesso.")
    proxima_atualizacao = calcular_proxima_atualizacao()
    logger.info("Próxima atualização programada para: %s", proxima_atualizacao.strftime('%Y-%m-%d %H:%M:%S'))

def collect_line_data(start_time, end_time):
    uuids = {
        "line1_in": {"uuid": "0e5b2c16-4d15-4ff4-b5a6-45b688e5d4d9", "line_id": 1},
        "line2_in": {"uuid": "1f4b2c16-4d15-4ff4-b5a6-45b688e5d4d9", "line_id": 2},
        "line3_in": {"uuid": "2e5b2c16-4d15-4ff4-b5a6-45b688e5d4d9", "line_id": 3},
        "line4_in": {"uuid": "3f4b2c16-4d15-4ff4-b5a6-45b688e5d4d9", "line_id": 4},
        "line4_out": {"uuid": "3f4b2c16-4d15-4ff4-b5a6-45b688e5d4d9", "line_id": 4}
    }

    line_data = {
        "total_in": 0,
        "line1_in": 0,
        "line2_in": 0,
        "line3_in": 0,
        "line4_in": 0,
        "line4_out": 0
    }

    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(get_line_counts_for_uuid, details["uuid"], details["line_id"], start_time, end_time): line_name
            for line_name, details in uuids.items()
        }

        for future in as_completed(futures):
            line_name = futures[future]
            try:
                line_total_count = future.result()
                line_data[line_name] = line_total_count
                line_data["total_in"] += line_total_count
            except Exception as e:
                logger.error("Erro ao tentar coletar e armazenar dados: %s", e, exc_info=True)

    logger.info(f"Line data: {line_data}")
    return line_data

def calcular_proxima_atualizacao():
    now = datetime.now().replace(second=0, microsecond=0)
    if now.minute < 20:
        return now.replace(minute=20)
    elif now.minute < 40:
        return now.replace(minute=40)
    else:
        return (now + timedelta(hours=1)).replace(minute=0)

def schedule_jobs():
    schedule.every().hour.at(":00").do(coletar_dados_sensor_OML01)
    schedule.every().hour.at(":20").do(coletar_dados_sensor_OML01)
    schedule.every().hour.at(":40").do(coletar_dados_sensor_OML01)

def main():
    logger.info("Script iniciado")
    coletar_dados_sensor_OML01()  # Executar imediatamente ao iniciar
    schedule_jobs()
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
