import sys
import os
from datetime import datetime, timedelta
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from tenacity import retry, wait_fixed, stop_after_attempt
import pandas as pd
from io import StringIO
import requests
from requests.auth import HTTPDigestAuth
import json
import time

# Adicionar o diretório 'core' e 'config' ao PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from core.models import HeatmapData, RegionalPeopleCountingData, PeopleCountingData, SaleData
from conector.autenticar import autenticar
from conector.consultar import consultar_vendas
from core.config import DATABASE_URL
from core.models import armazenar_dados_no_banco

# Configuração do logger
logging.basicConfig(level=logging.INFO, handlers=[
    logging.FileHandler("data_collector.log"),
    logging.StreamHandler(sys.stdout)
])
logger = logging.getLogger(__name__)

# Configuração do SQLAlchemy
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

# Configuração do sensor
BASE_URL = "http://93.108.96.96:21001"
USERNAME = "admin"
PASSWORD = "grnl.2024"
HEADERS = {"Content-Type": "application/json"}
LOJA = "OML01-Omnia GuimarãesShopping"
IP = "93.108.96.96:21001"

# URLs das lojas
stores = {
    "OML01-Omnia GuimarãesShopping": [],
    "ONL01-Only UBBO Amadora": ["93.108.245.76:21002", "93.108.245.76:21003"],
    "OML02-Omnia Fórum Almada": ["188.37.190.134:2201"],
    "OML03-Omnia Norteshopping": ["188.37.124.33:21002"]
}

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
    response = requests.post(url, headers=HEADERS, auth=HTTPDigestAuth(USERNAME, PASSWORD), data=json.dumps(payload))
    response.raise_for_status()
    return response.json()

@retry(wait=wait_fixed(10), stop=stop_after_attempt(5))
def get_people_counting(uuid, event_type):
    url = f"{BASE_URL}/api/v1/counting/getRecordsResult"
    payload = {"uuid": uuid, "event": event_type}
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
    response = requests.post(url, headers=HEADERS, auth=HTTPDigestAuth(USERNAME, PASSWORD), data=json.dumps(payload))
    response.raise_for_status()
    return response.json()

@retry(wait=wait_fixed(10), stop=stop_after_attempt(5))
def get_line_people_counting(uuid):
    url = f"{BASE_URL}/api/v1/counting/getRecordsResult"
    payload = {"uuid": uuid, "event": 0}
    response = requests.post(url, headers=HEADERS, auth=HTTPDigestAuth(USERNAME, PASSWORD), data=json.dumps(payload))
    response.raise_for_status()
    return response.json()

def get_line_counts_for_uuid(uuid, line_id, start_time, end_time):
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
                break
    return enter_count

def remove_existing_records(session, start_time, end_time):
    try:
        session.execute(text("""
            DELETE FROM regional_people_counting_data
            WHERE loja = :loja AND start_time = :start_time AND end_time = :end_time
        """), {'loja': LOJA, 'start_time': start_time, 'end_time': end_time})
        session.execute(text("""
            DELETE FROM heatmap_data
            WHERE loja = :loja AND start_time = :start_time AND end_time = :end_time
        """), {'loja': LOJA, 'start_time': start_time, 'end_time': end_time})
        session.execute(text("""
            DELETE FROM people_counting_data
            WHERE loja = :loja AND start_time = :start_time AND end_time = :end_time
        """), {'loja': LOJA, 'start_time': start_time, 'end_time': end_time})
        session.commit()
    except Exception as e:
        logger.error(f"Erro ao remover registros existentes: {str(e)}", exc_info=True)
        session.rollback()

def remove_duplicate_records(session, start_time, end_time):
    try:
        session.execute(text("""
            WITH duplicates AS (
                SELECT id, ROW_NUMBER() OVER (PARTITION BY loja, start_time, end_time ORDER BY id) AS rnum
                FROM regional_people_counting_data
                WHERE loja = :loja AND start_time >= :start_time AND end_time <= :end_time
            )
            DELETE FROM regional_people_counting_data WHERE id IN (SELECT id FROM duplicates WHERE rnum > 1)
        """), {'loja': LOJA, 'start_time': start_time, 'end_time': end_time})
        session.execute(text("""
            WITH duplicates AS (
                SELECT id, ROW_NUMBER() OVER (PARTITION BY loja, start_time, end_time ORDER BY id) AS rnum
                FROM heatmap_data
                WHERE loja = :loja AND start_time >= :start_time AND end_time <= :end_time
            )
            DELETE FROM heatmap_data WHERE id IN (SELECT id FROM duplicates WHERE rnum > 1)
        """), {'loja': LOJA, 'start_time': start_time, 'end_time': end_time})
        session.execute(text("""
            WITH duplicates AS (
                SELECT id, ROW_NUMBER() OVER (PARTITION BY loja, start_time, end_time ORDER BY id) AS rnum
                FROM people_counting_data
                WHERE loja = :loja AND start_time >= :start_time AND end_time <= :end_time
            )
            DELETE FROM people_counting_data WHERE id IN (SELECT id FROM duplicates WHERE rnum > 1)
        """), {'loja': LOJA, 'start_time': start_time, 'end_time': end_time})
        session.commit()
    except Exception as e:
        logger.error(f"Erro ao remover registros duplicados: {str(e)}", exc_info=True)
        session.rollback()

def remove_future_records(session):
    try:
        now = datetime.now()
        session.execute(text("""
            DELETE FROM regional_people_counting_data
            WHERE start_time > :now
        """), {'now': now})
        session.execute(text("""
            DELETE FROM heatmap_data
            WHERE start_time > :now
        """), {'now': now})
        session.execute(text("""
            DELETE FROM people_counting_data
            WHERE start_time > :now
        """), {'now': now})
        session.commit()
    except Exception as e:
        logger.error(f"Erro ao remover registros futuros: {str(e)}", exc_info=True)
        session.rollback()

def get_session():
    return SessionLocal()

def store_data_in_db(region_data, line_data, start_time, end_time):
    with get_session() as session:
        try:
            remove_existing_records(session, start_time, end_time)
            total_counts = [data['total_count'] for data in region_data.values()]
            average_heatmap_value = sum(total_counts) / len(total_counts)
            regional_people_counting_entry = RegionalPeopleCountingData(
                loja=LOJA, ip=IP, start_time=start_time, end_time=end_time,
                region1=region_data['Parede direita']['total_count'],
                region2=region_data['Mesa central']['total_count'],
                region3=region_data['Parede esquerda']['total_count'],
                region4=region_data['Balcão']['total_count'],
                total=sum(total_counts)
            )
            session.add(regional_people_counting_entry)
            heatmap_entry = HeatmapData(
                loja=LOJA, ip=IP, start_time=start_time, end_time=end_time,
                value=average_heatmap_value
            )
            session.add(heatmap_entry)
            people_counting_entry = PeopleCountingData(
                loja=LOJA, ip=IP, start_time=start_time, end_time=end_time,
                total_in=line_data["total_in"],
                line1_in=line_data["line1_in"],
                line2_in=line_data["line2_in"],
                line3_in=line_data["line3_in"],
                line4_in=line_data["line4_in"],
                line4_out=line_data["line4_out"]
            )
            session.add(people_counting_entry)
            session.commit()
        except Exception as e:
            logger.error(f"Erro ao armazenar dados no banco de dados: {str(e)}", exc_info=True)
            session.rollback()

def coletar_dados_sensor_OML01(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d %H:%M:%S")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d %H:%M:%S")
    
    # Verifica se o end_date não é no futuro
    if end_date > datetime.now():
        logger.error("A data de término não pode ser posterior ao momento atual.")
        return

    uuids_regional = {
        "Parede direita": {"uuid": "f3eaf2d2-8f89-4c26-914b-3b65a6d601c7", "region_id": 1},
        "Mesa central": {"uuid": "31d56e1f-75b6-4ad5-91d8-0a4cb8f5dc3d", "region_id": 2},
        "Parede esquerda": {"uuid": "70e26e9f-0bb7-4ad4-8e49-97c1c7e70363", "region_id": 3},
        "Balcão": {"uuid": "e24d7e3f-2b6a-465f-87b1-b1d3c7b229de", "region_id": 4}
    }

    uuids_line = {
        "line1_in": {"uuid": "0e5b2c16-4d15-4ff4-b5a6-45b688e5d4d9", "line_id": 1},
        "line2_in": {"uuid": "1f4b2c16-4d15-4ff4-b5a6-45b688e5d4d9", "line_id": 2},
        "line3_in": {"uuid": "2e5b2c16-4d15-4ff4-b5a6-45b688e5d4d9", "line_id": 3},
        "line4_in": {"uuid": "3f4b2c16-4d15-4ff4-b5a6-45b688e5d4d9", "line_id": 4}
    }

    current_time = start_date.replace(minute=0, second=0, microsecond=0)
    while current_time < end_date:
        next_time = current_time + timedelta(minutes=59, seconds=59)
        region_data = {}
        line_data = {
            "total_in": 0,
            "line1_in": 0,
            "line2_in": 0,
            "line3_in": 0,
            "line4_in": 0,
            "line4_out": 0
        }
        for name, details in uuids_regional.items():
            uuid = details["uuid"]
            region_id = details["region_id"]
            try:
                total_count, occupancy_time = get_counts_for_uuid(uuid, region_id, current_time, next_time)
                region_data[name] = {"total_count": total_count, "occupancy_time": occupancy_time}
            except Exception as e:
                logger.error(f"Erro ao processar dados para UUID {uuid} e região {name}: {str(e)}", exc_info=True)

        for line_name, details in uuids_line.items():
            try:
                line_total_count = get_line_counts_for_uuid(details["uuid"], details["line_id"], current_time, next_time)
                line_data[line_name] = line_total_count
                line_data["total_in"] += line_total_count
            except Exception as e:
                logger.error(f"Erro ao processar dados de linha para UUID {details['uuid']} ({line_name}): {str(e)}", exc_info=True)
        
        store_data_in_db(region_data, line_data, current_time, next_time)
        current_time = next_time + timedelta(seconds=1)

    if current_time < end_date:
        region_data = {}
        line_data = {"total_in": 0, "line1_in": 0, "line2_in": 0, "line3_in": 0, "line4_in": 0}
        next_time = end_date
        for name, details in uuids_regional.items():
            uuid = details["uuid"]
            region_id = details["region_id"]
            try:
                total_count, occupancy_time = get_counts_for_uuid(uuid, region_id, current_time, next_time)
                region_data[name] = {"total_count": total_count, "occupancy_time": occupancy_time}
            except Exception as e:
                logger.error(f"Erro ao processar dados para UUID {uuid} e região {name}: {str(e)}", exc_info=True)

        for line_name, details in uuids_line.items():
            try:
                line_total_count = get_line_counts_for_uuid(details["uuid"], details["line_id"], current_time, next_time)
                line_data[line_name] = line_total_count
                line_data["total_in"] += line_total_count
            except Exception as e:
                logger.error(f"Erro ao processar dados de linha para UUID {details['uuid']} ({line_name}): {str(e)}", exc_info=True)

        store_data_in_db(region_data, line_data, current_time, next_time)

    with get_session() as session:
        remove_duplicate_records(session, start_date, end_date)
        remove_future_records(session)

    logger.info("Process completed successfully.")

def parse_date(date_str):
    formats = ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%d', '%Y%m%d']
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Data no formato incorreto: {date_str}")

def coletar_dados_vendas(jwt_token, data_inicio, data_fim, loja):
    consulta_data = consultar_vendas_com_retry(jwt_token, data_inicio.strftime('%Y-%m-%d'), loja)
    logger.info(f"Resposta da consulta de vendas para a data {data_inicio} e loja {loja}: {consulta_data}")
    dados = []
    if consulta_data.get('Sucesso'):
        result_sets = consulta_data['Objecto']['ResultSets']
        if result_sets:
            first_result_set = result_sets[0]
            if first_result_set:
                dados.extend(first_result_set)
    
    dados_filtrados = []
    for d in dados:
        try:
            data_transacao = parse_date(d['Data'])
            if data_inicio <= data_transacao <= data_fim:
                dados_filtrados.append(d)
        except ValueError as e:
            logger.error(f"Data no formato incorreto: {d['Data']}. Erro: {str(e)}")
    
    return [dict(t) for t in {tuple(d.items()) for d in dados_filtrados}]

def get_jwt_token():
    return autenticar()

@retry(wait=wait_fixed(10), stop=stop_after_attempt(5))
def consultar_vendas_com_retry(jwt_token, data, loja):
    return consultar_vendas(jwt_token, data, loja)

@retry(wait=wait_fixed(10), stop=stop_after_attempt(5))
def fetch_and_store(url, parse_function, model, loja, ip):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = parse_function(response.text, loja, ip)
        data_dicts = [{k: v for k, v in d.__dict__.items() if k != '_sa_instance_state'} for d in data]
        if data_dicts:
            armazenar_dados_no_banco(data_dicts, model)
            logger.info(f"Dados armazenados no banco de dados com sucesso para a URL: {url}")
        else:
            logger.warning(f"Nenhum dado processado para a URL: {url}")
    except Exception as e:
        logger.error(f"Erro ao processar os dados da URL: {url} - {str(e)}", exc_info=True)
        raise

def parse_people_counting_data(text, loja, ip):
    try:
        df = pd.read_csv(StringIO(text))
        df.columns = [col.strip() for col in df.columns]
        data = [
            PeopleCountingData(
                loja=loja,
                ip=ip,
                start_time=datetime.strptime(row['StartTime'], '%Y/%m/%d %H:%M:%S'),
                end_time=datetime.strptime(row['EndTime'], '%Y/%m/%d %H:%M:%S'),
                total_in=int(row['Line1 - In']) + int(row['Line2 - In']) + int(row['Line3 - In']),
                line1_in=int(row['Line1 - In']),
                line2_in=int(row['Line2 - In']),
                line3_in=int(row['Line3 - In']),
                line4_in=int(row['Line4 - In']),
                line4_out=int(row['Line4 - Out'])
            )
            for index, row in df.iterrows()
        ]
        return data
    except Exception as e:
        logger.error(f"Erro ao analisar os dados de contagem de pessoas: {str(e)}", exc_info=True)
        return []

def parse_heatmap_data(text, loja, ip):
    try:
        df = pd.read_csv(StringIO(text))
        df.columns = [col.strip() for col in df.columns]
        data = [
            HeatmapData(
                loja=loja,
                ip=ip,
                start_time=datetime.strptime(row['StartTime'], '%Y-%m-%d %H:%M:%S'),
                end_time=datetime.strptime(row['EndTime'], '%Y-%m-%d %H:%M:%S'),
                value=int(row['Value(s)'])
            )
            for index, row in df.iterrows()
        ]
        return data
    except Exception as e:
        logger.error(f"Erro ao analisar os dados de heatmap: {str(e)}", exc_info=True)
        return []

def parse_regional_people_counting_data(text, loja, ip):
    try:
        df = pd.read_csv(StringIO(text))
        df.columns = [col.strip() for col in df.columns]
        data = [
            RegionalPeopleCountingData(
                loja=loja,
                ip=ip,
                start_time=datetime.strptime(row['StartTime'], '%Y/%m/%d %H:%M:%S'),
                end_time=datetime.strptime(row['EndTime'], '%Y/%m/%d %H:%M:%S'),
                region1=int(row['region1']),
                region2=int(row['region2']),
                region3=int(row['region3']),
                region4=int(row['region4']),
                total=int(row['Sum'])
            )
            for index, row in df.iterrows()
        ]
        return data
    except Exception as e:
        logger.error(f"Erro ao analisar os dados de contagem regional de pessoas: {str(e)}", exc_info=True)
        return []

def generate_urls(date, base_url, data_type):
    start_time = datetime(date.year, date.month, date.day, 0, 0, 0)
    end_time = datetime(date.year, date.month, date.day, 23, 59, 59)
    url = f"http://admin:grnl.2024@{base_url}/dataloader.cgi?dw={data_type}&time_start={start_time.strftime('%Y-%m-%d-%H:%M:%S')}&time_end={end_time.strftime('%Y-%m-%d-%H:%M:%S')}"
    return [url]

def process_data_for_store(loja, ips, date, data_type, parse_function, model):
    for ip in ips:
        base_url = f"{ip}"
        urls = generate_urls(date, base_url, data_type)
        for url in urls:
            try:
                fetch_and_store(url, parse_function, model, loja, ip)
            except Exception as e:
                logger.error(f"Erro ao tentar obter e armazenar dados para {loja} no IP {ip}: {str(e)}")

def collect_data_for_day(date):
    logger.info(f"Iniciando coleta de dados para o dia {date.strftime('%Y-%m-%d')}...")
    
    # Coletando dados de vendas
    logger.info("Coletando dados de vendas...")
    jwt_token = get_jwt_token()
    for loja in stores.keys():
        dados_vendas = coletar_dados_vendas(jwt_token, date, date, loja)
        if dados_vendas:
            armazenar_dados_no_banco(dados_vendas, SaleData)
            logger.info(f"Dados de vendas armazenados com sucesso para {loja} no dia {date.strftime('%Y-%m-%d')}")
        else:
            logger.warning(f"Sem dados de vendas para {loja} no dia {date.strftime('%Y-%m-%d')}")
    
    # Coletando dados de contagem de pessoas
    logger.info("Coletando dados de contagem de pessoas...")
    for loja, ips in stores.items():
        process_data_for_store(loja, ips, date, "vcalogcsv&report_type=0&linetype=31&statistics_type=3", parse_people_counting_data, PeopleCountingData)
    
    # Coletando dados de heatmap
    logger.info("Coletando dados de heatmap...")
    for loja, ips in stores.items():
        process_data_for_store(loja, ips, date, "heatmapcsv&sub_type=0", parse_heatmap_data, HeatmapData)
    
    # Coletando dados de contagem regional de pessoas
    logger.info("Coletando dados de contagem regional de pessoas...")
    for loja, ips in stores.items():
        process_data_for_store(loja, ips, date, "regionalcountlogcsv&report_type=0&lengthtype=0&length=0&region1=1&region2=1&region3=1&region4=1", parse_regional_people_counting_data, RegionalPeopleCountingData)

    logger.info(f"Todos os dados foram atualizados com sucesso para o dia {date.strftime('%Y-%m-%d')}.")

def get_date_input(prompt):
    while True:
        date_str = input(prompt)
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d")
            return date
        except ValueError:
            print("Data inválida. Por favor, insira no formato AAAA-MM-DD.")

def main():
    # Solicita a data de início e a data de fim no terminal
    data_inicio = get_date_input("Digite a data de início (AAAA-MM-DD): ")
    data_fim = get_date_input("Digite a data de fim (AAAA-MM-DD): ")

    # Verificação se a data de fim não é no futuro
    if data_fim > datetime.now():
        logger.error("A data de término não pode ser posterior ao momento atual.")
        return

    # Coleta de dados para o intervalo de datas
    current_date = data_inicio
    while current_date <= data_fim:
        collect_data_for_day(current_date)
        current_date += timedelta(days=1)

    # Coletar dados do sensor para o intervalo de datas
    start_date_str = data_inicio.strftime('%Y-%m-%d %H:%M:%S')
    end_date_str = data_fim.strftime('%Y-%m-%d %H:%M:%S')
    coletar_dados_sensor_OML01(start_date_str, end_date_str)

if __name__ == "__main__":
    main()
