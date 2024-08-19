import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from core.models import SaleData, PeopleCountingData, HeatmapData, RegionalPeopleCountingData, LastUpdate, armazenar_dados_no_banco
from conector.autenticar import autenticar
from conector.consultar import consultar_vendas
from tenacity import retry, wait_fixed, stop_after_attempt
import pandas as pd
from io import StringIO
import requests

# Configuração do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração do banco de dados
DATABASE_URL = 'sqlite:///c:/projetos/grnl_platform/bot_database.db'
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# URLs das lojas
stores = {
    "OML01-Omnia GuimarãesShopping": [],
    "ONL01-Only UBBO Amadora": ["93.108.245.76:21002", "93.108.245.76:21003"],
    "OML02-Omnia Fórum Almada": ["188.37.190.134:2201"],
    "OML03-Omnia Norteshopping": ["188.37.124.33:21002"]
}

def get_jwt_token():
    return autenticar()

@retry(wait=wait_fixed(10), stop=stop_after_attempt(5))
def consultar_vendas_com_retry(jwt_token, data, loja):
    return consultar_vendas(jwt_token, data, loja)

def coletar_dados_vendas(jwt_token, data, loja):
    consulta_data = consultar_vendas_com_retry(jwt_token, data.strftime('%Y-%m-%d'), loja)
    logger.info(f"Resposta da consulta de vendas para a data {data} e loja {loja}: {consulta_data}")
    dados = []
    if consulta_data.get('Sucesso'):
        result_sets = consulta_data['Objecto']['ResultSets']
        if result_sets:
            first_result_set = result_sets[0]
            if first_result_set:
                dados.extend(first_result_set)
    return [dict(t) for t in {tuple(d.items()) for d in dados}]

@retry(wait=wait_fixed(10), stop=stop_after_attempt(5))
def fetch_and_store(url, parse_function, model, loja, ip):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = parse_function(response.text, loja, ip)
        # Convert objects to dictionaries and remove SQLAlchemy internal attributes
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
                zartotal_in=int(row['Line1 - In']) + int(row['Line2 - In']) + int(row['Line3 - In']),
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
        dados_vendas = coletar_dados_vendas(jwt_token, date, loja)  # Passa 'loja' como argumento
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

    # Coleta de dados para o intervalo de datas
    current_date = data_inicio
    while current_date <= data_fim:
        collect_data_for_day(current_date)
        current_date += timedelta(days=1)

if __name__ == "__main__":
    main()
