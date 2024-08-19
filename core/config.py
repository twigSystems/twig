import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# URL base da API
URL_BASE = os.getenv('URL_BASE', "https://mainfashion-api.retailmanager.pt")

# Credenciais de autenticação
USERNAME = os.getenv('API_USERNAME', "consulta")
PASSWORD = os.getenv('API_PASSWORD', "Mf@2023!")

# Credenciais e URLs das lojas
username = os.getenv('STORE_USERNAME', "admin")
password = os.getenv('STORE_PASSWORD', "grnl.2024")
stores = {
    "OML01-Omnia GuimarãesShopping": ["http://93.108.96.96:21001/"],
    "ONL01-Only UBBO Amadora": ["http://93.108.245.76:21002/", "http://93.108.245.76:21003/"],
    "OML02-Omnia Fórum Almada": ["http://188.37.190.134:2201/"],
    "OML03-Omnia Norteshopping": ["http://188.37.124.33:21002/"]
}

# Itens a serem desconsiderados nas análises
itens_desconsiderados = [
    "5713758079406", "5713759955808", "5713759956829", 
    "CARRIERBAG1", "GIFTOPTION1", "GIFTOPTION2"
]

# Token do Telegram
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', "7320381187:AAGGqR6KIQySaEP7P_ucCf_9HV-gku7duM8")     # Token de Teste
TELEGRAM_TOKEN_TEST = os.getenv('TELEGRAM_TOKEN_TEST', "6673747177:AAGqX0BT0WFCBUjBpwrXt0tv4PjwdcpawSQ")   # Token do bot de Produção
TELEGRAM_TOKEN_QA = os.getenv('TELEGRAM_TOKEN_QA', "6629566873:AAGk0NdEK5cCXR98b1vuICqXyw-slQ8251c")   # Token do bot de QA

# Configurações do e-mail
EMAIL_CONFIG = {
    'SMTP_SERVER': os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
    'SMTP_PORT': int(os.getenv('SMTP_PORT', 587)),
    'SMTP_USER': os.getenv('EMAIL_USER', 'support.eur@greenole.com'),
    'SMTP_PASSWORD': os.getenv('EMAIL_PASSWORD', 'tpmy abeh vkis jdox'),
    'FROM_EMAIL': os.getenv('EMAIL_FROM', 'support.eur@greenole.com'),
    'TO_EMAILS': os.getenv('EMAIL_TO', 'pedro@greenole.com, martaamendoeira@mainfashion.pt, gilgoncalves@maintarget.pt').split(',')
}

# Configuração do banco de dados
DATABASE_URL = 'sqlite:///c:/projetos/grnl_platform/bot_database.db'

# Configurações da API do Trello
TRELLO_API_KEY = os.getenv('TRELLO_API_KEY', 'sua_chave_de_api')
TRELLO_TOKEN = os.getenv('TRELLO_TOKEN', 'seu_token')
TRELLO_BOARD_ID = os.getenv('TRELLO_BOARD_ID', 'IXYBBIdP')
