import requests
from core.config import URL_BASE, USERNAME, PASSWORD

def autenticar():
    auth_url = f"{URL_BASE}/api/autenticar"
    auth_payload = {
        "Username": USERNAME,
        "Password": PASSWORD
    }
    response = requests.post(auth_url, json=auth_payload)
    response.raise_for_status()
    auth_data = response.json()
    if auth_data.get('Sucesso'):
        return auth_data['Objecto']['IdentityToken']
    else:
        raise Exception(f"Erro na autenticação: {auth_data.get('Mensagem')}")