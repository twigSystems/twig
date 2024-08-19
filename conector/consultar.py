import requests

def consultar_vendas(jwt_token, data, loja):
    url_base = "https://mainfashion-api.retailmanager.pt"
    consulta_url = f"{url_base}/api/consulta/executarsync"
    consulta_payload = {
        "ConsultaId": "3af64719-a6b3-ee11-8933-005056b8cd07",
        "Parametros": [
            {"Nome": "Data", "Valor": data},
            {"Nome": "Loja", "Valor": loja}
        ]
    }
    headers = {
        "Authorization": f"Bearer {jwt_token}"
    }
    response = requests.post(consulta_url, json=consulta_payload, headers=headers)
    response.raise_for_status()
    return response.json()