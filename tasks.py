from celery_config import app

@app.task
def processar_dados_pesados(param1, param2):
    # Simule uma tarefa pesada com um tempo de espera
    import time
    time.sleep(10)
    return f"Dados processados com {param1} e {param2}"
