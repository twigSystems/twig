from celery import Celery

app = Celery('grnl_platform', broker='redis://localhost:8904/0')

app.conf.update(
    result_backend='redis://localhost:8904/0',
)

app.conf.update(
    result_expires=3600,
    worker_concurrency=10  # Define a concorrência de workers
)

@app.task
def add(x, y):
    return x + y

@app.task
def processar_dados_pesados(param1, param2):
    # Implemente sua lógica pesada aqui
    return f"Processado {param1} e {param2}"

if __name__ == '__main__':
    app.start()
