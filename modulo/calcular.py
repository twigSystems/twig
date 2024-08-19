# modulo/calcular.py
import pandas as pd

def calcular_total_pessoas(df):
    return df['Total'].sum()

def calcular_tempo_medio_permanencia(df):
    return df['Duração (segundos)'].mean()

def calcular_ocupacao_media(df):
    return df.mean(axis=1).mean()

def obter_datas(periodo):
    from datetime import datetime, timedelta
    now = datetime.now()
    if periodo == "Hoje":
        inicio = now.replace(hour=0, minute=0, second=0, microsecond=0)
        fim = now
    elif periodo == "Ontem":
        inicio = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        fim = inicio + timedelta(days=1)
    elif periodo == "Esta Semana":
        inicio = now - timedelta(days=now.weekday())
        inicio = inicio.replace(hour=0, minute=0, second=0, microsecond=0)
        fim = now
    elif periodo == "Este Mês":
        inicio = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        fim = now
    else:
        raise ValueError("Período inválido")
    
    inicio_lp = inicio - (fim - inicio)
    fim_lp = inicio - timedelta(seconds=1)
    return inicio, fim, inicio_lp, fim_lp
