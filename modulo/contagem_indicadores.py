def calcular_total_pessoas(df):
    return df['Total'].sum()

def calcular_tempo_medio_permanencia(df):
    return df['Duração (segundos)'].mean()

def calcular_ocupacao_media(df):
    return df.mean(axis=1).mean()
