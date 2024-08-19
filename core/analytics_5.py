import sys
import os
import logging
import argparse
from datetime import datetime, timedelta
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from sqlalchemy import func, distinct, create_engine, and_
import schedule


# Adicionar o diretório do projeto ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Tentar importar o módulo data_collector
try:
    from data_collector import armazenar_resultados_analiticos
except ImportError as e:
    logging.error(f"Erro ao importar data_collector: {str(e)}")
    sys.exit(1)

from core.models import SaleData, PeopleCountingData, HeatmapData, LastUpdate, AnalyticsResults, RegionalPeopleCountingData
from core.config import DATABASE_URL, itens_desconsiderados, stores

# Configuração do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Conexão com o banco de dados
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# Dicionário de nomes de regiões por loja
region_names = {
    "OML01-Omnia GuimarãesShopping": {
        'region1': 'Parede Direita',
        'region2': 'Mesa Central',
        'region3': 'Parede Esquerda',
        'region4': 'Balcão'
    },
    "OML02-Omnia Fórum Almada": {
        'region1': 'Parede Direita',
        'region2': 'Mesa Central',
        'region3': 'Parede Esquerda',
        'region4': 'Balcão'
    },
    "OML03-Omnia Norteshopping": {
        'region1': 'Parede Direita',
        'region2': 'Mesa Central',
        'region3': 'Parede Esquerda',
        'region4': 'Balcão'
    },
    "ONL01-Only UBBO Amadora": {
        'region1': 'Parede Direita',
        'region2': 'Parede Centro',
        'region3': 'Mesa',
        'region4': 'Hotspot',
        'region5': 'Ilha Centro',
        'region6': 'Canto',
        'region7': 'Parede Esquerda',
        'region8': 'Montra'
    }
}

# Funções auxiliares para cálculos e formatação
def calcular_diferenca(valor_atual, valor_anterior):
    if valor_anterior == 0:
        return valor_atual * 100
    return ((valor_atual - valor_anterior) / valor_anterior) * 100

def mostrar_resultados(atual, anterior, descricao, monetario=False):
    diferenca = calcular_diferenca(atual, anterior)
    direcao = "🟢" if diferenca >= 0 else "🔴"
    valor_atual = f"€{atual:.2f}" if monetario else f"{int(atual)}"
    return f"{direcao} {descricao}: {valor_atual} | {diferenca:.2f}%"

def mostrar_resultados_percentual(atual, anterior, descricao):
    diferenca = calcular_diferenca(atual, anterior)
    direcao = "🟢" if diferenca >= 0 else "🔴"
    valor_atual = f"{atual:.2f}%"
    return f"{direcao} {descricao}: {valor_atual} | {diferenca:.2f}%"

def mostrar_resultados_minutos(atual, anterior, descricao):
    valor_atual = f"{atual:.2f} min" if atual != 0 else "0 min"
    diferenca = calcular_diferenca(atual, anterior)
    direcao = "🟢" if diferenca >= 0 else "🔴"
    return f"{direcao} {descricao}: {valor_atual} | {diferenca:.2f}%"

def mostrar_resultados_unidades(atual, anterior, descricao):
    diferenca = calcular_diferenca(atual, anterior)
    direcao = "🟢" if diferenca >= 0 else "🔴"
    return f"{direcao} {descricao}: {atual:.2f} u. | {diferenca:.2f}%"

def mostrar_resultados_devolucoes(atual, anterior, descricao):
    diferenca = calcular_diferenca(atual, anterior)
    direcao = "🟢" if diferenca <= 0 else "🔴"  # Menos devoluções é melhor.
    valor_atual = f"{atual:.2f}%" if isinstance(atual, (int, float)) else "0%"
    return f"{direcao} {descricao}: {valor_atual} | {diferenca:.2f}%"

def mostrar_resultados_descontos(atual, anterior, descricao):
    diferenca = calcular_diferenca(atual, anterior)
    direcao = "🟢" if diferenca <= 0 else "🔴"  # Menos descontos é melhor.
    valor_atual = f"{atual:.2f}%" if isinstance(atual, (int, float)) else "0%"
    return f"{direcao} {descricao}: {valor_atual} | {diferenca:.2f}%"

def mostrar_resultados_ocupacao(valor_atual, valor_anterior, descricao):
    diferenca = calcular_diferenca(valor_atual, valor_anterior)
    direcao = "🟢" if diferenca > 0 else "🔴"
    return f"{direcao} {descricao}: {valor_atual:.2f}% | {diferenca:.2f}%"

# Funções principais de consulta e cálculo
def calcular_percentagem_ocupacao(loja, data_inicio, data_fim):
    registros = session.query(RegionalPeopleCountingData).filter(
        RegionalPeopleCountingData.loja == loja,
        RegionalPeopleCountingData.start_time >= data_inicio,
        RegionalPeopleCountingData.end_time <= data_fim
    ).all()

    total_ocupacao = {}
    total_geral = 0

    for registro in registros:
        if registro.ip not in total_ocupacao:
            total_ocupacao[registro.ip] = {'region1': 0, 'region2': 0, 'region3': 0, 'region4': 0}
        total_ocupacao[registro.ip]['region1'] += registro.region1
        total_ocupacao[registro.ip]['region2'] += registro.region2
        total_ocupacao[registro.ip]['region3'] += registro.region3
        total_ocupacao[registro.ip]['region4'] += registro.region4
        total_geral += (registro.region1 + registro.region2 + registro.region3 + registro.region4)

    numero_de_ips = len(total_ocupacao)
    numero_de_regioes = numero_de_ips * 4

    aggregated_regions = {f'region{i}': 0 for i in range(1, numero_de_regioes + 1)}

    index = 1
    for ip, ocupacao in total_ocupacao.items():
        for i in range(1, 5):
            region_key = f'region{index}'
            aggregated_regions[region_key] = ocupacao[f'region{i}']
            index += 1

    percentagens_ocupacao = {}
    for region, total in aggregated_regions.items():
        percentagens_ocupacao[region] = (total / total_geral) * 100 if total_geral > 0 else 0

    # Renomear as regiões conforme o dicionário de nomes de regiões
    if loja in region_names:
        percentagens_ocupacao_nomes = {}
        for region, percentagem in percentagens_ocupacao.items():
            region_name = region_names[loja].get(region, region)
            percentagens_ocupacao_nomes[region_name] = percentagem
        return percentagens_ocupacao_nomes

    return percentagens_ocupacao

def calcular_top_2_regioes_ocupadas(percentagens_ocupacao):
    return sorted(percentagens_ocupacao.items(), key=lambda x: x[1], reverse=True)[:2]

def calcular_menos_2_regioes_ocupadas(percentagens_ocupacao):
    return sorted(percentagens_ocupacao.items(), key=lambda x: x[1])[:2]

def consultar_vendas(lojas, data_inicio, data_fim):
    if isinstance(lojas, list) and len(lojas) == 1:
        lojas = lojas[0]

    vendas = session.query(SaleData).filter(
        SaleData.loja == lojas,
        SaleData.data >= data_inicio,
        SaleData.data <= data_fim,
        ~SaleData.item.in_(itens_desconsiderados)
    ).all()

    total_vendas_com_iva = sum(venda.valor_venda_com_iva for venda in vendas)
    total_vendas_sem_iva = sum(venda.valor_venda_sem_iva for venda in vendas)
    total_quantidade = sum(venda.quantidade for venda in vendas)
    total_valor_descontos = sum(venda.desconto for venda in vendas)

    transacoes_vendas = session.query(distinct(SaleData.referencia_documento)).filter(
        SaleData.loja == lojas,
        SaleData.data >= data_inicio,
        SaleData.data <= data_fim,
        SaleData.valor_venda_sem_iva >= 0,
        ~SaleData.item.in_(itens_desconsiderados)
    ).count()

    total_valor_devolucoes = sum(abs(venda.valor_venda_sem_iva) for venda in vendas if venda.valor_venda_sem_iva < 0)

    visitantes = session.query(
        func.sum(PeopleCountingData.line1_in + PeopleCountingData.line2_in + PeopleCountingData.line3_in)
    ).filter(
        PeopleCountingData.loja == lojas,
        PeopleCountingData.start_time >= data_inicio,
        PeopleCountingData.end_time <= data_fim
    ).scalar()

    soma_duracao_permanencia = session.query(
        func.sum(HeatmapData.value)
    ).filter(
        HeatmapData.loja == lojas,
        HeatmapData.start_time >= data_inicio,
        HeatmapData.end_time <= data_fim
    ).scalar()

    visitantes = visitantes or 0
    soma_duracao_permanencia = soma_duracao_permanencia or 0

    soma_duracao_permanencia_minutos = soma_duracao_permanencia / 60
    tempo_medio_permanencia = soma_duracao_permanencia_minutos / visitantes if visitantes > 0 else 0
    unidades_por_transacao = total_quantidade / transacoes_vendas if transacoes_vendas > 0 else 0
    ticket_medio_com_iva = total_vendas_com_iva / transacoes_vendas if transacoes_vendas > 0 else 0
    ticket_medio_sem_iva = total_vendas_sem_iva / transacoes_vendas if transacoes_vendas > 0 else 0
    taxa_conversao = (transacoes_vendas / visitantes) * 100 if visitantes > 0 else 0

    indice_devolucoes = (total_valor_devolucoes / total_vendas_sem_iva) * 100 if total_vendas_sem_iva > 0 else 0
    indice_descontos = (total_valor_descontos / total_vendas_com_iva) * 100 if total_vendas_com_iva > 0 else 0

    total_line4_in = session.query(
        func.sum(PeopleCountingData.line4_in)
    ).filter(
        PeopleCountingData.loja == lojas,
        PeopleCountingData.start_time >= data_inicio,
        PeopleCountingData.end_time <= data_fim
    ).scalar()

    total_line4_out = session.query(
        func.sum(PeopleCountingData.line4_out)
    ).filter(
        PeopleCountingData.loja == lojas,
        PeopleCountingData.start_time >= data_inicio,
        PeopleCountingData.end_time <= data_fim
    ).scalar()

    total_passagens = (total_line4_in or 0) + (total_line4_out or 0)
    taxa_entrada = visitantes / total_passagens * 100 if total_passagens > 0 else 0

    top_vendedores = session.query(
        SaleData.vendedor_nome_curto,
        func.sum(SaleData.valor_venda_sem_iva)
    ).filter(
        SaleData.loja == lojas,
        SaleData.data >= data_inicio,
        SaleData.data <= data_fim,
        ~SaleData.item.in_(itens_desconsiderados)
    ).group_by(SaleData.vendedor_nome_curto).order_by(func.sum(SaleData.valor_venda_sem_iva).desc()).limit(3).all()

    top_produtos = session.query(
        SaleData.item,
        SaleData.descritivo,
        func.sum(SaleData.quantidade)
    ).filter(
        SaleData.loja == lojas,
        SaleData.data >= data_inicio,
        SaleData.data <= data_fim,
        ~SaleData.item.in_(itens_desconsiderados)
    ).group_by(SaleData.item, SaleData.descritivo).order_by(func.sum(SaleData.quantidade).desc()).limit(5).all()

    ultima_coleta = session.query(func.max(LastUpdate.last_update_time)).scalar()

    logger.info(f"Consulta de vendas para a loja {lojas} de {data_inicio} a {data_fim} retornou {len(vendas)} registros")

    # Calcular a percentagem de ocupação das regiões
    ocupacao_regioes = calcular_percentagem_ocupacao(lojas, data_inicio, data_fim)
    
    return {
        'total_vendas_com_iva': total_vendas_com_iva,
        'total_vendas_sem_iva': total_vendas_sem_iva,
        'transacoes_vendas': transacoes_vendas,
        'visitantes': visitantes,
        'taxa_conversao': taxa_conversao,
        'tempo_medio_permanencia': tempo_medio_permanencia,
        'ticket_medio_com_iva': ticket_medio_com_iva,
        'ticket_medio_sem_iva': ticket_medio_sem_iva,
        'unidades_por_transacao': unidades_por_transacao,
        'indice_devolucoes': indice_devolucoes,
        'indice_descontos': indice_descontos,
        'entry_rate': taxa_entrada,
        'top_vendedores': top_vendedores,
        'top_produtos': [(item, descritivo, quantidade) for item, descritivo, quantidade in top_produtos],
        'ultima_coleta': ultima_coleta,
        'line4_in': total_line4_in or 0,
        'line4_out': total_line4_out or 0,
        'total_passagens': total_passagens or 0,
        'ocupacao_regioes': ocupacao_regioes,  # Adicionar ocupacao_regioes ao retorno
   }

def consultar_vendas_multiplas_lojas(lojas, data_inicio, data_fim):
    if isinstance(lojas, str):
        lojas = [lojas]

    vendas = session.query(SaleData).filter(
        SaleData.loja.in_(lojas),
        SaleData.data >= data_inicio,
        SaleData.data <= data_fim,
        ~SaleData.item.in_(itens_desconsiderados)
    ).all()

    total_vendas_com_iva = sum(venda.valor_venda_com_iva for venda in vendas)
    total_vendas_sem_iva = sum(venda.valor_venda_sem_iva for venda in vendas)
    total_quantidade = sum(venda.quantidade for venda in vendas)
    total_valor_descontos = sum(venda.desconto for venda in vendas)

    transacoes_vendas = session.query(distinct(SaleData.referencia_documento)).filter(
        SaleData.loja.in_(lojas),
        SaleData.data >= data_inicio,
        SaleData.data <= data_fim,
        SaleData.valor_venda_sem_iva >= 0,
        ~SaleData.item.in_(itens_desconsiderados)
    ).count()

    total_valor_devolucoes = sum(abs(venda.valor_venda_sem_iva) for venda in vendas if venda.valor_venda_sem_iva < 0)

    visitantes = session.query(
        func.sum(PeopleCountingData.line1_in + PeopleCountingData.line2_in + PeopleCountingData.line3_in)
    ).filter(
        PeopleCountingData.loja.in_(lojas),
        PeopleCountingData.start_time >= data_inicio,
        PeopleCountingData.end_time <= data_fim
    ).scalar()

    soma_duracao_permanencia = session.query(
        func.sum(HeatmapData.value)
    ).filter(
        HeatmapData.loja.in_(lojas),
        HeatmapData.start_time >= data_inicio,
        HeatmapData.end_time <= data_fim
    ).scalar()

    visitantes = visitantes or 0
    soma_duracao_permanencia = soma_duracao_permanencia or 0

    soma_duracao_permanencia_minutos = soma_duracao_permanencia / 60
    tempo_medio_permanencia = soma_duracao_permanencia_minutos / visitantes if visitantes > 0 else 0
    unidades_por_transacao = total_quantidade / transacoes_vendas if transacoes_vendas > 0 else 0
    ticket_medio_com_iva = total_vendas_com_iva / transacoes_vendas if transacoes_vendas > 0 else 0
    ticket_medio_sem_iva = total_vendas_sem_iva / transacoes_vendas if transacoes_vendas > 0 else 0
    taxa_conversao = (transacoes_vendas / visitantes) * 100 if visitantes > 0 else 0

    indice_devolucoes = (total_valor_devolucoes / total_vendas_sem_iva) * 100 if total_vendas_sem_iva > 0 else 0
    indice_descontos = (total_valor_descontos / total_vendas_com_iva) * 100 if total_vendas_com_iva > 0 else 0

    total_line4_in = session.query(
        func.sum(PeopleCountingData.line4_in)
    ).filter(
        PeopleCountingData.loja.in_(lojas),
        PeopleCountingData.start_time >= data_inicio,
        PeopleCountingData.end_time <= data_fim
    ).scalar()

    total_line4_out = session.query(
        func.sum(PeopleCountingData.line4_out)
    ).filter(
        PeopleCountingData.loja.in_(lojas),
        PeopleCountingData.start_time >= data_inicio,
        PeopleCountingData.end_time <= data_fim
    ).scalar()

    total_passagens = (total_line4_in or 0) + (total_line4_out or 0)
    taxa_entrada = visitantes / total_passagens * 100 if total_passagens > 0 else 0

    top_vendedores = session.query(
        SaleData.vendedor_nome_curto,
        func.sum(SaleData.valor_venda_sem_iva)
    ).filter(
        SaleData.loja.in_(lojas),
        SaleData.data >= data_inicio,
        SaleData.data <= data_fim,
        ~SaleData.item.in_(itens_desconsiderados)
    ).group_by(SaleData.vendedor_nome_curto).order_by(func.sum(SaleData.valor_venda_sem_iva).desc()).limit(3).all()

    top_produtos = session.query(
        SaleData.item,
        SaleData.descritivo,
        func.sum(SaleData.quantidade)
    ).filter(
        SaleData.loja.in_(lojas),
        SaleData.data >= data_inicio,
        SaleData.data <= data_fim,
        ~SaleData.item.in_(itens_desconsiderados)
    ).group_by(SaleData.item, SaleData.descritivo).order_by(func.sum(SaleData.quantidade).desc()).limit(5).all()

    ultima_coleta = session.query(func.max(LastUpdate.last_update_time)).scalar()

    ocupacao_regioes = calcular_percentagem_ocupacao(lojas, data_inicio, data_fim)

    return {
        'total_vendas_com_iva': total_vendas_com_iva,
        'total_vendas_sem_iva': total_vendas_sem_iva,
        'transacoes_vendas': transacoes_vendas,
        'visitantes': visitantes,
        'taxa_conversao': taxa_conversao,
        'tempo_medio_permanencia': tempo_medio_permanencia,
        'ticket_medio_com_iva': ticket_medio_com_iva,
        'ticket_medio_sem_iva': ticket_medio_sem_iva,
        'unidades_por_transacao': unidades_por_transacao,
        'indice_devolucoes': indice_devolucoes,
        'indice_descontos': indice_descontos,
        'entry_rate': taxa_entrada,
        'top_vendedores': top_vendedores,
        'top_produtos': [(item, descritivo, quantidade) for item, descritivo, quantidade in top_produtos],
        'ultima_coleta': ultima_coleta,
        'line4_in': total_line4_in or 0,
        'line4_out': total_line4_out or 0,
        'total_passagens': total_passagens or 0,
        'ocupacao_regioes': ocupacao_regioes,
   }

# Função para calcular resultados hora a hora
def calcular_resultados_hora_a_hora(lojas, data_inicio, data_fim):
    resultados_hora_a_hora = []

    for loja in lojas:
        current_time = data_inicio

        while current_time < data_fim:
            # Definir o próximo tempo como o final da hora atual (hh:59:59)
            next_time = current_time.replace(minute=59, second=59, microsecond=0)

            # Garantir que não ultrapasse a data de fim especificada
            if next_time > data_fim:
                next_time = data_fim

            # Consultar os resultados de vendas para o período atual
            resultados = consultar_vendas(loja, current_time, next_time)
            resultados['loja'] = loja
            resultados['data_inicio'] = current_time
            resultados['data_fim'] = next_time

            # Armazenar os resultados na lista
            resultados_hora_a_hora.append(resultados)

            # Avançar para o início da próxima hora
            current_time = current_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

    return resultados_hora_a_hora

def armazenar_resultados_analiticos(resultados):
    session = Session()
    try:
        # Verifica se já existe um registro para a mesma loja e período
        registro_existente = session.query(AnalyticsResults).filter(
            AnalyticsResults.loja == resultados['loja'],
            AnalyticsResults.data_inicio == resultados['data_inicio'],
            AnalyticsResults.data_fim == resultados['data_fim']
        ).first()

        if registro_existente:
            # Atualiza o registro existente
            registro_existente.total_vendas_com_iva = resultados['total_vendas_com_iva']
            registro_existente.total_vendas_sem_iva = resultados['total_vendas_sem_iva']
            registro_existente.transacoes_vendas = resultados['transacoes_vendas']
            registro_existente.visitantes = resultados['visitantes']
            registro_existente.taxa_conversao = resultados['taxa_conversao']
            registro_existente.tempo_medio_permanencia = resultados['tempo_medio_permanencia']
            registro_existente.ticket_medio_com_iva = resultados['ticket_medio_com_iva']
            registro_existente.ticket_medio_sem_iva = resultados['ticket_medio_sem_iva']
            registro_existente.unidades_por_transacao = resultados['unidades_por_transacao']
            registro_existente.indice_devolucoes = resultados['indice_devolucoes']
            registro_existente.indice_descontos = resultados['indice_descontos']
            registro_existente.entry_rate = resultados['entry_rate']
            registro_existente.total_passagens = resultados['total_passagens']
            registro_existente.ultima_coleta = resultados['ultima_coleta']
        else:
            # Cria um novo registro
            novo_registro = AnalyticsResults(
                loja=resultados['loja'],
                data_inicio=resultados['data_inicio'],
                data_fim=resultados['data_fim'],
                total_vendas_com_iva=resultados['total_vendas_com_iva'],
                total_vendas_sem_iva=resultados['total_vendas_sem_iva'],
                transacoes_vendas=resultados['transacoes_vendas'],
                visitantes=resultados['visitantes'],
                taxa_conversao=resultados['taxa_conversao'],
                tempo_medio_permanencia=resultados['tempo_medio_permanencia'],
                ticket_medio_com_iva=resultados['ticket_medio_com_iva'],
                ticket_medio_sem_iva=resultados['ticket_medio_sem_iva'],
                unidades_por_transacao=resultados['unidades_por_transacao'],
                indice_devolucoes=resultados['indice_devolucoes'],
                indice_descontos=resultados['indice_descontos'],
                entry_rate=resultados['entry_rate'],
                total_passagens=resultados['total_passagens'],
                ultima_coleta=resultados['ultima_coleta']
            )
            session.add(novo_registro)

        session.commit()
        logger.info("Resultados analíticos armazenados com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao armazenar resultados analíticos: {str(e)}", exc_info=True)
        session.rollback()
    finally:
        session.close()

# Funções para comparar períodos e obter datas
def comparar_periodo_anterior(loja, data_inicio, data_fim, now):
    if not data_inicio or not data_fim:
        return {}, {}

    periodo = data_fim - data_inicio

    if periodo <= timedelta(days=1):  # Hoje
        data_inicio_anterior = data_inicio - timedelta(days=7)
        data_fim_anterior = data_fim - timedelta(days=7)
    elif periodo <= timedelta(days=2):  # Ontem
        data_inicio_anterior = data_inicio - timedelta(days=7)
        data_fim_anterior = data_fim - timedelta(days=7)
    elif periodo <= timedelta(weeks=1):  # Esta Semana
        data_inicio_anterior = data_inicio - timedelta(days=7)
        data_fim_anterior = now - timedelta(days=7)
    else:  # Este Mês
        delta_days = periodo.days
        data_inicio_anterior = (data_inicio.replace(day=1) - timedelta(days=1)).replace(day=1)
        data_fim_anterior = data_inicio_anterior + timedelta(days=delta_days)

    resultados_atuais = consultar_vendas([loja], data_inicio, data_fim)
    resultados_anteriores = consultar_vendas([loja], data_inicio_anterior, data_fim_anterior)

    return resultados_atuais, resultados_anteriores

def obter_datas(periodo, now=None):
    if now is None:
        now = datetime.now()  # Inicializar `now` se não for passado
    if periodo == "Hoje":
        inicio = now.replace(hour=0, minute=0, second=0, microsecond=0)
        fim = now
        inicio_lp = (inicio - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
        fim_lp = inicio_lp + (fim - inicio)
    elif periodo == "Ontem":
        ontem = now - timedelta(days=1)
        inicio = ontem.replace(hour=0, minute=0, second=0, microsecond=0)
        fim = ontem.replace(hour=23, minute=59, second=59, microsecond=0)
        inicio_lp = inicio - timedelta(days=7)
        fim_lp = fim - timedelta(days=7)
    elif periodo == "Esta Semana":
        inicio = now - timedelta(days=now.weekday())
        inicio = inicio.replace(hour=0, minute=0, second=0, microsecond=0)
        fim = now
        inicio_lp = inicio - timedelta(days=7)
        fim_lp = inicio_lp + (fim - inicio)
    elif periodo == "Este Mês":
        inicio = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        fim = now
        inicio_lp = (inicio - timedelta(days=inicio.day)).replace(day=1)
        fim_lp = inicio_lp + (fim - inicio)
    elif periodo == "Customizado":
        inicio = None
        fim = None
        inicio_lp = None
        fim_lp = None
    return inicio, fim, inicio_lp, fim_lp

# Função principal
def main():
    parser = argparse.ArgumentParser(description="Script de análise")
    parser.add_argument("--start_time", type=str, required=True, help="Tempo de início no formato 'YYYY-MM-DD HH:MM:SS'")
    parser.add_argument("--end_time", type=str, required=True, help="Tempo de término no formato 'YYYY-MM-DD HH:MM:SS'")

    args = parser.parse_args()

    data_inicio = datetime.strptime(args.start_time, "%Y-%m-%d %H:%M:%S")
    data_fim = datetime.strptime(args.end_time, "%Y-%m-%d %H:%M:%S")

    # Lista de todas as lojas
    lojas = [
        "OML01-Omnia GuimarãesShopping",
        "OML02-Omnia Fórum Almada",
        "OML03-Omnia Norteshopping",
        "ONL01-Only UBBO Amadora"
    ]

    # Calcular os resultados hora a hora para todas as lojas
    resultados_hora_a_hora = calcular_resultados_hora_a_hora(lojas, data_inicio, data_fim)

    for resultados in resultados_hora_a_hora:
        armazenar_resultados_analiticos(resultados)

        # Imprimir os resultados das vendas e ocupação das regiões
        print(f"Resultados para {resultados['loja']} de {resultados['data_inicio']} a {resultados['data_fim']}:")
        for chave, valor in resultados.items():
            if isinstance(valor, list) and chave == 'top_vendedores':
                print(f"{chave}:")
                for vendedor in valor:
                    print(f"  - Vendedor: {vendedor[0]}, Total: {vendedor[1]}")
            elif isinstance(valor, list) and chave == 'top_produtos':
                print(f"{chave}:")
                for produto in valor:
                    print(f"  - Produto: {produto[0]}, Descritivo: {produto[1]}, Quantidade: {produto[2]}")
            elif isinstance(valor, dict) and chave == 'ocupacao_regioes':
                print(f"{chave}:")
                for regiao, percentagem in valor.items():
                    print(f"  - {regiao}: {percentagem:.2f}%")
            else:
                print(f"{chave}: {valor}")
        print("\n")

if __name__ == "__main__":
    main()
