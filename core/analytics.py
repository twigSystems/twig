import logging
from datetime import datetime, timedelta
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func, distinct, create_engine
import sys
import os

from data_collector import armazenar_resultados_analiticos

# Adicionar o diretório do projeto ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.models import SaleData, PeopleCountingData, HeatmapData, LastUpdate, AnalyticsResults
from core.config import DATABASE_URL, itens_desconsiderados, stores

# Configuração do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Conexão com o banco de dados
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# Mapeamento de grupos para lojas
groups = {
    'OMNIA': ['OML01-Omnia GuimarãesShopping', 'OML02-Omnia Fórum Almada', 'OML03-Omnia Norteshopping'],
    'ONLY': ['ONL01-Only UBBO Amadora']
}

def calcular_diferenca(valor_atual, valor_anterior):
    if valor_anterior == 0:
        return valor_atual * 100
    return ((valor_atual - valor_anterior) / valor_anterior) * 100

def mostrar_resultados(valor_atual, valor_anterior, descricao, monetario=False):
    diferenca = calcular_diferenca(valor_atual, valor_anterior)
    cor = "🟢" if diferenca > 0 else "🔴"
    if monetario:
        return f"{descricao}: €{valor_atual:.0f} | {diferenca:.2f}% {cor}"
    else:
        return f"{descricao}: {valor_atual:.0f} | {diferenca:.2f}% {cor}"

def mostrar_resultados_percentual(valor_atual, valor_anterior, descricao):
    diferenca = calcular_diferenca(valor_atual, valor_anterior)
    cor = "🟢" if diferenca > 0 else "🔴"
    return f"{descricao}: {valor_atual:.2f}% | {diferenca:.2f}% {cor}"

def mostrar_resultados_minutos(valor_atual, valor_anterior, descricao):
    diferenca = calcular_diferenca(valor_atual, valor_anterior)
    cor = "🟢" if diferenca > 0 else "🔴"
    return f"{descricao}: {valor_atual:.2f} min | {diferenca:.2f}% {cor}"

def mostrar_resultados_unidades(valor_atual, valor_anterior, descricao):
    diferenca = calcular_diferenca(valor_atual, valor_anterior)
    cor = "🟢" if diferenca > 0 else "🔴"
    return f"{descricao}: {valor_atual:.2f} u. | {diferenca:.2f}% {cor}"

def mostrar_resultados_devolucoes(valor_atual, valor_anterior, descricao):
    diferenca = calcular_diferenca(valor_atual, valor_anterior)
    cor = "🟢" if diferenca < 0 else "🔴"
    return f"{descricao}: {valor_atual:.2f}% | {diferenca:.2f}% {cor}"

def mostrar_resultados_descontos(valor_atual, valor_anterior, descricao):
    diferenca = calcular_diferenca(valor_atual, valor_anterior)
    cor = "🟢" if diferenca < 0 else "🔴"
    return f"{descricao}: {valor_atual:.2f}% | {diferenca:.2f}% {cor}"

def consultar_vendas(lojas, data_inicio, data_fim):
    session = Session()
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

    if visitantes is None:
        visitantes = 0

    if soma_duracao_permanencia is None:
        soma_duracao_permanencia = 0

    soma_duracao_permanencia_minutos = soma_duracao_permanencia / 60

    if visitantes > 0:
        tempo_medio_permanencia = soma_duracao_permanencia_minutos / visitantes
    else:
        tempo_medio_permanencia = 0

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

    if total_passagens > 0:
        taxa_entrada = visitantes / total_passagens * 100
    else:
        taxa_entrada = 0

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

    # Log para verificar se a consulta retornou resultados
    logger.info(f"Consulta de vendas para a loja {lojas} de {data_inicio} a {data_fim} retornou {len(vendas)} registros")

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
        'total_passagens': total_passagens or 0
    }

def consultar_dados_acumulados_por_grupo(grupo, inicio, fim, inicio_lp, fim_lp):
    session = Session()
    try:
        lojas_grupo = groups.get(grupo, [])

        def calcular_resultados(lojas, inicio, fim):
            resultados = {
                'total_vendas_com_iva': 0,
                'total_vendas_sem_iva': 0,
                'transacoes_vendas': 0,
                'visitantes': 0,
                'taxa_conversao': 0,
                'tempo_medio_permanencia': 0,
                'ticket_medio_com_iva': 0,
                'ticket_medio_sem_iva': 0,
                'unidades_por_transacao': 0,
                'indice_devolucoes': 0,
                'indice_descontos': 0,
                'entry_rate': 0,
                'line4_in': 0,
                'line4_out': 0,
                'total_passagens': 0,
                'top_vendedores': {},
                'top_produtos': {}
            }

            for loja in lojas:
                resultados_atuais = consultar_vendas([loja], inicio, fim)
                resultados = agregar_resultados(resultados, resultados_atuais)

            if resultados['visitantes'] > 0:
                resultados['taxa_conversao'] = (resultados['transacoes_vendas'] / resultados['visitantes']) * 100
            if resultados['transacoes_vendas'] > 0:
                resultados['ticket_medio_com_iva'] = resultados['total_vendas_com_iva'] / resultados['transacoes_vendas']
                resultados['ticket_medio_sem_iva'] = resultados['total_vendas_sem_iva'] / resultados['transacoes_vendas']
                resultados['unidades_por_transacao'] = resultados['unidades_por_transacao'] / resultados['transacoes_vendas']

            resultados['top_vendedores'] = sorted(resultados['top_vendedores'].items(), key=lambda x: x[1], reverse=True)[:5]
            resultados['top_produtos'] = sorted(resultados['top_produtos'].values(), key=lambda x: x[1], reverse=True)[:5]

            return resultados

        resultados_atuais = calcular_resultados(lojas_grupo, inicio, fim)
        resultados_anteriores = calcular_resultados(lojas_grupo, inicio_lp, fim_lp)

        return {
            'resultados_atuais': resultados_atuais,
            'resultados_anteriores': resultados_anteriores
        }

    except Exception as e:
        logger.error(f"Erro ao consultar dados acumulados por grupo: {str(e)}", exc_info=True)
        return {
            'resultados_atuais': {},
            'resultados_anteriores': {}
        }
    finally:
        session.close()

def agregar_resultados(agregado, resultados):
    for key, value in resultados.items():
        if isinstance(value, (int, float)):
            agregado[key] += value
        elif key in ['top_vendedores', 'top_produtos']:
            for subvalue in value:
                subkey = subvalue[0]  # Considerando que subvalue é uma tupla e o primeiro valor é a chave
                if subkey in agregado[key]:
                    agregado[key][subkey] += subvalue[1]
                else:
                    agregado[key][subkey] = subvalue[1]
        else:
            agregado[key] = value
    return agregado

def calcular_resultados(lojas, inicio, fim):
    resultados = {
        'total_vendas_com_iva': 0,
        'total_vendas_sem_iva': 0,
        'transacoes_vendas': 0,
        'visitantes': 0,
        'taxa_conversao': 0,
        'tempo_medio_permanencia': 0,
        'ticket_medio_com_iva': 0,
        'ticket_medio_sem_iva': 0,
        'unidades_por_transacao': 0,
        'indice_devolucoes': 0,
        'indice_descontos': 0,
        'entry_rate': 0,
        'line4_in': 0,
        'line4_out': 0,
        'total_passagens': 0,
        'top_vendedores': {},
        'top_produtos': {}
    }

    for loja in lojas:
        resultados_atuais = consultar_vendas([loja], inicio, fim)
        resultados = agregar_resultados(resultados, resultados_atuais)

    if resultados['visitantes'] > 0:
        resultados['taxa_conversao'] = (resultados['transacoes_vendas'] / resultados['visitantes']) * 100
    if resultados['transacoes_vendas'] > 0:
        resultados['ticket_medio_com_iva'] = resultados['total_vendas_com_iva'] / resultados['transacoes_vendas']
        resultados['ticket_medio_sem_iva'] = resultados['total_vendas_sem_iva'] / resultados['transacoes_vendas']
        resultados['unidades_por_transacao'] = resultados['unidades_por_transacao'] / resultados['transacoes_vendas']

    resultados['top_vendedores'] = sorted(resultados['top_vendedores'].items(), key=lambda x: x[1], reverse=True)[:5]
    resultados['top_produtos'] = sorted(resultados['top_produtos'].values(), key=lambda x: x[2], reverse=True)[:5]

    return resultados

def comparar_periodo_anterior(loja, data_inicio, data_fim):
    delta = timedelta(days=7)
    data_inicio_anterior = data_inicio - delta
    data_fim_anterior = data_fim - delta

    if loja in groups:
        resultados = consultar_dados_acumulados_por_grupo(loja, data_inicio, data_fim, data_inicio_anterior, data_fim_anterior)
        resultados_atuais = resultados['resultados_atuais']
        resultados_anteriores = resultados['resultados_anteriores']
    else:
        resultados_atuais = consultar_vendas([loja], data_inicio, data_fim)
        resultados_anteriores = consultar_vendas([loja], data_inicio_anterior, data_fim_anterior)

    return resultados_atuais, resultados_anteriores

def obter_datas(periodo):
    hoje = datetime.now()
    if periodo == "Hoje":
        inicio = hoje.replace(hour=0, minute=0, second=0, microsecond=0)
        fim = hoje
        inicio_lp = inicio - timedelta(days=7)
        fim_lp = fim - timedelta(days=7)
    elif periodo == "Ontem":
        ontem = hoje - timedelta(days=1)
        inicio = ontem.replace(hour=0, minute=0, second=0, microsecond=0)
        fim = ontem.replace(hour=23, minute=59, second=59, microsecond=0)
        inicio_lp = inicio - timedelta(days=7)
        fim_lp = fim - timedelta(days=7)
    elif periodo == "Esta Semana":
        inicio = hoje - timedelta(days=hoje.weekday())
        inicio = inicio.replace(hour=0, minute=0, second=0, microsecond=0)
        fim = hoje
        inicio_lp = inicio - timedelta(days=7)
        fim_lp = fim - timedelta(days=7)
    elif periodo == "Este Mês":
        inicio = hoje.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        fim = hoje
        inicio_lp = (inicio - timedelta(days=1)).replace(day=1)
        fim_lp = inicio - timedelta(days=1)
    elif periodo == "Customizado":
        inicio = None
        fim = None
        inicio_lp = None
        fim_lp = None
    return inicio, fim, inicio_lp, fim_lp

def consultar_dados_acumulados(loja, data_inicio, data_fim):
    session = Session()
    try:
        vendas = session.query(SaleData.data, func.sum(SaleData.valor_venda_com_iva).label('total_vendas_com_iva')).filter(
            SaleData.loja == loja,
            SaleData.data >= data_inicio,
            SaleData.data <= data_fim,
            ~SaleData.item.in_(itens_desconsiderados)
        ).group_by(SaleData.data).all()

        entradas = session.query(PeopleCountingData.start_time.label('data'), func.sum(PeopleCountingData.line1_in + PeopleCountingData.line2_in + PeopleCountingData.line3_in).label('visitantes')).filter(
            PeopleCountingData.loja == loja,
            PeopleCountingData.start_time >= data_inicio,
            PeopleCountingData.end_time <= data_fim
        ).group_by(PeopleCountingData.start_time).all()

        transacoes = session.query(SaleData.data, func.count(distinct(SaleData.referencia_documento)).label('transacoes_vendas')).filter(
            SaleData.loja == loja,
            SaleData.data >= data_inicio,
            SaleData.data <= data_fim,
            SaleData.valor_venda_sem_iva >= 0,
            ~SaleData.item.in_(itens_desconsiderados)
        ).group_by(SaleData.data).all()

        vendas_dict = {venda.data: venda.total_vendas_com_iva for venda in vendas}
        entradas_dict = {entrada.data: entrada.visitantes for entrada in entradas}
        transacoes_dict = {transacao.data: transacao.transacoes_vendas for transacao in transacoes}

        datas = sorted(set(vendas_dict.keys()).union(entradas_dict.keys()).union(transacoes_dict.keys()))

        dados_acumulados = []
        for data in datas:
            dados_acumulados.append({
                'data': data.strftime('%Y-%m-%d'),
                'total_vendas_com_iva': vendas_dict.get(data, 0),
                'visitantes': entradas_dict.get(data, 0),
                'transacoes_vendas': transacoes_dict.get(data, 0)
            })

        return dados_acumulados
    except Exception as e:
        logger.error(f"Erro ao consultar dados acumulados: {str(e)}", exc_info=True)
        return []
    finally:
        session.close()

def main():
    loja = "OML03-Omnia Norteshopping"
    data_inicio, data_fim, _, _ = obter_datas("Hoje")

    if loja in groups:
        resultados = consultar_dados_acumulados_por_grupo(loja, data_inicio, data_fim, data_inicio - timedelta(days=7), data_fim - timedelta(days=7))
        resultados_atuais = resultados['resultados_atuais']
        resultados_anteriores = resultados['resultados_anteriores']
    else:
        resultados_atuais = consultar_vendas([loja], data_inicio, data_fim)
        resultados_anteriores = consultar_vendas([loja], data_inicio - timedelta(days=7), data_fim - timedelta(days=7))

    # Logar resultados anteriores para debug
    logger.info(f"Resultados anteriores para a loja {loja} de {data_inicio - timedelta(days=7)} a {data_fim - timedelta(days=7)}: {resultados_anteriores}")

    resultados_atuais['loja'] = loja
    resultados_atuais['data_inicio'] = data_inicio
    resultados_atuais['data_fim'] = data_fim

    armazenar_resultados_analiticos(resultados_atuais)

if __name__ == "__main__":
    main()
