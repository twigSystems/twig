import logging

def calcular_valor_venda(dados, excluidos, tipo_venda):
    total = 0
    for item in dados:
        if item.item not in excluidos:
            valor = getattr(item, f"valor_venda_{tipo_venda}")
            total += valor
    return total

def calcular_valor_venda_com_iva(dados, excluidos):
    return calcular_valor_venda(dados, excluidos, 'com_iva')

def calcular_valor_venda_sem_iva(dados, excluidos):
    return calcular_valor_venda(dados, excluidos, 'sem_iva')

def calcular_total_transacoes(dados):
    return len(set(item.referencia_documento for item in dados if item.tipo_documento != "Nota Crédito"))

def calcular_unidades(dados, excluidos, condicao):
    return sum(condicao(item) for item in dados if item.item not in excluidos)

def calcular_unidades_vendidas(dados, excluidos):
    return calcular_unidades(dados, excluidos, lambda item: item.quantidade)

def calcular_valor_devolucoes_sem_iva(dados, excluidos):
    return calcular_unidades(dados, excluidos, lambda item: abs(item.valor_venda_sem_iva) if item.valor_venda_sem_iva < 0 else 0)

def calcular_unidades_devolvidas(dados, excluidos):
    return calcular_unidades(dados, excluidos, lambda item: abs(item.quantidade) if item.quantidade < 0 else 0)

def calcular_unidades_por_transacao(unidades_vendidas, total_transacoes):
    return unidades_vendidas / total_transacoes if total_transacoes else 0

def calcular_ticket_medio_sem_iva(valor_venda_sem_iva, total_transacoes):
    return valor_venda_sem_iva / total_transacoes if total_transacoes else 0

def calcular_indice_devolucoes(valor_devolucoes_sem_iva, valor_venda_sem_iva):
    return (valor_devolucoes_sem_iva / valor_venda_sem_iva) * 100 if valor_venda_sem_iva else 0

def calcular_taxa_conversao(total_transacoes, total_entradas):
    if total_entradas == 0:
        return 0
    return (total_transacoes / total_entradas) * 100

def calcular_tempo_medio_permanencia(total_permanence, total_entradas):
    if total_entradas == 0:
        return 0
    return total_permanence / total_entradas

def calcular_variacao_percentual(valor_atual, valor_lp):
    if valor_lp == 0:
        return 0
    return ((valor_atual - valor_lp) / valor_lp) * 100

def obter_icone_variacao(variacao, devolucao=False):
    if devolucao:
        if variacao <= 0:
            return "▲"
        elif 0 > variacao > -5:
            return "▶"
        else:
            return "▼"
    else:
        if variacao > 5:
            return "▲"
        elif -5 <= variacao <= 5:
            return "▶"
        else:
            return "▼"

def formatar_variacao_com_icone(valor, variacao, devolucao=False):
    icone = obter_icone_variacao(variacao, devolucao)
    return f"{valor:.2f} | {icone} {variacao:.2f}%"

def calcular_ocupacao_por_regiao(df, df_lp):
    ocupacao = ""
    if not df or not df_lp:
        return ocupacao

    regions = [col for col in df.columns if col.startswith('region')]
    total_sum = df[regions].sum().sum()
    total_sum_lp = df_lp[regions].sum().sum()
    for region in regions:
        region_sum = df[region].sum()
        region_sum_lp = df_lp[region].sum()
        percent_ocupacao = (region_sum / total_sum) * 100 if total_sum > 0 else 0
        percent_ocupacao_lp = (region_sum_lp / total_sum_lp) * 100 if total_sum_lp > 0 else 0
        variacao_ocupacao = calcular_variacao_percentual(percent_ocupacao, percent_ocupacao_lp)
        ocupacao += (
            f"**{region}**: {percent_ocupacao:.2f}% {formatar_variacao_com_icone(variacao_ocupacao, True)}\n"
        )
    return ocupacao

def calcular_indicadores(dados, dados_lp, loja, periodo, total_entradas, total_entradas_lp, total_permanence, total_permanence_lp, regional_df, regional_df_lp, excluidos, logger):
    valor_venda_sem_iva = calcular_valor_venda_sem_iva(dados, excluidos)
    total_transacoes = calcular_total_transacoes(dados)
    unidades_vendidas = calcular_unidades_vendidas(dados, excluidos)
    valor_devolucoes_sem_iva = calcular_valor_devolucoes_sem_iva(dados, excluidos)
    unidades_devolvidas = calcular_unidades_devolvidas(dados, excluidos)
    unidades_por_transacao = calcular_unidades_por_transacao(unidades_vendidas, total_transacoes)
    ticket_medio_sem_iva = calcular_ticket_medio_sem_iva(valor_venda_sem_iva, total_transacoes)
    indice_devolucoes = calcular_indice_devolucoes(valor_devolucoes_sem_iva, valor_venda_sem_iva)
    taxa_conversao = calcular_taxa_conversao(total_transacoes, total_entradas)
    tempo_medio_permanencia = calcular_tempo_medio_permanencia(total_permanence, total_entradas)

    valor_venda_sem_iva_lp = calcular_valor_venda_sem_iva(dados_lp, excluidos)
    total_transacoes_lp = calcular_total_transacoes(dados_lp)
    unidades_vendidas_lp = calcular_unidades_vendidas(dados_lp, excluidos)
    valor_devolucoes_sem_iva_lp = calcular_valor_devolucoes_sem_iva(dados_lp, excluidos)
    unidades_devolvidas_lp = calcular_unidades_devolvidas(dados_lp, excluidos)
    unidades_por_transacao_lp = calcular_unidades_por_transacao(unidades_vendidas_lp, total_transacoes_lp)
    ticket_medio_sem_iva_lp = calcular_ticket_medio_sem_iva(valor_venda_sem_iva_lp, total_transacoes_lp)
    indice_devolucoes_lp = calcular_indice_devolucoes(valor_devolucoes_sem_iva_lp, valor_venda_sem_iva_lp)
    taxa_conversao_lp = calcular_taxa_conversao(total_transacoes_lp, total_entradas_lp)
    tempo_medio_permanencia_lp = calcular_tempo_medio_permanencia(total_permanence_lp, total_entradas_lp)

    variacao_valor_venda_sem_iva = calcular_variacao_percentual(valor_venda_sem_iva, valor_venda_sem_iva_lp)
    variacao_total_transacoes = calcular_variacao_percentual(total_transacoes, total_transacoes_lp)
    variacao_unidades_vendidas = calcular_variacao_percentual(unidades_vendidas, unidades_vendidas_lp)
    variacao_valor_devolucoes_sem_iva = calcular_variacao_percentual(valor_devolucoes_sem_iva, valor_devolucoes_sem_iva_lp)
    variacao_unidades_devolvidas = calcular_variacao_percentual(unidades_devolvidas, unidades_devolvidas_lp)
    variacao_unidades_por_transacao = calcular_variacao_percentual(unidades_por_transacao, unidades_por_transacao_lp)
    variacao_ticket_medio_sem_iva = calcular_variacao_percentual(ticket_medio_sem_iva, ticket_medio_sem_iva_lp)
    variacao_indice_devolucoes = calcular_variacao_percentual(indice_devolucoes, indice_devolucoes_lp)
    variacao_taxa_conversao = calcular_variacao_percentual(taxa_conversao, taxa_conversao_lp)
    variacao_total_entradas = calcular_variacao_percentual(total_entradas, total_entradas_lp)
    variacao_tempo_medio_permanencia = calcular_variacao_percentual(tempo_medio_permanencia, tempo_medio_permanencia_lp)

    # Calcular ocupação por região se os dados regionais forem fornecidos
    ocupacao_por_regiao = calcular_ocupacao_por_regiao(regional_df, regional_df_lp) if regional_df and regional_df_lp else ""

    indicadores = {
        "valor_venda_sem_iva": valor_venda_sem_iva,
        "total_transacoes": total_transacoes,
        "unidades_vendidas": unidades_vendidas,
        "valor_devolucoes_sem_iva": valor_devolucoes_sem_iva,
        "unidades_devolvidas": unidades_devolvidas,
        "unidades_por_transacao": unidades_por_transacao,
        "ticket_medio_sem_iva": ticket_medio_sem_iva,
        "indice_devolucoes": indice_devolucoes,
        "taxa_conversao": taxa_conversao,
        "tempo_medio_permanencia": tempo_medio_permanencia,
        "total_entradas": total_entradas,
        "ocupacao_por_regiao": ocupacao_por_regiao
    }

    resposta = (
        f"Relatório de {periodo} para {loja.split('-')[1].strip()}\n\n"
        f"📊 **Desempenho**\n\n"
        f"Taxa de conversão: {formatar_variacao_com_icone(taxa_conversao, variacao_taxa_conversao)}\n"
        f"Venda sem IVA: {formatar_variacao_com_icone(valor_venda_sem_iva, variacao_valor_venda_sem_iva)}\n"
        f"Unidades vendidas: {formatar_variacao_com_icone(unidades_vendidas, variacao_unidades_vendidas)}\n"
        f"Transações: {formatar_variacao_com_icone(total_transacoes, variacao_total_transacoes)}\n"
        f"Total de Entradas: {formatar_variacao_com_icone(total_entradas, variacao_total_entradas)}\n"
        f"Ticket médio sem IVA: {formatar_variacao_com_icone(ticket_medio_sem_iva, variacao_ticket_medio_sem_iva)}\n"
        f"UPT: {formatar_variacao_com_icone(unidades_por_transacao, variacao_unidades_por_transacao)}\n"
        f"Tempo médio de permanência: {formatar_variacao_com_icone(tempo_medio_permanencia, variacao_tempo_medio_permanencia)} min\n\n"
        f"📉 **Devoluções**\n\n"
        f"Índice de devoluções: {formatar_variacao_com_icone(indice_devolucoes, variacao_indice_devolucoes, True)}\n"
        f"Devoluções sem IVA: {formatar_variacao_com_icone(valor_devolucoes_sem_iva, variacao_valor_devolucoes_sem_iva, True)}\n"
        f"Unidades devolvidas: {formatar_variacao_com_icone(unidades_devolvidas, variacao_unidades_devolvidas, True)}\n\n"
        f"📊 **Ocupação por região**\n\n{ocupacao_por_regiao}\n"
    )

    logger.info(f"Resposta gerada: {resposta}")
    return resposta, indicadores
