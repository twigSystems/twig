def filtrar_dados_por_loja(result_set, loja_escolhida):
    """
    Filter sales data by store.

    Parameters:
    result_set (list): List of sales data
    loja_escolhida (str): Chosen store

    Returns:
    list: Filtered sales data
    """
    return [record for record in result_set if loja_escolhida == "Todas as Lojas" or record['Loja'] == loja_escolhida]
