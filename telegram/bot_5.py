import os
import sys

import aiogram

# Adiciona o diretório principal ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
import asyncio
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand
import json
import random
import string
import matplotlib.pyplot as plt
import re
import io
from aiogram.utils.exceptions import MessageNotModified, InvalidQueryID, TelegramAPIError
from sqlalchemy import create_engine, func, Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd

# Adiciona o diretório principal ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.config import TELEGRAM_TOKEN_QA, stores, DATABASE_URL
from core.analytics_2 import (
    comparar_periodo_anterior,
    obter_datas,
    mostrar_resultados,
    mostrar_resultados_percentual,
    mostrar_resultados_minutos,
    mostrar_resultados_unidades,
    mostrar_resultados_devolucoes,
    mostrar_resultados_descontos,
    calcular_diferenca,
)
from conector.space_heatmap import generate_heatmap

from tasks import processar_dados_pesados

# Função de escape personalizada para o Markdown V2
def escape_md(text):
    escape_chars = r'\_*[]()~>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

# Configuração do logger com timestamp
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

# Configuração do banco de dados
DATABASE_URL = 'sqlite:///c:/projetos/grnl_platform/bot_database.db'
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# Variáveis globais para gerenciar timeouts
interaction_timeouts = {}

# Função para processar tarefas pesadas com Celery
async def handle_heavy_task(message: types.Message):
    print("Recebi o comando /start_heavy_task")
    await message.reply("Tarefa iniciada! Você será notificado quando estiver concluída.")
    
    # Disparar uma tarefa pesada para Celery
    result = processar_dados_pesados.apply_async(args=['param1', 'param2'])
    print(f"Tarefa disparada para Celery com ID: {result.id}")
    
    # Verificar o resultado da tarefa mais tarde
    while not result.ready():
        print("Esperando tarefa concluir...")
        await asyncio.sleep(1)
    
    result_value = result.get()
    print(f"Tarefa concluída com resultado: {result_value}")
    await message.reply(f"Tarefa concluída! Resultado: {result_value}")

class PeopleCountingData(Base):
    __tablename__ = 'people_counting_data'
    id = Column(Integer, primary_key=True)
    loja = Column(String)
    ip = Column(String)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    total_in = Column(Integer)
    line1_in = Column(Integer)
    line2_in = Column(Integer)
    line3_in = Column(Integer)
    line4_in = Column(Integer)

PERIODOS = [
    "Hoje",
    "Ontem",
    "Esta Semana",
    "Este Mês",
    "Customizado"
]

CHAT_ID_FILE = 'last_chat_id.txt'
USER_DATA_FILE = 'user_data.json'
INVITES_FILE = 'invites.json'
ALTERATION_CODES_FILE = 'alteration_codes.json'
SUPER_ADMIN_FILE = 'super_admin.json'
user_states = {}
user_data = {}
invites = {}
alteration_codes = {}
super_admin = {}

def initialize_json_file(file_path, default_content):
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        with open(file_path, 'w') as file:
            json.dump(default_content, file, indent=4)

# Inicialize os arquivos JSON
initialize_json_file(USER_DATA_FILE, {})
initialize_json_file(INVITES_FILE, {})
initialize_json_file(ALTERATION_CODES_FILE, {})
initialize_json_file(SUPER_ADMIN_FILE, {"chat_id": "", "username": ""})

def save_chat_id(chat_id):
    with open(CHAT_ID_FILE, 'w') as file:
        file.write(str(chat_id))

def get_last_chat_id():
    if os.path.exists(CHAT_ID_FILE):
        with open(CHAT_ID_FILE, 'r') as file:
            return file.read().strip()
    return None

def save_user_data():
    with open(USER_DATA_FILE, 'w') as file:
        json.dump(user_data, file, indent=4)

def load_user_data():
    global user_data
    with open(USER_DATA_FILE, 'r') as file:
        user_data = json.load(file)

def save_invites():
    with open(INVITES_FILE, 'w') as file:
        json.dump(invites, file, indent=4)

def load_invites():
    global invites
    with open(INVITES_FILE, 'r') as file:
        invites = json.load(file)

def save_alteration_codes():
    with open(ALTERATION_CODES_FILE, 'w') as file:
        json.dump(alteration_codes, file, indent=4)

def load_alteration_codes():
    global alteration_codes
    with open(ALTERATION_CODES_FILE, 'r') as file:
        alteration_codes = json.load(file)

def save_super_admin(admin_data):
    with open(SUPER_ADMIN_FILE, 'w') as file:
        json.dump(admin_data, file, indent=4)

def load_super_admin():
    global super_admin
    if os.path.exists(SUPER_ADMIN_FILE):
        with open(SUPER_ADMIN_FILE, 'r') as file:
            super_admin = json.load(file)
    else:
        super_admin = {}

    if 'nivel_acesso' not in super_admin:
        super_admin['nivel_acesso'] = 'Super Admin'

def generate_code():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

def get_user_info(message: types.Message):
    username = message.from_user.username if message.from_user.username else "N/A"
    chat_id = message.chat.id
    return f"User: {username}, Chat ID: {chat_id}"

def obter_datas(periodo):
    now = datetime.now()
    if periodo == "Hoje":
        inicio = datetime(now.year, now.month, now.day)
        fim = inicio + timedelta(days=1) - timedelta(seconds=1)
    elif periodo == "Ontem":
        fim = datetime(now.year, now.month, now.day) - timedelta(seconds=1)
        inicio = fim - timedelta(days=1) + timedelta(seconds=1)
    elif periodo == "Esta Semana":
        inicio = datetime(now.year, now.month, now.day) - timedelta(days=now.weekday())
        fim = inicio + timedelta(days=7) - timedelta(seconds=1)
    elif periodo == "Este Mês":
        inicio = datetime(now.year, now.month, 1)
        next_month = inicio.replace(day=28) + timedelta(days=4)
        fim = next_month - timedelta(days=next_month.day)
    else:
        raise ValueError(f"Período desconhecido: {periodo}")
    
    inicio_lp = inicio - timedelta(days=365)
    fim_lp = fim - timedelta(days=365)
    return inicio, fim, inicio_lp, fim_lp

async def cancel_interaction(chat_id):
    if chat_id in user_states:
        del user_states[chat_id]
    if chat_id in interaction_timeouts:
        del interaction_timeouts[chat_id]
    await bot.send_message(chat_id, "⚠️ A interação foi cancelada devido à inatividade.")

async def set_interaction_timeout(chat_id, timeout_seconds=300):
    if chat_id in interaction_timeouts:
        interaction_timeouts[chat_id].cancel()
    interaction_timeouts[chat_id] = asyncio.create_task(asyncio.sleep(timeout_seconds))
    await interaction_timeouts[chat_id]
    await cancel_interaction(chat_id)

async def send_welcome(message: types.Message):
    chat_id = message.chat.id
    nome_utilizador = message.from_user.first_name if message.from_user.first_name else "Utilizador"
    if str(chat_id) not in user_data and str(chat_id) != str(super_admin.get("chat_id")):
        await message.reply(f"🎉 Bem-vindo ao Assistente de Vendas! Parece que é novo por aqui. Por favor, utilize /registo para se registar.")
    else:
        await message.reply(f"🎉 Bem-vindo de volta! Utilize /consultar para começar uma consulta de vendas ou /help para ver as instruções.")
    logger.info(f"Comando /start recebido de {get_user_info(message)}")

async def send_help(message: types.Message):
    try:
        help_text = """
🆘 Aqui estão as instruções:

Utilize /consultar para iniciar uma consulta de vendas.
Selecione a loja desejada.
Selecione o período desejado.
Receba os indicadores e estatísticas!
🔑 Funções Adicionais:

/gerarconvite - Gera um convite para novos utilizadores (Apenas para níveis Admin e superiores).
/apagarutilizador - Remove um utilizador do sistema (Apenas para níveis Admin e superiores).
/listarusuarios - Lista todos os utilizadores registados (Apenas para níveis Admin e superiores).
/registo - Regista um novo utilizador usando um código de convite.
/funcoes - Lista todas as funções disponíveis para o seu nível de acesso.
/alterarnivel - Altera o nível de acesso de um utilizador existente (Apenas para níveis Admin e superiores).
/usarcodigo - Usa um código para alterar o seu nível de acesso.
/exportardados - Exporta os dados selecionados para um arquivo Excel.
📊 Indicadores Explicados

Taxa de conversão: Percentagem de visitas que resultaram em vendas. Calculada como (Total de transações / Total de entradas) * 100.
Total de Vendas (s/ IVA): Total das vendas sem o imposto.
Total de Vendas (c/ IVA): Total das vendas com o imposto.
Transações: Número total de vendas realizadas.
Visitantes: Número total de pessoas que entraram na loja.
Ticket médio (s/ IVA): Valor médio das vendas, sem impostos. Calculado como (Venda sem IVA / Total de transações).
Ticket médio (c/ IVA): Valor médio das vendas, com impostos. Calculado como (Venda com IVA / Total de transações).
Unidades por transação: Número médio de unidades vendidas por transação.
Tempo médio de permanência: Tempo médio que os clientes passam na loja.
Número de Passagens: Total de passagens pela frente da loja.
Entry Rate: Percentagem de visitantes em relação ao número total de passagens pela frente da loja. Calculado como (Visitantes / Total de Passagens) * 100.
Índice de devoluções: Percentagem do valor devolvido em relação às vendas. Calculado como (Valor de devoluções / Venda sem IVA) * 100.
Índice de descontos: Percentagem do valor descontado em relação às vendas. Calculado como (Valor de descontos / Venda com IVA) * 100.
📉 Variações
As variações são calculadas comparando o período atual com o período anterior correspondente. A variação percentual é calculada como ((valor atual - valor anterior) / valor anterior) * 100.
        """
        await message.reply(help_text)
        logger.info(f"Comando /help recebido de {get_user_info(message)}")
    except Exception as e:
        logger.error(f"Erro ao enviar mensagem de ajuda: {str(e)}", exc_info=True)
        await message.reply(f"Erro ao enviar mensagem de ajuda: {str(e)}")

async def listar_funcoes(message: types.Message):
    try:
        chat_id = message.chat.id
        user_info = user_data.get(str(chat_id), super_admin)
        nome_utilizador = message.from_user.first_name if message.from_user.first_name else "Utilizador"

        nivel_acesso = user_info.get('nivel_acesso', 'Indefinido')

        comandos = {
            "Super Admin": ["/consultar", "/gerarconvite", "/apagarutilizador", "/listarusuarios", "/alterarnivel", "/help", "/funcoes", "/usarcodigo", "/exportardados"],
            "Admin": ["/consultar", "/gerarconvite", "/apagarutilizador", "/listarusuarios", "/alterarnivel", "/help", "/funcoes", "/usarcodigo", "/exportardados"],
            "Geral": ["/consultar", "/help", "/funcoes", "/usarcodigo"],
            "Gestor de Grupo": ["/consultar", "/gerarconvite", "/listarusuarios", "/alterarnivel", "/help", "/funcoes", "/usarcodigo", "/exportardados"],
            "Gestor de Loja": ["/consultar", "/gerarconvite", "/listarusuarios", "/alterarnivel", "/help", "/funcoes", "/usarcodigo", "/exportardados"],
            "Lojista": ["/consultar", "/help", "/funcoes", "/usarcodigo"]
        }

        comandos_usuario = comandos.get(nivel_acesso, ["/help"])

        resposta = f"📜 Aqui estão os comandos disponíveis para si, {nome_utilizador}:\n"
        for comando in comandos_usuario:
            resposta += f"{comando}\n"

        await message.reply(resposta)
    except Exception as e:
        logger.error(f"Erro ao listar funções: {str(e)}", exc_info=True)
        await message.reply(f"Erro ao listar funções: {str(e)}")

async def registo(message: types.Message):
    try:
        chat_id = message.chat.id
        nome_utilizador = message.from_user.first_name if message.from_user.first_name else "Utilizador"
        if str(chat_id) in user_data:
            await message.reply(f"✅ Já está registado! Utilize /consultar para começar uma consulta de vendas ou /help para ver as instruções.")
            return

        user_states[chat_id] = {'step': 'codigo_convite'}
        await message.reply("🔑 Por favor, insira o código de convite:")
    except Exception as e:
        logger.error(f"Erro ao iniciar registro: {str(e)}", exc_info=True)
        await message.reply(f"Erro ao iniciar registro: {str(e)}")

async def processar_codigo_convite(message: types.Message):
    try:
        chat_id = message.chat.id
        codigo_convite = message.text
        nome_utilizador = message.from_user.first_name if message.from_user.first_name else "Utilizador"
        if chat_id in user_states and user_states[chat_id]['step'] == 'codigo_convite':
            if codigo_convite not in invites:
                await message.reply("❌ Código de convite inválido. Tente novamente.")
                return

            invite_info = invites.pop(codigo_convite)
            save_invites()

            nivel_acesso = invite_info['nivel_acesso']
            grupo = invite_info.get('grupo', 'Todos')
            loja = invite_info.get('loja', 'Todas')

            user_data[str(chat_id)] = {
                'nivel_acesso': nivel_acesso,
                'grupo': grupo,
                'loja': loja,
                'username': message.from_user.username if message.from_user.username else message.from_user.first_name
            }
            save_user_data()

            await message.reply(f"🎉 Registo concluído! Agora tem acesso ao nível {nivel_acesso}. Utilize /consultar para começar.")
            logger.info(f"Usuário registrado: {get_user_info(message)}, Nível de Acesso: {nivel_acesso}, Grupo: {grupo}, Loja: {loja}")
    except Exception as e:
        logger.error(f"Erro ao processar código de convite: {str(e)}", exc_info=True)
        await message.reply(f"Erro ao processar código de convite: {str(e)}")

async def gerar_convite(message: types.Message):
    try:
        chat_id = message.chat.id
        nome_utilizador = message.from_user.first_name if message.from_user.first_name else "Utilizador"
        if str(chat_id) not in user_data and str(chat_id) != str(super_admin.get("chat_id")):
            await message.reply("🚫 Apenas Super Admins, Admins e gestores podem gerar convites.")
            return

        user_info = user_data.get(str(chat_id), super_admin)
        if user_info['nivel_acesso'] == 'Super Admin':
            niveis_acesso = ["Admin", "Geral", "Gestor de Grupo", "Gestor de Loja", "Lojista"]
        elif user_info['nivel_acesso'] == 'Admin':
            niveis_acesso = ["Geral", "Gestor de Grupo", "Gestor de Loja", "Lojista"]
        elif user_info['nivel_acesso'] == 'Gestor de Grupo':
            niveis_acesso = ["Gestor de Loja", "Lojista"]
        elif user_info['nivel_acesso'] == 'Gestor de Loja':
            niveis_acesso = ["Lojista"]
        
        user_states[chat_id] = {'step': 'nivel_acesso_convite'}
        markup = InlineKeyboardMarkup()
        for nivel in niveis_acesso:
            markup.add(InlineKeyboardButton(nivel, callback_data=f"nivel_acesso_convite:{nivel}"))
        markup.add(InlineKeyboardButton("Saír", callback_data="sair"))
        await message.reply("📜 Selecione o nível de acesso para o convite:", reply_markup=markup)
    except Exception as e:
        logger.error(f"Erro ao iniciar geração de convite: {str(e)}", exc_info=True)
        await message.reply(f"Erro ao iniciar geração de convite: {str(e)}")

async def gerar_codigo_alteracao(message: types.Message):
    try:
        chat_id = message.chat.id
        nome_utilizador = message.from_user.first_name if message.from_user.first_name else "Utilizador"
        if str(chat_id) not in user_data and str(chat_id) != str(super_admin.get("chat_id")):
            await message.reply("🚫 Apenas Admins podem gerar códigos de alteração de nível de acesso.")
            return

        user_info = user_data.get(str(chat_id), super_admin)
        if user_info['nivel_acesso'] not in ['Super Admin', 'Admin']:
            await message.reply("🚫 Você não tem permissão para gerar códigos de alteração de nível de acesso.")
            return

        user_states[chat_id] = {'step': 'nivel_acesso_alteracao'}
        markup = InlineKeyboardMarkup()
        niveis_acesso = ["Admin", "Geral", "Gestor de Grupo", "Gestor de Loja", "Lojista"]
        for nivel in niveis_acesso:
            markup.add(InlineKeyboardButton(nivel, callback_data=f"nivel_acesso_alteracao:{nivel}"))
        markup.add(InlineKeyboardButton("Saír", callback_data="sair"))
        await message.reply("📜 Selecione o nível de acesso para o código de alteração:", reply_markup=markup)
    except Exception as e:
        logger.error(f"Erro ao iniciar geração de código de alteração: {str(e)}", exc_info=True)
        await message.reply(f"Erro ao iniciar geração de código de alteração: {str(e)}")

async def processar_nivel_acesso_alteracao(call: types.CallbackQuery):
    try:
        chat_id = call.message.chat.id
        nivel_acesso = call.data.split(":")[1]
        codigo_alteracao = generate_code()
        alteration_codes[codigo_alteracao] = {'nivel_acesso': nivel_acesso}
        save_alteration_codes()
        await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        await call.message.reply(f"🔑 Código de alteração gerado! Envie este código para o utilizador:\n\n{codigo_alteracao}\n\n[Link para o bot](https://t.me/MainfashionBot)")
        logger.info(f"Código de alteração gerado: {codigo_alteracao} para acesso {nivel_acesso}")
        await call.answer()
    except Exception as e:
        logger.error(f"Erro ao processar nível de acesso para código de alteração: {str(e)}", exc_info=True)
        await call.message.reply(f"Erro ao processar nível de acesso para código de alteração: {str(e)}")

async def usarcodigo(message: types.Message):
    try:
        chat_id = message.chat.id
        nome_utilizador = message.from_user.first_name if message.from_user.first_name else "Utilizador"
        if str(chat_id) not in user_data and str(chat_id) != str(super_admin.get("chat_id")):
            await message.reply("❌ Você não está registado. Use /registo para se registar.")
            return

        user_states[chat_id] = {'step': 'codigo_alteracao'}
        await message.reply("🔑 Por favor, insira o código de alteração de nível de acesso:")
    except Exception as e:
        logger.error(f"Erro ao iniciar uso de código de alteração: {str(e)}", exc_info=True)
        await message.reply(f"Erro ao iniciar uso de código de alteração: {str(e)}")

async def processar_codigo_alteracao(message: types.Message):
    try:
        chat_id = message.chat.id
        codigo_alteracao = message.text
        nome_utilizador = message.from_user.first_name if message.from_user.first_name else "Utilizador"
        if chat_id in user_states and user_states[chat_id]['step'] == 'codigo_alteracao':
            if codigo_alteracao not in alteration_codes:
                await message.reply("❌ Código de alteração inválido. Tente novamente.")
                return

            alteration_info = alteration_codes.pop(codigo_alteracao)
            save_alteration_codes()

            nivel_acesso = alteration_info['nivel_acesso']
            user_data[str(chat_id)]['nivel_acesso'] = nivel_acesso
            save_user_data()

            await message.reply(f"✅ Alteração de nível concluída! Agora tem acesso ao nível {nivel_acesso}.")
            logger.info(f"Utilizador {get_user_info(message)} alterou nível de acesso para: {nivel_acesso}")
    except Exception as e:
        logger.error(f"Erro ao processar código de alteração: {str(e)}", exc_info=True)
        await message.reply(f"Erro ao processar código de alteração: {str(e)}")

async def processar_nivel_acesso_convite(call: types.CallbackQuery):
    try:
        chat_id = call.message.chat.id
        nivel_acesso = call.data.split(":")[1]
        user_states[chat_id] = {'step': 'grupo_convite', 'nivel_acesso': nivel_acesso}
        user_info = user_data.get(str(chat_id), super_admin)

        if nivel_acesso in ["Geral", "Admin"]:
            codigo_convite = generate_code()
            invites[codigo_convite] = {'nivel_acesso': nivel_acesso, 'grupo': 'Todos'}
            save_invites()
            await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
            await call.message.reply(f"🔑 Convite gerado! Envie este código ao utilizador:\n\n{codigo_convite}\n\n[Link para o bot](https://t.me/MainfashionBot)")
            logger.info(f"Convite gerado: {codigo_convite} para acesso {nivel_acesso}")
            await call.answer()
        elif nivel_acesso in ["Gestor de Grupo", "Gestor de Loja", "Lojista"]:
            if user_info['nivel_acesso'] in ['Super Admin', 'Admin'] or (user_info['nivel_acesso'] == 'Gestor de Grupo' and nivel_acesso in ['Gestor de Loja', 'Lojista']) or (user_info['nivel_acesso'] == 'Gestor de Loja' and nivel_acesso == 'Lojista'):
                markup = InlineKeyboardMarkup()
                grupos = ["OMNIA", "ONLY"] if user_info['nivel_acesso'] in ['Super Admin', 'Admin'] else [user_info['grupo']]
                for grupo in grupos:
                    markup.add(InlineKeyboardButton(grupo, callback_data=f"grupo_convite:{grupo}"))
                markup.add(InlineKeyboardButton("Saír", callback_data="sair"))
                await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
                await call.message.reply("🏢 Selecione o grupo de lojas para o convite:", reply_markup=markup)
                await call.answer()
            else:
                await call.message.reply("🚫 Você não tem permissão para gerar convites para esse nível de acesso.")
        else:
            grupo = user_info.get('grupo', 'Indefinido')
            if grupo == 'Indefinido':
                if user_info['nivel_acesso'] in ['Super Admin', 'Admin']:
                    grupo = 'Todos'
                else:
                    await call.message.reply("Erro de configuração: Grupo não definido. Por favor, contate o administrador.")
                    return

            prefixo_grupo = "OML" if grupo == "OMNIA" else "ONL"
            lojas_grupo = {k: v for k, v in stores.items() if k.startswith(prefixo_grupo)}
            user_states[chat_id]['grupo'] = grupo

            markup = InlineKeyboardMarkup()
            for loja in lojas_grupo:
                markup.add(InlineKeyboardButton(loja, callback_data=f"loja_convite:{loja}"))
            markup.add(InlineKeyboardButton("Saír", callback_data="sair"))
            await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
            await call.message.reply("🏬 Selecione a loja para o convite:", reply_markup=markup)
            await call.answer()
    except Exception as e:
        logger.error(f"Erro ao processar nível de acesso para convite: {str(e)}", exc_info=True)
        await call.message.reply(f"Erro ao processar nível de acesso para convite: {str(e)}")

async def processar_grupo_convite(call: types.CallbackQuery):
    try:
        chat_id = call.message.chat.id
        grupo = call.data.split(":")[1].upper()
        user_states[chat_id]['grupo'] = grupo
        nivel_acesso = user_states[chat_id]['nivel_acesso']

        if nivel_acesso == "Gestor de Grupo":
            codigo_convite = generate_code()
            invites[codigo_convite] = {'nivel_acesso': nivel_acesso, 'grupo': grupo}
            save_invites()
            await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
            await call.message.reply(f"🔑 Convite gerado! Envie este código ao utilizador:\n\n{codigo_convite}\n\n[Link para o bot](https://t.me/MainfashionBot)")
            logger.info(f"Convite gerado: {codigo_convite} para grupo {grupo} com nível {nivel_acesso}")
            await call.answer()
        else:
            prefixo_grupo = "OML" if grupo == "OMNIA" else "ONL"
            lojas_grupo = {k: v for k, v in stores.items() if k.startswith(prefixo_grupo)}
            markup = InlineKeyboardMarkup()
            for loja in lojas_grupo:
                markup.add(InlineKeyboardButton(loja, callback_data=f"loja_convite:{loja}"))
            markup.add(InlineKeyboardButton("Saír", callback_data="sair"))
            await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
            await call.message.reply("🏬 Selecione a loja para o convite:", reply_markup=markup)
            await call.answer()
    except Exception as e:
        logger.error(f"Erro ao processar grupo de convite: {str(e)}", exc_info=True)
        await call.message.reply(f"Erro ao processar grupo de convite: {str(e)}")

async def processar_loja_convite(call: types.CallbackQuery):
    try:
        chat_id = call.message.chat.id
        loja = call.data.split(":")[1]
        grupo = user_states[chat_id]['grupo']
        nivel_acesso = user_states[chat_id]['nivel_acesso']
        codigo_convite = generate_code()
        invites[codigo_convite] = {'nivel_acesso': nivel_acesso, 'grupo': grupo, 'loja': loja}
        save_invites()
        await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        await call.message.reply(f"🔑 Convite gerado! Envie este código ao utilizador:\n\n{codigo_convite}\n\n[Link para o bot](https://t.me/MainfashionBot)")
        logger.info(f"Convite gerado: {codigo_convite} para loja {loja} do grupo {grupo} com nível {nivel_acesso}")
        await call.answer()
    except Exception as e:
        logger.error(f"Erro ao processar loja de convite: {str(e)}", exc_info=True)
        await call.message.reply(f"Erro ao processar loja de convite: {str(e)}")

async def apagar_utilizador(message: types.Message):
    try:
        chat_id = message.chat.id
        nome_utilizador = message.from_user.first_name if message.from_user.first_name else "Utilizador"
        if str(chat_id) not in user_data and str(chat_id) != str(super_admin.get("chat_id")):
            await message.reply("🚫 Apenas Super Admins, Admins e gestores podem apagar utilizadores.")
            return

        user_states[chat_id] = {'step': 'apagar_usuario'}
        await message.reply("🗑️ Por favor, insira o Chat ID do utilizador a ser removido:")
    except Exception as e:
        logger.error(f"Erro ao iniciar remoção de utilizador: {str(e)}", exc_info=True)
        await message.reply(f"Erro ao iniciar remoção de utilizador: {str(e)}")

async def processar_apagar_usuario(message: types.Message):
    try:
        chat_id = message.chat.id
        chat_id_remover = message.text
        nome_utilizador = message.from_user.first_name if message.from_user.first_name else "Utilizador"
        if chat_id in user_states and user_states[chat_id]['step'] == 'apagar_usuario':
            if chat_id_remover not in user_data:
                await message.reply("❌ Chat ID não encontrado. Tente novamente.")
                return

            nivel_acesso = user_data.get(str(chat_id), super_admin)['nivel_acesso']
            if nivel_acesso == 'Super Admin' or (nivel_acesso == 'Admin') or (nivel_acesso == 'Gestor de Grupo' and user_data[chat_id_remover]['grupo'] == user_data[str(chat_id)]['grupo']) or (nivel_acesso == 'Gestor de Loja' and user_data[chat_id_remover]['loja'] == user_data[str(chat_id)]['loja']):
                user_data.pop(chat_id_remover)
                save_user_data()
                await message.reply(f"✅ Utilizador {chat_id_remover} removido com sucesso!")
                logger.info(f"Utilizador {chat_id_remover} removido por {get_user_info(message)}")
            else:
                await message.reply("🚫 Você não tem permissão para remover este utilizador.")
    except Exception as e:
        logger.error(f"Erro ao processar remoção de utilizador: {str(e)}", exc_info=True)
        await message.reply(f"Erro ao processar remoção de utilizador: {str(e)}")

async def listar_usuarios(message: types.Message):
    try:
        chat_id = message.chat.id
        nome_utilizador = message.from_user.first_name if message.from_user.first_name else "Utilizador"
        if str(chat_id) not in user_data and str(chat_id) != str(super_admin.get("chat_id")):
            await message.reply("❌ Não está registado. Utilize /registo para se registar.")
            return

        nivel_acesso = user_data.get(str(chat_id), super_admin)['nivel_acesso']
        grupo = user_data.get(str(chat_id), super_admin).get('grupo', 'Todos')
        loja = user_data.get(str(chat_id), super_admin).get('loja', 'Todas')

        if nivel_acesso not in ['Super Admin', 'Admin', 'Gestor de Grupo', 'Gestor de Loja']:
            await message.reply("🚫 Apenas Super Admins, Admins e gestores podem listar utilizadores.")
            return

        resposta = "📜 Utilizadores:\n"
        if nivel_acesso == 'Super Admin' or nivel_acesso == 'Admin':
            for uid, info in user_data.items():
                resposta += f"{uid}: {info['username']} - {info['nivel_acesso']}\n"
        elif nivel_acesso == 'Gestor de Grupo':
            for uid, info in user_data.items():
                if info['grupo'] == grupo:
                    resposta += f"{uid}: {info['username']} - {info['nivel_acesso']}\n"
        elif nivel_acesso == 'Gestor de Loja':
            for uid, info in user_data.items():
                if info['loja'] == loja:
                    resposta += f"{uid}: {info['username']} - {info['nivel_acesso']}\n"
        
        await message.reply(resposta)
    except Exception as e:
        logger.error(f"Erro ao listar utilizadores: {str(e)}", exc_info=True)
        await message.reply(f"Erro ao listar utilizadores: {str(e)}")

async def exportardados(message: types.Message):
    chat_id = message.chat.id
    nome_utilizador = message.from_user.first_name if message.from_user.first_name else "Utilizador"
    if str(chat_id) not in user_data and str(chat_id) != str(super_admin.get("chat_id")):
        await message.reply("❌ Não está registado. Utilize /registo para se registar.")
        return

    user_info = get_user_info(message)
    user_record = user_data.get(str(chat_id), super_admin)

    if 'nivel_acesso' not in user_record:
        await message.reply("Erro de configuração: Nível de acesso não definido. Por favor, contate o administrador.")
        return

    nivel_acesso = user_record['nivel_acesso']
    grupo = user_record.get('grupo', 'Todos')
    loja = user_record.get('loja', 'Todas')

    if nivel_acesso not in ["Super Admin", "Admin", "Geral"]:
        await message.reply("🚫 Apenas Super Admins, Admins e usuários Gerais podem exportar dados.")
        return

    logger.info(f"Comando /exportardados recebido por {user_info}")

    if nivel_acesso in ["Super Admin", "Admin"]:
        markup = InlineKeyboardMarkup()
        grupos = ["OMNIA", "ONLY"]
        for grupo in grupos:
            markup.add(InlineKeyboardButton(grupo, callback_data=f"exportar_grupo:{grupo}"))
        markup.add(InlineKeyboardButton("Saír", callback_data="sair"))
        await message.reply("🏢 Selecione o grupo que deseja consultar:", reply_markup=markup)
    else:
        lojas = [loja]
        markup = InlineKeyboardMarkup()
        for loja in lojas:
            markup.add(InlineKeyboardButton(loja, callback_data=f"exportar_loja:{loja}"))
        markup.add(InlineKeyboardButton("Saír", callback_data="sair"))
        await message.reply("🏬 Selecione a loja que deseja consultar:", reply_markup=markup)

async def process_exportar_grupo(call: types.CallbackQuery):
    try:
        user_info = get_user_info(call.message)
        grupo = call.data.split(":")[1]
        await call.answer(f"Grupo selecionado: {grupo} ✅")
        logger.info(f"Grupo selecionado: {grupo} por {user_info}")

        await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        await bot.send_message(call.message.chat.id, f"Grupo {grupo} selecionado! ✅", reply_to_message_id=call.message.message_id)

        prefixo_grupo = "OML" if grupo == "OMNIA" else "ONL"
        lojas = [loja for loja in stores.keys() if loja.startswith(prefixo_grupo)]

        markup = InlineKeyboardMarkup()
        for loja in lojas:
            markup.add(InlineKeyboardButton(loja, callback_data=f"exportar_loja:{loja}"))
        markup.add(InlineKeyboardButton("Saír", callback_data="sair"))
        await bot.send_message(call.message.chat.id, "🏬 Selecione a loja que deseja consultar:", reply_markup=markup)
    except Exception as e:
        logger.error(f"Erro ao processar grupo para exportação: {str(e)}", exc_info=True)
        await call.message.reply(f"Erro ao processar grupo para exportação: {str(e)}")

async def process_exportar_loja(call: types.CallbackQuery):
    try:
        user_info = get_user_info(call.message)
        loja = call.data.split(":")[1]
        await call.answer(f"Loja selecionada: {loja} ✅")
        logger.info(f"Loja selecionada: {loja} por {user_info}")

        await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        await bot.send_message(call.message.chat.id, f"Loja {loja} selecionada! ✅", reply_to_message_id=call.message.message_id)

        user_states[call.message.chat.id] = {'loja': loja, 'step': 'data_hora_inicio_exportar'}
        await bot.send_message(call.message.chat.id, "🕒 Insira a data e hora de início para exportação (formato: dd-MM-yyyy HH:00):")
    except Exception as e:
        logger.error(f"Erro ao processar loja para exportação: {str(e)}", exc_info=True)
        await call.message.reply(f"Erro ao processar loja para exportação: {str(e)}")

async def processar_data_hora_inicio_exportar(message: types.Message):
    try:
        user_info = get_user_info(message)
        chat_id = message.chat.id
        if chat_id in user_states and user_states[chat_id]['step'] == 'data_hora_inicio_exportar':
            data_hora_inicio = datetime.strptime(message.text, '%d-%m-%Y %H:00')
            user_states[chat_id]['data_hora_inicio'] = data_hora_inicio
            user_states[chat_id]['step'] = 'data_hora_fim_exportar'
            await message.reply(f"✅ Data e hora de início selecionadas: {data_hora_inicio.strftime('%d-%m-%Y %H:00')} ✅")
            await message.reply("🕒 Insira a data e hora de fim para exportação (formato: dd-MM-yyyy HH:00):")
            logger.info(f"Data e hora de início {data_hora_inicio.strftime('%d-%m-%Y %H:00')} selecionadas para a loja {user_states[chat_id]['loja']} por {user_info}")
    except ValueError:
        await message.reply("❌ Formato de data e hora inválido. Por favor, insira no formato: dd-MM-yyyy HH:00")

async def processar_data_hora_fim_exportar(message: types.Message):
    try:
        user_info = get_user_info(message)
        chat_id = message.chat.id
        if chat_id in user_states and user_states[chat_id]['step'] == 'data_hora_fim_exportar':
            data_hora_fim = datetime.strptime(message.text, '%d-%m-%Y %H:00') - timedelta(seconds=1)
            data_hora_inicio = user_states[chat_id]['data_hora_inicio']
            loja = user_states[chat_id]['loja']
            user_states[chat_id]['data_hora_fim'] = data_hora_fim
            await message.reply(f"✅ Data e hora de fim selecionadas: {data_hora_fim.strftime('%d-%m-%Y %H:%M:%S')} ✅")
            await exportar_dados(message, loja, data_hora_inicio, data_hora_fim)
            logger.info(f"Data e hora de fim {data_hora_fim.strftime('%d-%m-%Y %H:%M:%S')} selecionadas para a loja {loja} por {user_info}")
    except ValueError:
        await message.reply("❌ Formato de data e hora inválido. Por favor, insira no formato: dd-MM-yyyy HH:00")

async def exportar_dados(message: types.Message, loja, inicio, fim):
    try:
        session = Session()
        dados = session.query(
            func.date(PeopleCountingData.start_time).label('data'),
            func.sum(PeopleCountingData.line1_in + PeopleCountingData.line2_in + PeopleCountingData.line3_in).label('total_in')
        ).filter(
            PeopleCountingData.loja == loja,
            PeopleCountingData.start_time >= inicio,
            PeopleCountingData.end_time <= fim
        ).group_by(
            func.date(PeopleCountingData.start_time)
        ).all()
        
        resultados = []
        for dado in dados:
            resultados.append({
                'Data': dado.data,
                'Total In': dado.total_in
            })

        df = pd.DataFrame(resultados)
        nome_arquivo = f'people_counting_{loja}_{inicio.date()}_to_{fim.date()}.xlsx'
        df.to_excel(nome_arquivo, index=False)
        
        with open(nome_arquivo, 'rb') as arquivo:
            await bot.send_document(message.chat.id, arquivo)
        os.remove(nome_arquivo)
        await bot.send_message(message.chat.id, "✅ Exportação concluída com sucesso!")
        logger.info(f"Arquivo {nome_arquivo} gerado e enviado com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao exportar dados: {str(e)}", exc_info=True)
        await message.reply(f"Erro ao exportar dados: {str(e)}")
    finally:
        session.close()

async def consultar(message: types.Message):
    try:
        chat_id = message.chat.id
        nome_utilizador = message.from_user.first_name if message.from_user.first_name else "Utilizador"
        if str(chat_id) not in user_data and str(chat_id) != str(super_admin.get("chat_id")):
            await message.reply("❌ Não está registado. Utilize /registo para se registar.")
            return
        
        user_info = get_user_info(message)
        user_record = user_data.get(str(chat_id), super_admin)
        
        if 'nivel_acesso' not in user_record:
            await message.reply("Erro de configuração: Nível de acesso não definido. Por favor, contate o administrador.")
            return
        
        nivel_acesso = user_record['nivel_acesso']
        grupo = user_record.get('grupo', 'Todos')
        loja = user_record.get('loja', 'Todas')

        logger.info(f"Comando /consultar recebido por {user_info}")

        if nivel_acesso in ["Super Admin", "Admin", "Geral"]:
            markup = InlineKeyboardMarkup()
            grupos = ["OMNIA", "ONLY"]
            for grupo in grupos:
                markup.add(InlineKeyboardButton(grupo, callback_data=f"consultar_grupo:{grupo}"))
            markup.add(InlineKeyboardButton("Saír", callback_data="sair"))
            await message.reply("🏢 Selecione o grupo que deseja consultar:", reply_markup=markup)
        elif nivel_acesso == "Gestor de Grupo":
            prefixo_grupo = "OML" if grupo == "OMNIA" else "ONL"
            lojas = [loja for loja in stores.keys() if loja.startswith(prefixo_grupo)]
            markup = InlineKeyboardMarkup()
            for loja in lojas:
                markup.add(InlineKeyboardButton(loja, callback_data=f"consultar_loja:{loja}"))
            markup.add(InlineKeyboardButton("Saír", callback_data="sair"))
            await message.reply("🏬 Selecione a loja que deseja consultar:", reply_markup=markup)
        else:
            lojas = [loja]
            markup = InlineKeyboardMarkup()
            for loja in lojas:
                markup.add(InlineKeyboardButton(loja, callback_data=f"consultar_loja:{loja}"))
            markup.add(InlineKeyboardButton("Saír", callback_data="sair"))
            await message.reply("🏬 Selecione a loja que deseja consultar:", reply_markup=markup)
    except Exception as e:
        logger.error(f"Erro ao configurar consulta: {str(e)}", exc_info=True)
        await message.reply(f"Erro ao configurar consulta: {str(e)}")

async def process_consultar_grupo(call: types.CallbackQuery):
    try:
        user_info = get_user_info(call.message)
        grupo = call.data.split(":")[1]

        try:
            await call.answer(f"Grupo selecionado: {grupo} ✅", show_alert=False)
        except InvalidQueryID:
            logger.warning("Query ID inválido ou timeout expirado ao responder à callback query.")

        logger.info(f"Grupo selecionado: {grupo} por {user_info}")

        await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        await bot.send_message(call.message.chat.id, f"Grupo {grupo} selecionado! ✅", reply_to_message_id=call.message.message_id)

        prefixo_grupo = "OML" if grupo == "OMNIA" else "ONL"
        lojas = [loja for loja in stores.keys() if loja.startswith(prefixo_grupo)]

        markup = InlineKeyboardMarkup()
        for loja in lojas:
            markup.add(InlineKeyboardButton(loja, callback_data=f"consultar_loja:{loja}"))
        markup.add(InlineKeyboardButton("Saír", callback_data="sair"))
        await bot.send_message(call.message.chat.id, "🏬 Selecione a loja que deseja consultar:", reply_markup=markup)
        
        # Iniciar timeout
        asyncio.create_task(set_interaction_timeout(call.message.chat.id))

    except Exception as e:
        logger.error(f"Erro ao processar grupo para consulta: {str(e)}", exc_info=True)
        await call.message.reply(f"Erro ao processar grupo para consulta: {str(e)}")

async def process_consultar_loja(call: types.CallbackQuery):
    try:
        user_info = get_user_info(call.message)
        loja = call.data.split(":")[1]

        try:
            await call.answer(f"Loja selecionada: {loja} ✅", show_alert=False)
        except InvalidQueryID:
            logger.warning("Query ID inválido ou timeout expirado ao responder à callback query.")

        logger.info(f"Loja selecionada: {loja} por {user_info}")

        await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        await bot.send_message(call.message.chat.id, f"Loja {loja} selecionada! ✅", reply_to_message_id=call.message.message_id)

        markup = InlineKeyboardMarkup()
        for periodo in PERIODOS:
            markup.add(InlineKeyboardButton(periodo, callback_data=f"periodo:{loja}:{periodo}"))
        markup.add(InlineKeyboardButton("Saír", callback_data="sair"))
        await bot.send_message(call.message.chat.id, "📅 Selecione o período que deseja consultar:", reply_markup=markup)
        
        # Iniciar timeout
        asyncio.create_task(set_interaction_timeout(call.message.chat.id))

    except Exception as e:
        logger.error(f"Erro ao processar loja para consulta: {str(e)}", exc_info=True)
        await call.message.reply(f"Erro ao processar loja para consulta: {str(e)}")

async def process_periodo_step(call: types.CallbackQuery):
    dados_periodo = call.data.split(":")
    loja = dados_periodo[1]
    periodo = dados_periodo[2]
    user_info = get_user_info(call.message)

    await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
    await bot.send_message(call.message.chat.id, f"Período {periodo} selecionado! ✅", reply_to_message_id=call.message.message_id)

    if periodo == "Customizado":
        user_states[call.message.chat.id] = {'loja': loja, 'step': 'data_hora_inicio'}
        await bot.send_message(call.message.chat.id, "🕒 Insira a data e hora de início (formato: dd-MM-yyyy HH:00):")
    else:
        await processar_periodo(call.message, loja, periodo)
        logger.info(f"Período {periodo} selecionado para a loja {loja} por {user_info}")

async def processar_data_hora_inicio(message: types.Message):
    try:
        user_info = get_user_info(message)
        chat_id = message.chat.id
        if chat_id in user_states and user_states[chat_id]['step'] == 'data_hora_inicio':
            data_hora_inicio = datetime.strptime(message.text, '%d-%m-%Y %H:00')
            user_states[chat_id]['data_hora_inicio'] = data_hora_inicio
            user_states[chat_id]['step'] = 'data_hora_fim'
            await message.reply(f"✅ Data e hora de início selecionadas: {data_hora_inicio.strftime('%d-%m-%Y %H:00')} ✅")
            await message.reply("🕒 Insira a data e hora de fim (formato: dd-MM-yyyy HH:00):")
            logger.info(f"Data e hora de início {data_hora_inicio.strftime('%d-%m-%Y %H:00')} selecionadas para a loja {user_states[chat_id]['loja']} por {user_info}")
    except ValueError:
        await message.reply("❌ Formato de data e hora inválido. Por favor, insira no formato: dd-MM-yyyy HH:00")

async def processar_data_hora_fim(message: types.Message):
    try:
        user_info = get_user_info(message)
        chat_id = message.chat.id
        if chat_id in user_states and user_states[chat_id]['step'] == 'data_hora_fim':
            data_hora_fim = datetime.strptime(message.text, '%d-%m-%Y %H:00') - timedelta(seconds=1)
            data_hora_inicio = user_states[chat_id]['data_hora_inicio']
            loja = user_states[chat_id]['loja']
            user_states[chat_id]['data_hora_fim'] = data_hora_fim
            await message.reply(f"✅ Data e hora de fim selecionadas: {data_hora_fim.strftime('%d-%m-%Y %H:%M:%S')} ✅")
            await processar_periodo(message, loja, "Customizado", data_hora_inicio, data_hora_fim)
            logger.info(f"Data e hora de fim {data_hora_fim.strftime('%d-%m-%Y %H:%M:%S')} selecionadas para a loja {loja} por {user_info}")
    except ValueError:
        await message.reply("❌ Formato de data e hora inválido. Por favor, insira no formato: dd-MM-yyyy HH:00")

def mostrar_resultados(atual, anterior, descricao, monetario=False):
    diferenca = calcular_diferenca(atual, anterior)
    direcao = "🟢" if diferenca >= 0 else "🔴"
    valor_atual = f"€{atual:.2f}" if monetario else f"{int(atual)}"
    return f"{descricao}: {valor_atual} | {diferenca:.2f}% {direcao}"

def mostrar_resultados_percentual(atual, anterior, descricao):
    diferenca = calcular_diferenca(atual, anterior)
    direcao = "🟢" if diferenca >= 0 else "🔴"
    valor_atual = f"{atual:.2f}%"
    return f"{descricao}: {valor_atual} | {diferenca:.2f}% {direcao}"

def mostrar_resultados_minutos(atual, anterior, descricao):
    if atual == 0:
        valor_atual = "0 min"
    else:
        valor_atual = f"{atual:.2f} min"
    diferenca = calcular_diferenca(atual, anterior)
    direcao = "🟢" if diferenca >= 0 else "🔴"
    return f"{descricao}: {valor_atual} | {diferenca:.2f}% {direcao}"

def mostrar_resultados_unidades(atual, anterior, descricao):
    diferenca = calcular_diferenca(atual, anterior)
    direcao = "🟢" if diferenca >= 0 else "🔴"
    return f"{descricao}: {atual:.2f} u. | {diferenca:.2f}% {direcao}"

def mostrar_resultados_devolucoes(atual, anterior, descricao):
    diferenca = calcular_diferenca(atual, anterior)
    direcao = "🟢" if diferenca >= 0 else "🔴"
    valor_atual = f"{atual:.2f}%" if isinstance(atual, (int, float)) else "0%"
    return f"{descricao}: {valor_atual} | {diferenca:.2f}% {direcao}"

def mostrar_resultados_descontos(atual, anterior, descricao):
    diferenca = calcular_diferenca(atual, anterior)
    direcao = "🟢" if diferenca >= 0 else "🔴"
    valor_atual = f"{atual:.2f}%" if isinstance(atual, (int, float)) else "0%"
    return f"{descricao}: {valor_atual} | {diferenca:.2f}% {direcao}"

async def processar_periodo(call_or_message, loja, periodo, inicio=None, fim=None):
    mensagem_carregando = None
    try:
        user_info = get_user_info(call_or_message)
        if not inicio or not fim:
            inicio, fim, inicio_lp, fim_lp = obter_datas(periodo)
        else:
            inicio_lp = inicio - timedelta(days=365)
            fim_lp = fim - timedelta(days=365)

        chat_id = call_or_message.chat.id
        message_id = call_or_message.message_id

        logger.info(f"Período selecionado: {periodo} para a loja {loja} por {user_info}")

        try:
            await bot.edit_message_reply_markup(chat_id, message_id, reply_markup=None)
        except:
            logger.warning(f"Não foi possível editar a mensagem: {message_id}")

        mensagem_carregando = await bot.send_message(chat_id, "⏳ Carregando os dados, por favor aguarde um momento.")

        resultados_atuais, resultados_anteriores = comparar_periodo_anterior(loja, inicio, fim)

        saudacao = "Bom dia" if datetime.now().hour < 12 else "Boa tarde" if datetime.now().hour < 18 else "Boa noite"
        
        resposta = f"{saudacao}, {user_info.split(':')[1].split(',')[0]}! 🌞\n\n"
        resposta += f"**Resumo para a loja {loja} de {inicio.strftime('%Y-%m-%d %H:%M')} a {fim.strftime('%Y-%m-%d %H:%M')}:** 📊\n\n"
        resposta += f"**Indicadores de Desempenho:** 📈\n\n"
        resposta += mostrar_resultados(resultados_atuais['total_vendas_com_iva'], resultados_anteriores['total_vendas_com_iva'], "Total de Vendas (c/ IVA)", monetario=True) + "\n"
        resposta += mostrar_resultados(resultados_atuais['total_vendas_sem_iva'], resultados_anteriores['total_vendas_sem_iva'], "Total de Vendas (s/ IVA)", monetario=True) + "\n"
        resposta += mostrar_resultados(resultados_atuais['transacoes_vendas'], resultados_anteriores['transacoes_vendas'], "Transações") + "\n"
        resposta += mostrar_resultados(resultados_atuais['visitantes'], resultados_anteriores['visitantes'], "Visitantes") + "\n"
        resposta += mostrar_resultados_percentual(resultados_atuais['taxa_conversao'], resultados_anteriores['taxa_conversao'], "Taxa de Conversão") + "\n"
        resposta += mostrar_resultados_minutos(resultados_atuais['tempo_medio_permanencia'], resultados_anteriores['tempo_medio_permanencia'], "Tempo Médio de Permanência") + "\n"

        # Adicione aqui os novos indicadores
        # data_inicio_validos = datetime(2024, 7, 29)
        #if inicio >= data_inicio_validos:
        resposta += mostrar_resultados(resultados_atuais['total_passagens'], resultados_anteriores['total_passagens'], "Número de Passagens") + "\n"
        resposta += mostrar_resultados_percentual(resultados_atuais['entry_rate'], resultados_anteriores['entry_rate'], "Taxa de Captação") + "\n"

        resposta += "\n**Indicadores de Eficiência:** 🛠️\n\n"
        resposta += mostrar_resultados(resultados_atuais['ticket_medio_com_iva'], resultados_anteriores['ticket_medio_com_iva'], "Ticket Médio (c/ IVA)", monetario=True) + "\n"
        resposta += mostrar_resultados(resultados_atuais['ticket_medio_sem_iva'], resultados_anteriores['ticket_medio_sem_iva'], "Ticket Médio (s/ IVA)", monetario=True) + "\n"
        resposta += mostrar_resultados_unidades(resultados_atuais['unidades_por_transacao'], resultados_anteriores['unidades_por_transacao'], "Unidades por Transação") + "\n"
        resposta += mostrar_resultados_devolucoes(resultados_atuais['indice_devolucoes'], resultados_anteriores['indice_devolucoes'], "Índice de Devoluções") + "\n"
        resposta += mostrar_resultados_descontos(resultados_atuais['indice_descontos'], resultados_anteriores['indice_descontos'], "Índice de Descontos") + "\n"

        resposta += "\n**Top Vendedores (s/IVA):** 🏅\n\n"
        for vendedor, valor in resultados_atuais['top_vendedores']:
            resposta += f"{vendedor}: €{valor:.0f}" + "\n"

        resposta += "\n**Top Produtos (Qtd):** 🛒\n\n"
        for item, descritivo, quantidade in resultados_atuais['top_produtos']:
            resposta += f"{descritivo} ({item}): {quantidade:.0f} u." + "\n"
            
        resposta += f"\nÚltima atualização dos dados: {resultados_atuais['ultima_coleta'].strftime('%Y-%m-%d %H:%M')} 📅"

        resposta = escape_md(resposta)

        await asyncio.sleep(3)
        if mensagem_carregando:
            await bot.delete_message(chat_id, mensagem_carregando.message_id)
        await bot.send_message(chat_id, resposta, parse_mode='MarkdownV2')

        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("Mapa de Calor", callback_data=f"heatmap:{loja}:{periodo}"))
        # markup.add(InlineKeyboardButton("Gráfico de Fluxo", callback_data=f"fluxo:{loja}:{periodo}"))
        markup.add(InlineKeyboardButton("Saír", callback_data="sair"))
        await bot.send_message(chat_id, "📊 Deseja obter o Mapa de Calor ou o Gráfico de Fluxo para este período?", reply_markup=markup)
    except Exception as e:
        logger.error(f"Erro ao processar período: {str(e)}", exc_info=True)
        await asyncio.sleep(3)
        if mensagem_carregando:
            await bot.delete_message(chat_id, mensagem_carregando.message_id)
        await bot.send_message(chat_id, "⚠️ Houve um problema. Por favor, utilize /consultar para reiniciar o processo.")

async def cancelar_consulta(call: types.CallbackQuery):
    try:
        user_info = get_user_info(call.message)
        try:
            await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except aiogram.utils.exceptions.MessageNotModified:
            logger.warning(f"Mensagem não modificada: {call.message.message_id}")
        
        await call.answer("❌ Consulta cancelada pelo utilizador")
        logger.info(f"Consulta cancelada pelo usuário {user_info}")
        
        await bot.send_message(call.message.chat.id, "❌ Consulta cancelada. Utilize /consultar para iniciar nova consulta ou /funcoes para listar todas as opções.")
    except Exception as e:
        logger.error(f"Erro ao cancelar consulta: {str(e)}", exc_info=True)
        await bot.send_message(call.message.chat.id, f"Erro ao cancelar consulta: {str(e)}")

async def process_heatmap_choice(call: types.CallbackQuery):
    try:
        _, loja, periodo = call.data.split(":")
    except ValueError:
        logger.error(f"Callback data format error: {call.data}")
        await call.message.reply("⚠️ Formato de dados inválido. Por favor, utilize /consultar para tentar novamente.")
        return

    mensagem_carregando_heatmap = None
    user_info = get_user_info(call.message)
    try:
        await call.answer(f"Opção selecionada: Heatmap")
        logger.info(f"Opção selecionada: Heatmap para a loja {loja}, período {periodo} por {user_info}")

        await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)

        mensagem_carregando_heatmap = await bot.send_message(call.message.chat.id, "🌡️ A gerar os mapas de calor, por favor aguarde um momento.")
        
        if periodo == "Customizado":
            datas = user_states.get(call.message.chat.id, {})
            inicio, fim = datas.get('data_hora_inicio'), datas.get('data_hora_fim')
            if not (inicio and fim):
                await bot.send_message(call.message.chat.id, "⚠️ Período customizado inválido. Utilize /consultar para tentar novamente.")
                return
        else:
            inicio, fim, _, _ = obter_datas(periodo)

        now = datetime.now()
        if fim > now:
            fim = now

        sub_type = 1 if periodo in ["Hoje", "Ontem"] else 2 if periodo == "Esta Semana" else 3

        ip_addresses = stores.get(loja, [])
        for ip in ip_addresses:
            heatmap_image = generate_heatmap(ip, inicio.strftime('%Y-%m-%d-%H-%M-%S'), fim.strftime('%Y-%m-%d-%H-%M-%S'), sub_type)
            if heatmap_image:
                await bot.send_photo(call.message.chat.id, heatmap_image)
                heatmap_image.close()
            else:
                await bot.send_message(call.message.chat.id, f"⚠️ Não foi possível gerar o mapa de calor para o IP: {ip}")

        if mensagem_carregando_heatmap:
            await bot.delete_message(call.message.chat.id, mensagem_carregando_heatmap.message_id)
        await bot.send_message(call.message.chat.id, "✅ Processo concluído. Utilize /consultar para iniciar uma nova consulta.")
    except Exception as e:
        logger.error(f"Erro ao processar escolha do heatmap: {str(e)}", exc_info=True)
        if mensagem_carregando_heatmap:
            await bot.delete_message(call.message.chat.id, mensagem_carregando_heatmap.message_id)
        await bot.send_message(call.message.chat.id, "⚠️ Houve um problema ao processar sua escolha. Por favor, utilize /consultar para reiniciar o processo.")

def consultar_dados_acumulados(loja, inicio, fim):
    session = Session()
    try:
        dados = session.query(
            func.strftime('%Y-%m-%d %H:00:00', PeopleCountingData.start_time).label('hora'),
            func.sum(PeopleCountingData.line1_in + PeopleCountingData.line2_in + PeopleCountingData.line3_in).label('visitantes'),
            func.sum(PeopleCountingData.total_in).label('conversoes')
        ).filter(
            PeopleCountingData.loja == loja,
            PeopleCountingData.start_time >= inicio,
            PeopleCountingData.end_time <= fim
        ).group_by(
            func.strftime('%Y-%m-%d %H:00:00', PeopleCountingData.start_time)
        ).all()
        
        resultados = [{'hora': dado.hora, 'visitantes': dado.visitantes, 'conversoes': dado.conversoes} for dado in dados]
        return resultados
    except Exception as e:
        logger.error(f"Erro ao buscar dados acumulados: {str(e)}", exc_info=True)
        return []
    finally:
        session.close()

async def process_flow_choice(call: types.CallbackQuery):
    try:
        data_parts = call.data.split(":")
        if len(data_parts) != 4:
            raise ValueError("Callback data format error")

        _, choice, loja, periodo = data_parts
        mensagem_carregando_fluxo = None
        user_info = get_user_info(call.message)

        await call.answer(f"Opção selecionada: {choice}")
        logger.info(f"Opção selecionada: {choice} para o gráfico de fluxo da loja {loja}, período {periodo} por {user_info}")

        await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)

        mensagem_carregando_fluxo = await bot.send_message(call.message.chat.id, "📈 A gerar o gráfico de fluxo, por favor aguarde um momento.")
        
        if periodo == "Customizado":
            datas = user_states.get(call.message.chat.id, {})
            inicio, fim = datas.get('data_hora_inicio'), datas.get('data_hora_fim')
            if not (inicio and fim):
                await bot.send_message(call.message.chat.id, "⚠️ Período customizado inválido. Utilize /consultar para tentar novamente.")
                return
        else:
            inicio, fim, _, _ = obter_datas(periodo)

        dados = consultar_dados_acumulados(loja, inicio, fim)

        if not dados:
            if mensagem_carregando_fluxo:
                await bot.delete_message(call.message.chat.id, mensagem_carregando_fluxo.message_id)
            await bot.send_message(call.message.chat.id, "⚠️ Não há dados disponíveis para gerar o gráfico de fluxo.")
            return

        horas = [dado['hora'] for dado in dados]
        visitantes = [dado['visitantes'] for dado in dados]
        conversoes = [dado['conversoes'] for dado in dados]

        fig, ax1 = plt.subplots()

        cor_barras = 'tab:blue'
        ax1.set_xlabel('Hora do Dia')
        ax1.set_ylabel('Visitantes', color=cor_barras)
        ax1.bar(horas, visitantes, color=cor_barras, label='Visitantes')
        ax1.tick_params(axis='y', labelcolor=cor_barras)

        ax2 = ax1.twinx()
        cor_linha = 'tab:red'
        ax2.set_ylabel('Taxa de Conversão (%)', color=cor_linha)
        ax2.plot(horas, conversoes, color=cor_linha, label='Taxa de Conversão (%)')
        ax2.tick_params(axis='y', labelcolor=cor_linha)

        fig.tight_layout()
        plt.title(f"Gráfico de Fluxo - Loja {loja} - Período {periodo}")
        ax1.legend(loc='upper left')
        ax2.legend(loc='upper right')

        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        plt.close(fig)

        if mensagem_carregando_fluxo:
            await bot.delete_message(call.message.chat.id, mensagem_carregando_fluxo.message_id)
        await bot.send_photo(call.message.chat.id, buf)
        buf.close()
        await bot.send_message(call.message.chat.id, "✅ Processo concluído. Utilize /consultar para iniciar uma nova consulta.")
    except ValueError as ve:
        logger.error(f"Callback data format error: {call.data} - {ve}")
        await bot.send_message(call.message.chat.id, "⚠️ Formato de dados inválido. Por favor, utilize /consultar para tentar novamente.")
    except Exception as e:
        logger.error(f"Erro ao processar escolha do gráfico de fluxo: {str(e)}", exc_info=True)
        if mensagem_carregando_fluxo:
            await bot.delete_message(call.message.chat.id, mensagem_carregando_fluxo.message_id)
        await bot.send_message(call.message.chat.id, "⚠️ Houve um problema ao processar sua escolha. Por favor, utilize /consultar para reiniciar o processo.")

async def enviar_mensagem_reinicio(chat_id):
    await bot.send_message(chat_id, "🔄 Vamos lá voltar a conversar! Utilize /consultar para começar uma nova consulta ou /help para ver as instruções. 😃")

async def enviar_mensagem_desligamento(chat_id):
    await bot.send_message(chat_id, "⚠️ Estamos temporariamente fora do ar para melhorarmos a nossa ferramenta e trazer novas funcionalidades. Vamos notificá-lo assim que estivermos de volta. Até breve! 🚀✨")

def signal_handler(sig, frame):
    last_chat_id = get_last_chat_id()
    if last_chat_id:
        loop = asyncio.get_event_loop()
        loop.create_task(enviar_mensagem_desligamento(last_chat_id))
    logger.info("Bot desligado")
    loop.stop()

async def set_commands(bot: Bot):
    commands = [
        BotCommand(command="/start", description="Inicia o bot"),
        BotCommand(command="/help", description="Mostra as instruções"),
        BotCommand(command="/funcoes", description="Lista todas as funções disponíveis"),
        BotCommand(command="/registo", description="Regista um novo usuário usando um código de convite"),
        BotCommand(command="/consultar", description="Inicia uma consulta de vendas"),
        BotCommand(command="/exportardados", description="Exporta os dados em um arquivo Excel"),
        BotCommand(command="/gerarconvite", description="Gera um convite para novos usuários (Admin)"),
        BotCommand(command="/apagarutilizador", description="Remove um usuário do sistema (Admin)"),
        BotCommand(command="/listarusuarios", description="Lista todos os usuários registrados (Admin)"),
        BotCommand(command="/alterarnivel", description="Gera um código de alteração de nível de acesso (Admin)"),
        BotCommand(command="/usarcodigo", description="Usa um código para alterar seu nível de acesso")
    ]
    await bot.set_my_commands(commands)

import signal

def signal_handler(sig, frame):
    last_chat_id = get_last_chat_id()
    if last_chat_id:
        asyncio.create_task(enviar_mensagem_desligamento(last_chat_id))
    logger.info("Bot desligado")
    asyncio.get_event_loop().stop()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

async def main():
    global bot, dp

    # Carregar dados dos arquivos JSON
    load_user_data()
    load_invites()
    load_alteration_codes()
    load_super_admin()

    bot = Bot(token=TELEGRAM_TOKEN_QA)
    dp = Dispatcher(bot)

    # Definir comandos
    await set_commands(bot)

    # Registrando handlers
    dp.register_message_handler(handle_heavy_task, commands=['start_heavy_task'])
    dp.register_message_handler(send_welcome, commands=['start'])
    dp.register_message_handler(send_help, commands=['help'])
    dp.register_message_handler(listar_funcoes, commands=['funcoes'])
    dp.register_message_handler(registo, commands=['registo'])
    dp.register_message_handler(consultar, commands=['consultar'])
    dp.register_message_handler(gerar_convite, commands=['gerarconvite'])
    dp.register_message_handler(apagar_utilizador, commands=['apagarutilizador'])
    dp.register_message_handler(listar_usuarios, commands=['listarusuarios'])
    dp.register_message_handler(gerar_codigo_alteracao, commands=['alterarnivel'])
    dp.register_message_handler(usarcodigo, commands=['usarcodigo'])
    dp.register_message_handler(exportardados, commands=['exportardados'])
    dp.register_message_handler(processar_codigo_convite, lambda message: message.chat.id in user_states and user_states[message.chat.id]['step'] == 'codigo_convite')
    dp.register_message_handler(processar_apagar_usuario, lambda message: message.chat.id in user_states and user_states[message.chat.id]['step'] == 'apagar_usuario')
    dp.register_message_handler(processar_codigo_alteracao, lambda message: message.chat.id in user_states and user_states[message.chat.id]['step'] == 'codigo_alteracao')
    dp.register_message_handler(processar_data_hora_inicio, lambda message: message.chat.id in user_states and user_states[message.chat.id]['step'] == 'data_hora_inicio')
    dp.register_message_handler(processar_data_hora_fim, lambda message: message.chat.id in user_states and user_states[message.chat.id]['step'] == 'data_hora_fim')
    dp.register_message_handler(processar_data_hora_inicio_exportar, lambda message: message.chat.id in user_states and user_states[message.chat.id]['step'] == 'data_hora_inicio_exportar')
    dp.register_message_handler(processar_data_hora_fim_exportar, lambda message: message.chat.id in user_states and user_states[message.chat.id]['step'] == 'data_hora_fim_exportar')
    dp.register_callback_query_handler(processar_nivel_acesso_convite, lambda call: call.data.startswith('nivel_acesso_convite:'))
    dp.register_callback_query_handler(processar_grupo_convite, lambda call: call.data.startswith('grupo_convite:'))
    dp.register_callback_query_handler(processar_loja_convite, lambda call: call.data.startswith('loja_convite:'))
    dp.register_callback_query_handler(processar_nivel_acesso_alteracao, lambda call: call.data.startswith('nivel_acesso_alteracao:'))
    dp.register_callback_query_handler(process_consultar_grupo, lambda call: call.data.startswith('consultar_grupo:'))
    dp.register_callback_query_handler(process_consultar_loja, lambda call: call.data.startswith('consultar_loja:'))
    dp.register_callback_query_handler(process_exportar_grupo, lambda call: call.data.startswith('exportar_grupo:'))
    dp.register_callback_query_handler(process_exportar_loja, lambda call: call.data.startswith('exportar_loja:'))
    dp.register_callback_query_handler(process_periodo_step, lambda call: call.data.startswith('periodo:'))
    dp.register_callback_query_handler(cancelar_consulta, lambda call: call.data == 'sair')
    dp.register_callback_query_handler(process_heatmap_choice, lambda call: call.data.startswith('heatmap:'))
    dp.register_callback_query_handler(process_flow_choice, lambda call: call.data.startswith('fluxo:'))

    try:
        await dp.start_polling()
    except Exception as e:
        logger.error(f"Erro durante a execução do bot: {str(e)}", exc_info=True)
        raise
    finally:
        await bot.close()

if __name__ == '__main__':
    load_super_admin()
    if super_admin.get('chat_id'):
        asyncio.run(main())
    else:
        print("Super Admin não está definido. Por favor, configure o Super Admin no arquivo super_admin.json.")