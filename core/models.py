from sqlalchemy import Column, Integer, String, Float, DateTime, create_engine, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging
from datetime import datetime

# Configuração do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração do banco de dados
DATABASE_URL = 'sqlite:///c:/projetos/grnl_platform/bot_database.db'
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

class SaleData(Base):
    __tablename__ = 'sales_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    loja = Column(String, nullable=False)
    data = Column(DateTime, nullable=False)
    codigo = Column(String, nullable=False)
    referencia_documento = Column(String, nullable=False)
    documento_original = Column(String, nullable=True)
    tipo_documento = Column(String, nullable=False)
    hora = Column(String, nullable=False)
    vendedor_codigo = Column(String, nullable=False)
    vendedor_nome_curto = Column(String, nullable=False)
    item = Column(String, nullable=False)
    descritivo = Column(String, nullable=False)
    quantidade = Column(Float, nullable=False)
    valor_venda_com_iva = Column(Float, nullable=False)
    valor_venda_sem_iva = Column(Float, nullable=False)
    iva = Column(Float, nullable=False)
    desconto = Column(Float, nullable=False)
    percentual_desconto = Column(Float, nullable=False)
    motivo_desconto = Column(String, nullable=True)

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
    line4_out = Column(Integer)  # Adicionando line4_out

class HeatmapData(Base):
    __tablename__ = 'heatmap_data'
    id = Column(Integer, primary_key=True)
    loja = Column(String)
    ip = Column(String)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    value = Column(Integer)

class RegionalPeopleCountingData(Base):
    __tablename__ = 'regional_people_counting_data'
    id = Column(Integer, primary_key=True)
    loja = Column(String)
    ip = Column(String)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    region1 = Column(Integer)
    region2 = Column(Integer)
    region3 = Column(Integer)
    region4 = Column(Integer)
    total = Column(Integer)

class AnalyticsResults(Base):
    __tablename__ = 'analytics_results'

    id = Column(Integer, primary_key=True, index=True)
    loja = Column(String, index=True)
    data_inicio = Column(DateTime, index=True)
    data_fim = Column(DateTime, index=True)
    total_vendas_com_iva = Column(Float)
    total_vendas_sem_iva = Column(Float)
    transacoes_vendas = Column(Integer)
    visitantes = Column(Integer)
    taxa_conversao = Column(Float)
    tempo_medio_permanencia = Column(Float)
    ticket_medio_com_iva = Column(Float)
    ticket_medio_sem_iva = Column(Float)
    unidades_por_transacao = Column(Float)
    indice_devolucoes = Column(Float)
    indice_descontos = Column(Float)
    entry_rate = Column(Float)  # Adicionando entry_rate
    total_passagens = Column(Integer)  # Adicionando total_passagens
    ultima_coleta = Column(DateTime)

class LastUpdate(Base):
    __tablename__ = 'last_update'
    id = Column(Integer, primary_key=True)
    loja = Column(String, nullable=False)
    last_update_time = Column(DateTime, nullable=False)

class ProcessedFiles(Base):
    __tablename__ = 'processed_files'
    id = Column(Integer, primary_key=True)
    file_name = Column(String, unique=True)

Base.metadata.create_all(engine)

def processar_dados_venda_entrada(dado):
    try:
        dado_processado = {
            'loja': dado['Loja'],
            'data': datetime.strptime(dado['Data'] + dado['Hora'], '%Y%m%d%H:%M:%S'),  # Aqui garantimos a conversão correta
            'codigo': dado['Codigo'],
            'referencia_documento': dado['ReferenciaDocumento'],
            'documento_original': dado['DocumentoOriginal'] if dado['DocumentoOriginal'] else None,
            'tipo_documento': dado['TipoDocumento'],
            'hora': dado['Hora'],
            'vendedor_codigo': dado['VendedorCodigo'],
            'vendedor_nome_curto': dado['VendedorNomeCurto'],
            'item': dado['Item'],
            'descritivo': dado['Descritivo'],
            'quantidade': float(dado['QuantidadeDataTypeNumber'].replace(',', '.')),
            'valor_venda_com_iva': float(dado['Valor venda com IVADataTypeNumber'].replace(',', '.')),
            'valor_venda_sem_iva': float(dado['Valor venda sem IVADataTypeNumber'].replace(',', '.')),
            'iva': float(dado['IVADataTypeNumber'].replace(',', '.')),
            'desconto': float(dado['DescontoDataTypeNumber'].replace(',', '.')),
            'percentual_desconto': float(dado['% DescontoDataTypeNumber'].replace(',', '.')),
            'motivo_desconto': dado['Motivo Desconto'] if dado['Motivo Desconto'] else None
        }
        return SaleData(**dado_processado)
    except Exception as e:
        logger.error(f"Erro ao processar dados de entrada: {str(e)}", exc_info=True)
        return None

def verificar_existencia_duplicada(session, dado):
    existe = session.query(SaleData).filter(
        and_(
            SaleData.referencia_documento == dado.referencia_documento,
            SaleData.item == dado.item,
            SaleData.data == dado.data
        )
    ).first()
    return existe is not None

def armazenar_dados_no_banco(dados, model):
    session = Session()
    try:
        if model == SaleData:
            objetos = [processar_dados_venda_entrada(d) for d in dados if d is not None]
        else:
            objetos = [model(**d) for d in dados]

        for obj in objetos:
            if isinstance(obj, SaleData) and verificar_existencia_duplicada(session, obj):
                logger.warning(f"Dado duplicado encontrado: {obj.referencia_documento}, item: {obj.item}, data: {obj.data}")
                continue
            session.add(obj)

        session.commit()
        logger.info("Dados armazenados no banco de dados com sucesso")
    except Exception as e:
        logger.error(f"Erro ao armazenar dados no banco de dados: {str(e)}", exc_info=True)
        session.rollback()
    finally:
        session.close()

def buscar_dados_do_banco(loja, inicio, fim, model, date_attr):
    session = Session()
    try:
        date_column = getattr(model, date_attr)
        
        logger.info(f"Buscando dados para a loja {loja} entre {inicio} e {fim}")

        dados = session.query(model).filter(
            model.loja == loja,
            date_column >= inicio,
            date_column <= fim
        ).all()

        ultima_atualizacao = session.query(model).filter(
            model.loja == loja
        ).order_by(date_column.desc()).first()

        if ultima_atualizacao:
            ultima_atualizacao_data = getattr(ultima_atualizacao, date_attr)
            return dados, ultima_atualizacao_data
        return dados, None
    except Exception as e:
        logger.error(f"Erro ao buscar dados do banco: {str(e)}", exc_info=True)
        return [], None
    finally:
        session.close()

# Função para atualizar ou criar registros de contagem de pessoas
def atualizar_ou_criar_dados_people_counting(dados):
    session = Session()
    try:
        for dado in dados:
            start_time = datetime.strptime(dado['StartTime'], '%Y/%m/%d %H:%M:%S')
            end_time = datetime.strptime(dado['EndTime'], '%Y/%m/%d %H:%M:%S')
            
            # Ignorar registros no futuro
            if start_time > datetime.now():
                logger.info(f"Ignorando registro futuro: {start_time}")
                continue

            # Atualizar ou criar registro
            record = session.query(PeopleCountingData).filter(
                and_(
                    PeopleCountingData.start_time == start_time,
                    PeopleCountingData.end_time == end_time,
                    PeopleCountingData.loja == dado['Loja']
                )
            ).first()

            if record:
                logger.info(f"Atualizando registro existente para {start_time} - {end_time}")
                record.total_in = dado['Total - In']
                record.line1_in = dado['Line1 - In']
                record.line2_in = dado['Line2 - In']
                record.line3_in = dado['Line3 - In']
                record.line4_in = dado['Line4 - In']
                record.line4_out = dado['Line4 - Out']  # Adicionando line4_out
            else:
                logger.info(f"Criando novo registro para {start_time} - {end_time}")
                novo_registro = PeopleCountingData(
                    loja=dado['Loja'],
                    ip=dado['IP'],
                    start_time=start_time,
                    end_time=end_time,
                    total_in=dado['Total - In'],
                    line1_in=dado['Line1 - In'],
                    line2_in=dado['Line2 - In'],
                    line3_in=dado['Line3 - In'],
                    line4_in=dado['Line4 - In'],
                    line4_out=dado['Line4 - Out']  # Adicionando line4_out
                )
                session.add(novo_registro)

        session.commit()
        logger.info("Dados de contagem de pessoas atualizados com sucesso")
    except Exception as e:
        logger.error(f"Erro ao atualizar dados de contagem de pessoas: {str(e)}", exc_info=True)
        session.rollback()
    finally:
        session.close()

# Função para atualizar ou criar registros de contagem de pessoas regionais
def atualizar_ou_criar_dados_regionais(dados):
    session = Session()
    try:
        for dado in dados:
            start_time = datetime.strptime(dado['StartTime'], '%Y/%m/%d %H:%M:%S')
            end_time = datetime.strptime(dado['EndTime'], '%Y/%m/%d %H:%M:%S')
            
            # Ignorar registros no futuro
            if start_time > datetime.now():
                logger.info(f"Ignorando registro futuro: {start_time}")
                continue

            # Atualizar ou criar registro
            record = session.query(RegionalPeopleCountingData).filter(
                and_(
                    RegionalPeopleCountingData.start_time == start_time,
                    RegionalPeopleCountingData.end_time == end_time,
                    RegionalPeopleCountingData.loja == dado['Loja']
                )
            ).first()

            if record:
                logger.info(f"Atualizando registro existente para {start_time} - {end_time}")
                record.region1 = dado['region1']
                record.region2 = dado['region2']
                record.region3 = dado['region3']
                record.region4 = dado['region4']
                record.total = dado['total']
            else:
                logger.info(f"Criando novo registro para {start_time} - {end_time}")
                novo_registro = RegionalPeopleCountingData(
                    loja=dado['Loja'],
                    ip=dado['IP'],
                    start_time=start_time,
                    end_time=end_time,
                    region1=dado['region1'],
                    region2=dado['region2'],
                    region3=dado['region3'],
                    region4=dado['region4'],
                    total=dado['total']
                )
                session.add(novo_registro)

        session.commit()
        logger.info("Dados regionais de contagem de pessoas atualizados com sucesso")
    except Exception as e:
        logger.error(f"Erro ao atualizar dados regionais de contagem de pessoas: {str(e)}", exc_info=True)
        session.rollback()
    finally:
        session.close()

# Função para atualizar ou criar registros de heatmap
def atualizar_ou_criar_dados_heatmap(dados):
    session = Session()
    try:
        for dado in dados:
            start_time = datetime.strptime(dado['StartTime'], '%Y-%m-%d %H:%M:%S')
            end_time = datetime.strptime(dado['EndTime'], '%Y-%m-%d %H:%M:%S')
            
            # Ignorar registros no futuro
            if start_time > datetime.now():
                logger.info(f"Ignorando registro futuro: {start_time}")
                continue

            # Atualizar ou criar registro
            record = session.query(HeatmapData).filter(
                and_(
                    HeatmapData.start_time == start_time,
                    HeatmapData.end_time == end_time,
                    HeatmapData.loja == dado['Loja']
                )
            ).first()

            if record:
                logger.info(f"Atualizando registro existente para {start_time} - {end_time}")
                record.value = dado['Value']
            else:
                logger.info(f"Criando novo registro para {start_time} - {end_time}")
                novo_registro = HeatmapData(
                    loja=dado['Loja'],
                    ip=dado['IP'],
                    start_time=start_time,
                    end_time=end_time,
                    value=dado['Value']
                )
                session.add(novo_registro)

        session.commit()
        logger.info("Dados de heatmap atualizados com sucesso")
    except Exception as e:
        logger.error(f"Erro ao atualizar dados de heatmap: {str(e)}", exc_info=True)
        session.rollback()
    finally:
        session.close()
