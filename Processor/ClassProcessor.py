import threading
from datetime import datetime
import time
from Logs import ClassLogger
from Processar.Process_api import process_api
from Processar.Process_from_name import process_from_name
from Processar.Process_verify import process_verify_status
from Processar.Process_MatchName import process_match_name
from Conexao import ConectionClass, ConectionPool
from concurrent.futures import ThreadPoolExecutor, as_completed
# from db_poll import DbPool
# from Conexao.ConectionTrheaddeConectionPoll import ConectionClass as t
from Mail.ClassMail import enviar_email_all
from Model.ClassModel import buscar_teste, search_data_interpol
import pandas as pd
from pathlib import Path
import csv




class Processor:
    def __init__(self, max_workers: int = 10, batch_size: int = 1000):
        self.config = ConectionClass.DbConfig()
        self.max_workers = max_workers
        self.max_workers_conn = 2
        self.batch_size = batch_size
        # self.idProcesso = idProcesso
        self.servidor = 'https://ws-public.interpol.int/notices/v1/red'
        # self.servidor_nationality = 'https://ws-public.interpol.int/notices/v1/red?&forename=JACK&nationality'  #busca por pais gama maior de resultados  o resultado da api mostra no maximo 160 por api 
        self.servidor_nationality = 'https://ws-public.interpol.int/notices/v1/red?nationality'  #busca por pais gama maior de resultados  o resultado da api mostra no maximo 160 por api 
        self.servidor_push_expecifg_id= 'https://ws-public.interpol.int/notices/v1/red/'   # ESTA URL PASSANDO O ID DO DA INTERPOL (entity_id "2026-15452")  ELE TRAZ DADOS ESPECIFICOS EXEMPLO "TIPO DO CRIME, PAIS DE ACUSACAO, LINGUAS QUE FALA,"
        self.servidor_get_from_name= 'https://ws-public.interpol.int/notices/v1/red?&forename'   # ESTA URL PASSANDO O ID DO DA INTERPOL (entity_id "2026-15452")  ELE TRAZ DADOS ESPECIFICOS EXEMPLO "TIPO DO CRIME, PAIS DE ACUSACAO, LINGUAS QUE FALA,"
        self.servidor_headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        self.batch_counter_status1 = 0
        self.batch_counter_status2 = 0
        self.batch_counter_status4 = 0
        self.qtPage = 160 # resultado na tela e apresentado somente 160 registros 
        self.indicePage = 1
        self.time_sleps = 2
        self.periodo = 'SEMANAL'
        self.true = True
        self.false =False
        self.batch_size_verify = 50
        self.lock = threading.Lock()
        self.db = ConectionPool.DbPool(maxconn=self.max_workers)

    def executar(self):
        inicio = datetime.now()
        ClassLogger.logger.info("=" * 80)
        ClassLogger.logger.info(f"Iniciando Interpol - Consulta Proscore - {inicio}")
        time.sleep(2)
        ClassLogger.logger.info("=" * 80)

        try:
            
            total_processados = process_api(self)

            ClassLogger.logger.info(f"minha quantidade de dados processados :  {total_processados}")
          
            fim = datetime.now()
            duracao = (fim - inicio).total_seconds()
            ClassLogger.logger.info("---" * 80)
          

        except Exception as e:
            ClassLogger.logger.error(f"Erro fatal na execução: {str(e)}")
            error = f"Erro fatal na execução: process_api {str(e)}"
            corpo = f"""<h2 style="color:red;">Falha no processo de Captura e tratamento dos dados</h2> <p>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Mensagem:: {error}</p>"""
            enviar_email_all(corpo)

        finally:
             
             ClassLogger.logger.error(f"SAINDO NO FINALIY")
             
             pass

    def enviar_email(self):
        desti = 'anderson@proscore.com.br'
        assunto = 'Teste de Envio de Email'
        corpo = f"atualizacao da base da interpol,processo finaliazado  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        result_email = enviar_email_all(corpo)
        ClassLogger.logger.info(f"ENVIANDO EMAIL DE TESTE PARA VER SE O SERVIÇO DE EMAIL ESTA FUNCIONANDO")
        pass


    def busca_dados(self): 
        ClassLogger.logger.info('busca dados')
        
        buscar_teste(self)
        pass
        
    def teste_busca_interpol(self): 
        lista = []
        exist_id = False
        exist_name = False
        ClassLogger.logger.info('busca dados')
        caminho_arquivo_csv = Path('Arquivos/dados_unique.csv')

        # df = pd.read_csv(caminho_arquivo_csv)
        df = pd.read_csv(caminho_arquivo_csv, sep=';')
        coluna_id = df['id']
        lista = df['id'].tolist()
        # lista.append(coluna_id)


        # print(f" MINHA LISTA {lista}")

        # with open(caminho_arquivo_csv, mode='r', encoding='utf-8') as arquivo_csv:
        #         leitor = csv.reader(arquivo_csv)
        #         for linha in leitor:
        #             print(linha)
        # search_data_interpol
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
          with self.db.get_connection() as conn:
            for teste in lista:
                print(f"DADOS NO FOR {teste}")
                future_busca = executor.submit(search_data_interpol, self, teste)
                exist_id = future_busca.result()
                
                # retorno = search_data_interpol(self,conn,teste)
                print(f"MEU RETORNO DA PESQUISA :: do id {teste} , {exist_id}")
            
        
        pass
    
    
    def from_name_interpol(self): 
        ClassLogger.logger.info('INICIO A CHAMADA PARA A BUSCA POR NOME DENTRO DA API')
        ClassLogger.logger.info(f"[{time.strftime('%H:%M:%S')}] Iniciando a consulta from_name_interpol")
       
        try:
           result =  process_from_name(self)
          
        except Exception as e:
                ClassLogger.logger.info(f"[{time.strftime('%H:%M:%S')}] Finalizado o processo de busca por nome dados_interpol")
                error = f"Erro fatal na execução Busca por nome: {str(e)}"
                corpo = f"""<h2 style="color:red;"> Finalizado o processo de busca por nome dados interpol</h2> <p>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Mensagem:: {error}</p>"""
                enviar_email_all(corpo)
        finally:
            return result
    pass

    def atualiza_dados_interpol(self):
        ClassLogger.logger.info('IREI SOLICITAR A PESQUISA PELO O ID PARA SABER SE ESTA ATIVO OU INATIVO')
        ClassLogger.logger.info(f"[{time.strftime('%H:%M:%S')}] Iniciando a consulta atualiza_dados_interpol")
        
        try:
           result =  process_verify_status(self)
          
        except Exception as e:
                ClassLogger.logger.info(f"[{time.strftime('%H:%M:%S')}] erro na verificação de ativo e inativo {str(e)}")
                error = f"Erro fatal na execução: {str(e)}"
                corpo = f"""<h2 style="color:red;"> erro na verificação de ativo e inativo dados interpol</h2> <p>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Mensagem:: {error}</p>"""
                enviar_email_all(corpo)

        finally:
             ClassLogger.logger.info(f"[{time.strftime('%H:%M:%S')}] Finalizado o processo de busca por nome dados_interpol")
            
    pass
        
    def match_name(self):
        ClassLogger.logger.info('IREI SOLICITAR OS NOME PARA O MATCH NAME , PEGANDO O CPF NA PROSCORE PARA SABER SE ESTA ATIVO OU INATIVO')
      
        # print(f"MINHA THEADS {self.max_workers}")
       
        try:
            process_match_name(self)
            ClassLogger.logger.info(f"[{time.strftime('%H:%M:%S')}] Iniciando a consulta match_name")
        
        except Exception as e:
                ClassLogger.logger.info(f"[{time.strftime('%H:%M:%S')}] erro na verificação do match name proscore {str(e)}")
                error = f"Erro fatal na execução: {str(e)}"
                corpo = f"""<h2 style="color:red;"> erro na verificação match name proscore</h2> <p>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Mensagem:: {error}</p>"""
                enviar_email_all(corpo)

        finally:
             ClassLogger.logger.info(f"[{time.strftime('%H:%M:%S')}] Finalizado o processo de busca por match name")
        pass


    def executar_ciclo(self):
        self.executar()   
        # self.enviar_email()
        # self.busca_dados()   
        # self.teste_busca_interpol()   
        # self.from_name_interpol()   
        # self.atualiza_dados_interpol()
        #self.match_name()
        ClassLogger.logger.info(f"[{time.strftime('%H:%M:%S')}] Iniciando a consulta")      
    def executar_ciclo_name(self):
        ClassLogger.logger.info(f"[{time.strftime('%H:%M:%S')}] Iniciando a consulta From Name Primeira letras")     
        # self.from_name_interpol()