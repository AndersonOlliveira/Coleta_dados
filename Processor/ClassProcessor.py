import threading
from datetime import datetime
import time
from Logs import ClassLogger
from Processar.Process_api import process_api
from Conexao import ConectionClass
# from Conexao.ConectionTrheaddeConectionPoll import ConectionClass as t
from Mail.ClassMail import enviar_email_all
from Model.ClassModel import buscar_teste, search_data_interpol
from psycopg2.pool import ThreadedConnectionPool
from dataclasses import asdict



class Processor:
    def __init__(self, max_workers: int = 10, batch_size: int = 1000):
        self.config = ConectionClass.DbConfig()
        self.max_workers = max_workers
        self.batch_size = batch_size
        # self.idProcesso = idProcesso
        self.servidor = 'https://ws-public.interpol.int/notices/v1/red'
        self.servidor_nationality = 'https://ws-public.interpol.int/notices/v1/red?nationality'  #busca por pais gama maior de resultados  o resultado da api mostra no maximo 160 por api 
        self.servidor_push_expecifg_id= 'https://ws-public.interpol.int/notices/v1/'   # ESTA URL PASSANDO O ID DO DA INTERPOL (entity_id "2026-15452")  ELE TRAZ DADOS ESPECIFICOS EXEMPLO "TIPO DO CRIME, PAIS DE ACUSACAO, LINGUAS QUE FALA,"
        self.servidor_headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        self.batch_counter_status1 = 0
        self.batch_counter_status2 = 0
        self.batch_counter_status4 = 0
        self.qtPage = 160 # resultado na tela e apresentado somente 160 registros 
        self.indicePage = 1
        self.time_sleps = 5
        self.periodo = 'SEMANAL'
        self.true = True
        self.false =False
        self.lock = threading.Lock()
        self.pool = ThreadedConnectionPool(
            minconn=1, 
            maxconn=self.max_workers, 
           **asdict(self.config) )

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
            ClassLogger.logger.info(f"Processamento concluído em {duracao:.2f} segundos")
            ClassLogger.logger.info(f"Total de registros processados: {total_processados}")

        except Exception as e:
            ClassLogger.logger.error(f"Erro fatal na execução: {str(e)}", exc_info=True)

        finally:
             ClassLogger.logger.error(f"SAINDO NO FINALIY")
             pass

    def enviar_email(self):
        desti = 'anderson@proscore.com.br'
        assunto = 'Teste de Envio de Email'
        corpo = f"atualizacao da base da interpol,processo finaliazado  {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"
        result_email = enviar_email_all(corpo)
        ClassLogger.logger.info(f"ENVIANDO EMAIL DE TESTE PARA VER SE O SERVIÇO DE EMAIL ESTA FUNCIONANDO")
        pass


    def busca_dados(self): 
        ClassLogger.logger.info('busca dados')
        
        buscar_teste(self)
        pass
        
    def teste_busca_interpol(self): 
        ClassLogger.logger.info('busca dados')
        # search_data_interpol
        search_data_interpol(self,'2012-328264')


    def executar_ciclo(self):
        # self.executar()   
        # self.enviar_email()
        # self.busca_dados()   
        self.teste_busca_interpol()   
        ClassLogger.logger.info(f"[{time.strftime('%H:%M:%S')}] Iniciando a consulta")      