import threading
from datetime import datetime
import time
from Logs import ClassLogger
from Processar.Process_api import Process_api
from Conexao import ConectionClass


class Processor:
    def __init__(self, max_workers: int = 10, batch_size: int = 1000):
        self.config = ConectionClass.DbConfig()
        self.max_workers = max_workers
        self.batch_size = batch_size
        # self.idProcesso = idProcesso
        self.servidor = 'https://ws-public.interpol.int/notices/v1/red'
        self.servidor_nationality = 'https://ws-public.interpol.int/notices/v1/red?nationality'  #busca por pais gama maior de resultados  o resultado da api mostra no maximo 160 por api 
        self.batch_counter_status1 = 0
        self.batch_counter_status2 = 0
        self.batch_counter_status4 = 0
        self.qtPage = 160 # resultado na tela e apresentado somente 160 registros 
        self.indicePage = 1
        self.lock = threading.Lock()

    def executar(self):
        inicio = datetime.now()
        ClassLogger.logger.info("=" * 80)
        ClassLogger.logger.info(f"Iniciando Progestor - Consulta Proscore - {inicio}")
        time.sleep(2)
        ClassLogger.logger.info("=" * 80)

        try:
            
            total_processados = Process_api(self)

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


    def executar_ciclo(self):
        self.executar()   
        ClassLogger.logger.info(f"[{time.strftime('%H:%M:%S')}] Iniciando a consulta")      