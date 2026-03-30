from Processor.ClassProcessor import Processor
import time
from threading import Timer
from Logs import ClassLogger
import threading

if __name__ == "__main__":
    instance = Processor(max_workers=2, batch_size=5)
    
    instance.executar_ciclo()
 
    # Loop Infinito
    # while True:
    #     try:
       
             
        # except KeyboardInterrupt:
            # Permite parar o script com Ctrl+C no terminal
    ClassLogger.logger.info("\nEncerrando loop por comando do usuário (Ctrl+C).")
            # break
        # except Exception as e:
            # Lida com erros inesperados e continua o loop
    ClassLogger.logger.info(f"[{time.strftime('%H:%M:%S')}] Erro inesperado: . Continuará em 30 segundos.")
            