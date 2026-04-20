import time
from threading import Timer
from Logs import ClassLogger
from Mail.ClassMail import enviar_email_all
from Processor.ClassProcessor import Processor



if __name__ == "__main__":
    ClassLogger.logger.info(f"meu arquivo foi chamado via")
    print(f"meu arquivo foi chamado via {[{time.strftime('%H:%M:%S')}]}")