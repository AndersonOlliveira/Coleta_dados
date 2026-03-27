import logging
from logging.handlers import RotatingFileHandler
#//*====================================================
#//*Config do log
#//*====================================================
class log:
# classLogger.py
 logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('interpol_logger.log'),
        logging.StreamHandler()
    ]
)


logger = logging.getLogger('interpol_logger')
