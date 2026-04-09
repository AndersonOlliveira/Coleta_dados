import json
from Logs import ClassLogger
import os
import pandas as pd
from .Request import push_request,push_new_resquests
from concurrent.futures import ThreadPoolExecutor, as_completed
import unicodedata
import re
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime
from Mail.ClassMail import enviar_email_all
from urllib.parse import urlparse, parse_qs
import sys
# from collections import defaultdict
from Conexao import ConectionClass,ConectionPool
from Model.ClassModel import list_interpol,update_id_interpol_status



def process_verify_status(self):
      lista_pesquisa_url =[]
   

      with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
          future_interpol = executor.submit(list_interpol, self)
          lista_ids_interpol = future_interpol.result()

          if lista_ids_interpol:
             ClassLogger.logger.warning('INICIANDO VERIFICARCAO SE ESTA ATIVO OU INATIVO')

             print(f"MINHA LISTA {lista_ids_interpol}")
             for dados in lista_ids_interpol:
                  interpol = dados.get('id_interpol', '')

                  print(f"TENHO O ID?{interpol}")
                #   params = f"&resultPerPage={self.qtPage}&page={self.indicePage}"
                
               #    lista_singlas_name = f"{self.servidor}/2026-19190"
                  lista_singlas_name = f"{self.servidor}/{interpol}"

          if interpol:
             lista_pesquisa_url.append(lista_singlas_name)

        
        
          print(f"MINHA URL MONTADA {lista_pesquisa_url}")


          #REALIZAO O RESQUEST 
          with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
               result_pesquisa = list(executor.map(
               lambda url: push_new_resquests(url, self.time_sleps),
               lista_pesquisa_url
               ))


          print(f"DADOS RETORNADOS {result_pesquisa}")

          if result_pesquisa is not None:
              ClassLogger.logger.info('Vou realizar o processamento dos dados retornados do consumo da api')
              for resultado in result_pesquisa:
                   print(f"resultado do processamento {resultado}")
                   if resultado.get('message') == False:
                        ClassLogger.logger.info('O registro esta inativo ou nao encontrado na interpol')

                        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                             future_interpol = executor.submit(update_id_interpol_status, self, resultado.get('id_interpol'), False, datetime.now().strftime("%Y-%m-%d %H:%M:%S") )
                             result_update = future_interpol.result()
                             print(f"Resultado Baixa : {result_update}")
                         

                   else:
                        ClassLogger.logger.info('O registro esta ativo na interpol')
      








