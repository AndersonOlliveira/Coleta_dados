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
from Model.ClassModel import get_data_match_name_base
from Model.ClassModel import buscar_teste, insert_interpol, update_info_process,search_data_interpol,exists_by_name,insert_base_interpol,update_data_interpol,update_id_interpol




def process_match_name(self):
  
      lista_pesquisa_name =[]
   

      with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
          future_interpol = executor.submit(get_data_match_name_base, self)
          lista_name_braisil = future_interpol.result()

          if lista_name_braisil:
             ClassLogger.logger.warning('INICIANDO VERIFICARCAO SE ESTA ATIVO OU INATIVO')
             print(f"MINHA LISTA {lista_name_braisil}")

             if lista_name_braisil:
                 F
                 lista_pesquisa_name.append(
                     {'NOME': lista_names.get('name'), 'DATA_NASCIMENTO': lista_names.get('data_nascimento')} for lista_names in lista_name_braisil )


             print(f"MINHA LISTA PARA PESQUISA PELO O NOME {lista_pesquisa_name}")