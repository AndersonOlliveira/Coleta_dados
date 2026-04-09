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
from Model.ClassModel import get_data_match_name_base,search_from_name_interpol,push_cpf
from Mail.ClassMail import enviar_email_all
from functions.funcoes import remover_acentos, remover_conhetes, tratar_entrada




def process_match_name(self):
  
    lista_pesquisa_name =[]
    contador_por_matchName = defaultdict(lambda: {"QTUPDATE": 0, "ERROR": 0})
    falhas_ids = []
   

    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
          future_interpol = executor.submit(get_data_match_name_base, self)
          lista_name_braisil = future_interpol.result()

   
    if lista_name_braisil:  
        #MINHA WORKES
        with ThreadPoolExecutor(max_workers=self.max_workers_conn) as executor:
            futures = [
                 executor.submit(search_from_name_interpol,self,lista_names.get('nome'), lista_names.get('data_nascimento'),lista_names.get('ID_INTERPOL'),lista_names.get('id_tabela'))
                       for lista_names in lista_name_braisil
                    ]
            
            for future in as_completed(futures):
                result = future.result()

                            
                print(f"MEU RETORNO DO RESULT O QUE VEM AQUI? {result}")
                                # print(f"tenho acesso as siglas {result['person_sigla_unico']}")
                                
                if result['status'] == "sucesso":
                                          
                   contador_por_matchName[result['ID_COLUNA_INTERPOL']]["QTUPDATE"] += 1
                               #SE EXISTIR VOU ATUALIAZR O NOVO REGISTRO COM O ID DA TABELA
                   udpate_match_name = push_cpf(self,result['CPF'], result['ID_COLUNA_INTERPOL'])
                   print(f"MINHA ATUALIZAÇÃO DO CPF {udpate_match_name}")
                else:
                    falhas_ids.append(result)
                                                    # falha_ +=1
                    contador_por_matchName[result['ID_COLUNA_INTERPOL']]["ERROR"] += 1

                #    print(f"LISTA DE RETONRO:: {retorno_lista}")


                
            
        print(f"MINHA LISTA PARA PESQUISA PELO O NOME {falhas_ids}")
        if falhas_ids:
                tabela_error = pd.DataFrame(falhas_ids)
                tabela_error = tabela_error.fillna(0) 
                convertida_error =  tabela_error.to_html(index=False, border=1, justify='center')
                corpo_error = f"Lista de dados com error :<br> {convertida_error}"
                corpo = f"""
                <h2 style="color:green;">Match Names Não Encontrados Base ProScore</h2>
                <p>{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>"""
                html_final = f"""<html><body> {corpo}
                <hr>
                {corpo_error if corpo_error else "<p>Sem erros encontrados</p>"}
                </body></html>
                """
                
                
                result_email = enviar_email_all(html_final)

            # print(f"Resultado do enviar e-mail {result_email}")

            #envia a quantiade para 
                return result_email