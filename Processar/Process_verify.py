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
from functions.funcoes import remover_acentos, remover_conhetes, tratar_entrada,dividir_lotes




def process_verify_status(self):
      new_tabel =[]
      tabela_atualizar= []
      result_pesquisa = []
      lista_pesquisa_url =[]
      contador_inativos = defaultdict(lambda: {
      "UPATVIO": 0,
      "NA": 0,
      "ERROR":0,
      "QTINSERT": 0
    })
   

      with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
          future_interpol = executor.submit(list_interpol, self)
          lista_ids_interpol = future_interpol.result()

          if lista_ids_interpol:
             ClassLogger.logger.info('INICIANDO VERIFICARCAO SE ESTA ATIVO OU INATIVO')

             
             for dados in lista_ids_interpol:
                  interpol = dados.get('id_interpol', '')
                  lista_singlas_name = f"{self.servidor}/{interpol}"
                  
                  if interpol:
                     lista_pesquisa_url.append(lista_singlas_name)

        
        
        
         
          for lote in dividir_lotes(lista_pesquisa_url, self.batch_size_verify):
               futures = []
                    #REALIZAO O RESQUEST 
               with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                         # result_pesquisa = list(executor.map(
                         # lambda url: push_new_resquests(url, self.time_sleps),
                         # lista_pesquisa_url
                         # ))
                    futures = [
                              executor.submit(push_new_resquests, url, self.max_workers)
                              for url in lote
                              # for url in lista_pesquisa_url
                    ]
                         
                    for future in as_completed(futures):
                         try:
                              result = future.result()
                              result_pesquisa.append(result)
                         except Exception as e:
                              print(f"Erro ao processar a requisação {str(e)}")


          

          if result_pesquisa is not None:
              
              for resultado in result_pesquisa:
                   ClassLogger.logger.info(f"resultado do processamento {resultado}")
                   if resultado.get('message') == False:
                        ClassLogger.logger.info('O registro esta inativo ou nao encontrado na interpol')
                        contador_inativos[resultado.get('id_interpol')]["UPATVIO"] += 1

                        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                             future_interpol = executor.submit(update_id_interpol_status, self, resultado.get('id_interpol'), False, datetime.now().strftime('%Y-%m-%d') )
                             result_update = future_interpol.result()
                             print(f"Resultado Baixa : {result_update}")
                         

                   else:
                        print(f'O registro esta ativo na interpol')
                        contador_inativos[resultado.get('id_interpol')]["NA"] += 1

     

          for pais, totais in contador_inativos.items():
          
               nova_linha = {
                    'DATA CAPTURA': datetime.now().strftime("%d/%m/%Y %H:%M"), 
                    'PAIS_BUSCADO': pais,
                    'QTA INATIVOS': totais["UPATVIO"],
                    'QTA ATIVOS': totais["NA"],
                    
               }
               
               new_tabel.append(nova_linha)

    
          tabela_atualizar = new_tabel
          ClassLogger.logger.info(f"TABELA DE RESUMO POR PAIS 'SIGLAS' {tabela_atualizar}")
          
          minha_tabela_montada = pd.DataFrame(tabela_atualizar)

          convertida = minha_tabela_montada.to_html(index=False, border=1, justify='center')

          corpo = f"""
          <h2 style="color:green;">Verificação de Status inativos interpol </h2>
          <p>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
          {convertida}"""
          html_final = f"""
          <html>
          <body>
          {corpo}
          </body>
          </html>
          """
          return enviar_email_all(html_final)







