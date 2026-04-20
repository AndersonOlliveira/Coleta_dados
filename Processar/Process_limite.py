import json
from Logs import ClassLogger
import os
import time
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
from Model.ClassModel import buscar_teste, insert_interpol, update_info_process,search_data_interpol,exists_by_name,insert_base_interpol,update_data_interpol,update_id_interpol,get_lista_name_base_interpol,insert_data_interpol_new
from functions.funcoes import remover_acentos, remover_conhetes, tratar_entrada, path_arquivo, dividir_lotes








def process_limite_countrie(self):
    #pego os dados e faco a busca
    ClassLogger.logger.info(f"INIICIANDO A BUSCA POR DADOS PARA SABER O LIMIE DOS DADOS")
    des = []
    mapa = {}
    todas_temporaria_siglas = []
    siglas = []
    id_insert_return =[]
    resultados = []
    url_completa = []
    links_interpol = []
    id_insert_return = []
    todas_temporaria_siglas = []
    todas_pessoas =[]
    lista_com_sinplificad = []
    tabela_atualizar = []
    lista_urls_pesquisa = []
    lista_detalhes_pesquisa =[]
    lista_tres_primera_letras =[]
    contador_por_pais = defaultdict(lambda: {
    "INSERT": 0,
    "NA": 0,
    "ERROR":0,
    "QTINSERT": 0,
    "UPDATE": 0,
    "UPDATE_NAME": 0,

    })

    
    #MONTO SIGLAS UNICAS
    caminho_countress = path_arquivo() 

    lista_coutries = caminho_countress

    caminho_countries = Path('Arquivos/countries_poucos.json')
                
    with open(lista_coutries) as lista_coutrie:
          lista_decodificadas = json.load(lista_coutrie)
            #  print(lista_decodificadas)
          for pais in lista_decodificadas:
              codigo_pais = pais.get('value')
              nome_pais = pais.get('name')
              mapa[codigo_pais] = nome_pais


        
    url_servidor_nationality = self.servidor_nationality
        
    params = f"&resultPerPage={self.qtPage}&page={self.indicePage}"
    
    ClassLogger.logger.info(f"Minha Url chamada no Countries: {url_servidor_nationality}")
        
    with open(caminho_countries, 'r') as lista_countries:
            linha_countries = json.load(lista_countries)

            quantidade_nomes = len(linha_countries)

             

        
    for items in linha_countries:
            siglas_paises = items.get('value')
            if isinstance(siglas_paises, str):
                lista_limpa = [s.strip() for s in siglas_paises.split(',')]
             
                todas_temporaria_siglas.extend(lista_limpa)
            elif isinstance(siglas_paises, list):
                 todas_temporaria_siglas.extend(siglas_paises)


            todas_temporaria_siglas.sort()

        
    tratar_singlas = [sigla for sigla in todas_temporaria_siglas if sigla.strip()]
    
    for list_siglas in todas_temporaria_siglas:
        
            if list_siglas:
                url_completa = f"{url_servidor_nationality}={list_siglas}{params}"
                links_interpol.append(url_completa)
                print(f"País: {list_siglas} | Link API: {url_completa}")
                
                
    
    if links_interpol:
       

        for lote in dividir_lotes(links_interpol , self.batch_size_verify):
                
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    
                        futures_dados = [
                        executor.submit(push_new_resquests, url, self.max_workers)
                        for url in lote
                        ]
                        

                        id_pai = id_insert_return[0] if id_insert_return else None
                        
                        for future in as_completed(futures_dados):
                            try:
                                result = future.result()
                                if isinstance(result, dict):
                                        result['id_geral_'] = id_pai
                                
                                resultados.append(result)
                            except Exception as e:
                                
                                # Você pode optar por adicionar um resultado de erro à lista ou simplesmente ignorar
                                ClassLogger.logger.error(f"Erro ao processar a URL: {e}", exc_info=True)
        

        
        
        # ClassLogger.logger.info(f"MEU RESULTADO COM A RESPOSTA : {resultados}")

        for bloco in resultados:
            if not isinstance(bloco, dict):
                continue  #  pula sem quebrar nada
                
            
            pessoas = bloco.get('_embedded', {}).get('notices', [])
            link = bloco.get('_links', {}).get('self', {}).get('href', {})
            total_api = bloco.get('total')
            if pessoas and total_api > 160:
                lista_com_sinplificad.append({'LINK_PAIS': link, 'total': total_api})
                
                for pessoa in pessoas:
                    pessoa['link_busca'] = link
                    
                    
                    pessoa['total_api'] = total_api


                todas_pessoas.extend(pessoas)
                # lista_com_sinplificad.extend(pessoas)

        #https://ws-public.interpol.int/notices/v1/red?&forename=HA&nationality=RU
        ClassLogger.logger.info(f"MEUS DADOS QUE FORAM MAIOR QUE O LIMITE DO RETORNO DE 160:")
        print(f"{lista_com_sinplificad}")


        

                    

      