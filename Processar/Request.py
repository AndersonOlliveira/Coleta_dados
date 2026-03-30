

from typing import Dict, List,Optional, Tuple
from Logs import ClassLogger
import time
import json
from curl_cffi import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor



def push_request(self,countries = None, url_new = None):
    todas_temporaria_siglas= []
    links_interpol = []
    url_completa = []
    tempo_chamada = self.time_sleps


    # print(f"ESTOU ACESSANDO OS DADOS")
    # print(f"tipo: {type(countries)}")
    # print(f"MEU LINK tipo: {url_new}")

    if url_new is not None:
         print(f"ESTOU ACESSANDO OS DADOS dentro do if")
         print(f"MEU LINK tipo dentro do if: {url_new}")
         links_interpol.append(url_new)

    elif countries is not None:
        print(f"PASSO AQUI NESTE IF PARA VER SE O PROBLEMA É A URL NOVA")
        url = self.servidor
        url_servidor_nationality = self.servidor_nationality
        
        params = f"&resultPerPage={self.qtPage}&page={self.indicePage}"
        
        ClassLogger.logger.info(f"Minha Url chamada no Countries: {url_servidor_nationality}")
        
        with open(countries, 'r') as lista_countries:
             linha_countries = json.load(lista_countries)

             quantidade_nomes = len(linha_countries)

             print(f"A lista contém {quantidade_nomes} nomes/países.")

        # print(f"minha linha?: {linha_countries}")
        for items in linha_countries:
            siglas_paises = items.get('value')
            if isinstance(siglas_paises, str):
                lista_limpa = [s.strip() for s in siglas_paises.split(',')]
             
                todas_temporaria_siglas.extend(lista_limpa)
            elif isinstance(siglas_paises, list):
                 todas_temporaria_siglas.extend(siglas_paises)


            todas_temporaria_siglas.sort()
    
        for list_siglas in todas_temporaria_siglas:
        
            if list_siglas:
                url_completa = f"{url_servidor_nationality}={list_siglas}{params}"
                links_interpol.append(url_completa)
                # print(f"País: {list_siglas} | Link API: {url_completa}")


   
    if links_interpol:
        print(f" minha quantidade de url : {len(links_interpol)}")
        print(f"Link API: {links_interpol}")
            
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            resultados = list(executor.map(
                lambda url: push_new_resquests(url, self.time_sleps),
                links_interpol
            ))

        # print(f"Finalizado : {resultados}")

        return resultados


def push_new_resquest(url, time_sleps):

    try:
        print(f"Chamando: {url}")

        time.sleep(time_sleps)

        agora = datetime.now()
        print(f"Agora: {agora}")

        return agora

    except Exception as e:
        print(f"Erro: {e}")


def push_new_resquests(url, time_sleps):
    
        resposta = ""


        try:
                
                response = requests.get(url, impersonate="chrome110", timeout=(30))
                agora = datetime.now()
                print(f"Agora: {agora}")
                print(f"Chamada a cada : dd{agora}")
                print(f"TTempo de parada a cada chamada:  {time_sleps}")
                time.sleep(time_sleps)
                response.raise_for_status()
                resposta = response.json()
        except requests.exceptions.Timeout:
                retorno_interpol = "TIMEOUT: Requisição excedeu 5 minutos"
                erro = True
                ClassLogger.logger.error(f"Timeout na requisição: {url}")
        except requests.exceptions.RequestException as e:
                retorno_interpol = f"ERRO: {str(e)}"
                erro = True
                ClassLogger.logger.error(f"Erro na requisição: {str(e)}")

        return resposta

def rest_interpol_id(url):

    print(f"ACESSANDO A URL PARA CONSULTA?")