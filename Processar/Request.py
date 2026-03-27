

from typing import Dict, List,Optional, Tuple
from Logs import ClassLogger
import time
import json
from curl_cffi import requests
from datetime import datetime






def push_request(self,countries):

     
    url = self.servidor
    url_servidor_nationality = self.servidor_nationality
    tempo_chamada = 50
        
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    params =   f"&resultPerPage={self.qtPage}&page={self.indicePage}"
    
    todas_temporaria_siglas= []
    links_interpol = []
   
    ClassLogger.logger.info(f"Minha Url: {url_servidor_nationality}")

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




    print(f" minha quantidade de url : {len(links_interpol)}")
    print(f"Link API: {links_interpol}")


    for urls in links_interpol:
       
        try:
            
            # response = requests.get(url, impersonate="chrome110", timeout=(30))
            agora = datetime.now()
            print(f"Agora: {agora}")
            print(f"Chamada a cada : dd{agora}")
            print(f"TTempo de parada a cada chamada:  {tempo_chamada}")
            time.sleep(tempo_chamada)
        except requests.exceptions.Timeout:
            #             retorno_interpol = "TIMEOUT: Requisição excedeu 5 minutos"
            #              erro = True
             ClassLogger.logger.error(f"Timeout na requisição: {url}")
        except requests.exceptions.RequestException as e:
            #    retorno_interpol = f"ERRO: {str(e)}"
             #   erro = True
              ClassLogger.logger.error(f"Erro na requisição: {str(e)}")


    #pensar em um tempo para chamada a cada url


            #retorno_interpol = ""

    #try:
               # response = requests.get(url, impersonate="chrome110", timeout=(30))
                # response = requests.get(url, headers=headers,timeout=(300, 300))
             #   response.raise_for_status() 
                # resposta = response.text
                # print(resposta.content.decode())
              
         #       retorno_interpol = response.json()
                
                # print(dados)
                # print(json.dumps(json_data, indent=4)) 

                # Retorna para quem chamou a função
                # print(json_data) 
    
              #  erro = False
 #   except requests.exceptions.Timeout:
   #             retorno_interpol = "TIMEOUT: Requisição excedeu 5 minutos"
  #              erro = True
   #             ClassLogger.logger.error(f"Timeout na requisição: {url}")

   # except requests.exceptions.RequestException as e:
            #    retorno_interpol = f"ERRO: {str(e)}"
             #   erro = True
              #  ClassLogger.logger.error(f"Erro na requisição: {str(e)}")


    # print(f"MEUS DADOS DE RESPOSTA {resposta}")


    #return retorno_interpol