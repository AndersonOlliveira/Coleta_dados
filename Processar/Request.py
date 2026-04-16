

from typing import Dict, List,Optional, Tuple
from Logs import ClassLogger
import time
import json
from curl_cffi import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from Mail.ClassMail import enviar_email_all
from functions.funcoes import dividir_lotes
from Conexao import ConectionClass
from Model.ClassModel import buscar_teste, insert_interpol
from urllib.parse import urlparse, parse_qs
import random
import threading 
from collections import Counter, defaultdict
from Processor.ClassResquest import RateLimiter


def push_request(self,countries = None, url_new = None):
    resultados = []
    url_completa = []
    links_interpol = []
    id_insert_return = []
    todas_temporaria_siglas = []

    if url_new is not None:
         
         links_interpol.append(url_new)

    elif countries is not None:
        
       
        url_servidor_nationality = self.servidor_nationality
        
        params = f"&resultPerPage={self.qtPage}&page={self.indicePage}"

        
        ClassLogger.logger.info(f"Minha Url chamada no Countries: {url_servidor_nationality}")
        
        with open(countries, 'r') as lista_countries:
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
        

    
        with ConectionClass.DbConnect(self.config, auto_commit=False) as conn_status:
             cursor_initil = conn_status.cursor()
             lista_insert = {'periodizacao': self.periodo ,'siglas' : (', '.join(tratar_singlas)) , 'url': url_servidor_nationality, 'data_captura': datetime.now().strftime("%Y-%m-%d")} 
             id_insert_return.append(insert_interpol(self,lista_insert,cursor_initil,conn_status))
             
           

             conn_status.commit()
            
             cursor_initil.close()
             time.sleep(0.5)


        for list_siglas in todas_temporaria_siglas:
        
            if list_siglas:
                #passando uma letra a mais ele passar por pardao pegar tudo 
                     
                

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

                    

        return resultados, id_insert_return




buffer_mensagens_emails =[]
timer_ativo = False
lock_error = threading.Lock()

def push_new_resquests(url,max_retries):
        global buffer_mensagens_emails, timer_ativo, lock_error 
        
        JANELA_COLETA_SEGUNDOS = 30
        resposta = ""
        erro = False
        lock_erros = threading.Lock()
        acumulo_erros = defaultdict(lambda: {
        "ERROR":0,
        })
    
        session = requests.Session()
        rate_limiter = RateLimiter()
       

        session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Connection": "keep-alive"
        })
      

        for tentativa in range(max_retries):
            try:
                rate_limiter.wait()

                navegador = random.choice([
                            "chrome110",
                            # "chrome116",
                            # "edge101"
                        ])

               
                
                response = session.get(
                        url,
                        impersonate="chrome110",
                        timeout=15
                )

            
                if response.status_code == 403:
                    print(f" 403 detectado (tentativa {tentativa+1})")
                    msg_custom = f"Acesso Negado (403). Verifique permissões ou Headers. Detalhes: {url}"
                    
                    with lock_erros:
                        erro = True
                        acumulo_erros[url]["ERROR"] += 1
                        buffer_mensagens_emails.append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg_custom}")

                    # Se não houver um timer rodando, inicia um agora
                        if not timer_ativo:
                            timer_ativo = True
                                 # Inicia uma thread separada que vai esperar X segundos antes de enviar tudo
                            threading.Timer(JANELA_COLETA_SEGUNDOS, disparar_email_agrupado).start()

                    ClassLogger.logger_urls.error(f"Erro 403: {msg_custom}")

                    rate_limiter.increase_penalty()
                    time.sleep(2)

                    continue

                rate_limiter.decrease_penalty()
                response.raise_for_status()
                resposta = response.json()


                
            except requests.exceptions.Timeout:
                resposta = f"TIMEOUT: Requisição excedeu 5 minutos {url}"
                erro = True
                ClassLogger.logger.error(f"Timeout na requisição: {url}")
            except requests.exceptions.HTTPError as e:
                # Tenta extrair o corpo da resposta do erro (ex: {"error": "token_expired"})
                
                try:
                    detalhes_servidor = e.response.json()
                except:
                    detalhes_servidor = e.response.text

                status = e.response.status_code
                msg_custom = f"Erro HTTP {status}: {detalhes_servidor}"
               
                s = urlparse(url)
               
                    
                # Extrair a URL base até '/red/'
                url_base = f"{s.scheme}://{s.netloc}/notices/v1/red/"
                
                
                # Extrair o ID após '/red/'
                path = s.path
                if '/red/' in path:
                    id_interpol = path.split('/red/')[-1]
                    print(f"ID extraído: {id_interpol}")
                else:
                    id_interpol = None
                
                if status == 403:
                    msg_custom = f"Acesso Negado (403). Verifique permissões ou Headers. Detalhes: {url}"
                    acumulo_erros[url_base]["ERROR"] += 1
                    ClassLogger.logger.error(f"Erro 403: {msg_custom}")
                elif status == 404 and url_base == "https://ws-public.interpol.int/notices/v1/red/":
                    
                    # if url_base == "https://ws-public.interpol.int/notices/v1/red/":
                   
                    erro = False
                    resposta = {"message":False,"id_interpol": id_interpol}
                    return resposta
                    
                else:
                    msg_custom = f"Recurso não encontrado (404). Verifique a URL: {url}"

                resposta = f"ERRO: {msg_custom}"
                erro = True
                ClassLogger.logger.error(f"Erro na requisição: {msg_custom}")

            except requests.exceptions.ConnectionError:
                    resposta = f"ERRO: Falha de conexão. Verifique sua internet ou o status do servidor. URL: {url}"
                    erro = True
                    ClassLogger.logger.error(f"Erro de Conexão: Verifique o host. URL: {url}")

            except requests.exceptions.RequestException as e:
                
                    resposta = f"ERRO GENÉRICO: {str(e)}"
                    erro = True
                    ClassLogger.logger.error(f"Erro inesperado: {str(e)}")


        

        if acumulo_erros:
            corpo_email  = montar_email_erros(acumulo_erros)
            
            

        if erro:
           
            enviar_email_all(resposta)
            


        
        return resposta

def rest_interpol_id(url):

    print(f"ACESSANDO A URL PARA CONSULTA?")


def montar_email_erros(acumulo_erros):
    linhas = [" Resumo de erros 403:\n"]

    for url, dados in acumulo_erros.items():
        linhas.append(f" {url}")
        

    return "\n".join(linhas)



#funcao para agrupar e enviar os erros caso tenham mais erros
def disparar_email_agrupado():
         with lock_error:
                if buffer_mensagens_emails:
                    qtd = len(buffer_mensagens_emails)
                    corpo = "\\n".join(buffer_mensagens_emails)
                    
                
            
                
                
                enviar_email_all(f"<h2>Resumo de erros 403</h2><p>{corpo.replace('', '<br>')}</p>")
                
                buffer_mensagens_emails.clear() # Limpa para o próximo lote
         timer_ativo = False




def push_new_resquest(url, time_sleps):

    try:
        

        time.sleep(time_sleps)

        agora = datetime.now()
        

        return agora

    except Exception as e:
        print(f"Erro: {e}")