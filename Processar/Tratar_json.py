# import aiohttp
# import asyncio
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
from datetime import datetime, date
import time
from Mail.ClassMail import enviar_email_all
from urllib.parse import urlparse, parse_qs
import sys
# from collections import defaultdict
from Conexao import ConectionClass,ConectionPool
from Model.ClassModel import buscar_teste, insert_interpol, update_info_process,search_data_interpol,exists_by_name,insert_base_interpol,update_data_interpol,update_id_interpol,insert_data_interpol_new
from functions.funcoes import remover_acentos, remover_conhetes, tratar_entrada, path_arquivo,dividir_lotes,dividir_lotes_tratar




def trata_json(self,caminho_countries, retorno_api,id_insert_return):
    mapa = {}
    lista = []
    detalhes = []
    tratamento = []
    falhas_ids = []
    lista_urls = []
    todas_pessoas = []
    tabela_atualizar =[]
    nome_traduzido = set()
    lista_paises_unicos = []
    lista_paises_total_api = {}
    id_insert_return_detalhe = []
    grupos_por_pais = defaultdict(list)
    contador_por_pais = defaultdict(lambda: {
    "INSERT": 0,
    "NA": 0,
    "ERROR":0,
    "QTINSERT": 0,
    "UPDATE": 0,
    "UPDATE_NAME": 0,

    })
    # PEGO PAISES NOMES E SIGLAS
    caminho_countress = path_arquivo()
    
    tratamento = retorno_api #RETORNO DO CONSUMO DA API
    
    lista_coutries = caminho_countress
    # ADICIONO DENTRO DA VARIAVEL MAPA PARA ASSOCIAR SIGLAS E NOMES PARA PEGAR OS NOMES DOS PAISES PARA SALVAR NA BASE     
    with open(lista_coutries) as lista_coutrie:
         lista_decodificadas = json.load(lista_coutrie)
         for pais in lista_decodificadas:
             codigo_pais = pais.get('value')
             nome_pais = pais.get('name')
             mapa[codigo_pais] = nome_pais

    #PERCORRO A LISTA PARA PARA PULAR DADOS INVALIDOS E NO CASO COM RETORNOS VAZIOS, E PEGO LINKS PARA CONSULTAS INDIVIDUAIS
    for bloco in tratamento:
        if not isinstance(bloco, dict):
            continue  #  pula sem quebrar nada
              
        
        pessoas = bloco.get('_embedded', {}).get('notices', [])
        link = bloco.get('_links').get('self', {}).get('href', {})
        total_api = bloco.get('total')
        if pessoas:
            for pessoa in pessoas:
                pessoa['link_busca'] = link
                pessoa['total_api'] = total_api
                if link not in lista_paises_unicos:
                   s = urlparse(link)
                   params = parse_qs(s.query)
                   lista_paises_unicos.append(params.get('nationality'))
                   list_teste = params.get('nationality')
                for result_pais in list_teste:
                    lista_paises_total_api[result_pais] = total_api
        else: 
            s = urlparse(link)
            params = parse_qs(s.query)
            lista_paises_unicos.append(params.get('nationality'))
            list_teste = params.get('nationality')
            for result_pais in list_teste:
                lista_paises_total_api[result_pais] = total_api
            

        todas_pessoas.extend(pessoas)
            
   # PEGO OS PAISEIS QUE FORAM SOLITADOS NA BUSCA , REMOVO AS DUPLICATAS PARA REALIZAR A CONTAGEM E APRESENTAR NA TABELA FINAL
    lista_paises_unicos = list(dict.fromkeys(sub[0].strip() for sub in lista_paises_unicos))
    nome_traduzido.update([mapa.get(s, s) for s in lista_paises_total_api])
   

    #  url da busca por id interpol
    with ConectionClass.DbConnect(self.config, auto_commit=False) as conn_status:
         cursor_initil = conn_status.cursor()
         lista_url_interpol = {'periodizacao': self.periodo , 'siglas': '=PARAMENTO DE BUSCA ID DA INTERPO Exe: /111-222', 'url': self.servidor_push_expecifg_id, 'data_captura': datetime.now().strftime("%Y-%m-%d")} 
         id_insert_return_detalhe.append(insert_interpol(self,lista_url_interpol,cursor_initil,conn_status))
         conn_status.commit()
         cursor_initil.close()
          
   
    
    for pessoa in todas_pessoas:
        lista_url = pessoa.get('_links', {}).get('self', {}).get('href')

    
        if lista_url:
           lista_urls.append(lista_url)
    #CHAMO A API QUE VEM NO RETORNO DA CHAMADA DA PAIS, NELE JA ME ROTANA O LINK COM O ID INDIVIDUAL PARA A PESSOA , COLOCANDO O RESULTANDO DENTRO DE DETALHES PARA POPULAR ABAIXO
    

    for lote in dividir_lotes(lista_urls , self.batch_size_verify):
            futures = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                 futures = [
                    executor.submit(push_new_resquests, url,  self.max_workers) for url in lote]

                 for future in as_completed(futures):
                        try:
                            result = future.result()
                            detalhes.append(result) # FAZ AS CONSULTAS INDIVIDUAIS E ADICIONA DENTRO DE DETALHE
                            print("✔ Detalhe recebido") 
                        except Exception as e:
                              ClassLogger.logger.error(f"Erro ao processar a URL: {e}", exc_info=True)
                    
            
  
    id_geral_url_interpol = id_insert_return_detalhe[0] if id_insert_return_detalhe else None

            # Percorre a lista e injeta o ID em cada dicionário retornado
    for item in detalhes:
        if isinstance(item, dict):
           item['id_geral_iterpol'] = id_geral_url_interpol


    pd.set_option('display.max_colwidth', None) # PARA VISUALIZAR EM ESCALA GRANDE 
    for pessoa in todas_pessoas:
       
        url_busca = pessoa.get('link_busca', "")
        s = urlparse(url_busca)
        params = parse_qs(s.query)
        sigla_buscas = params.get('nationality', ['Desconhecido'])[0]
            
        # Guarda a pessoa no grupo desse país
        grupos_por_pais[sigla_buscas].append(pessoa)

      
        
    for sigla_busca in lista_paises_unicos:
        paises_encontrados = set()
        pessoas_do_grupo = grupos_por_pais.get(sigla_busca, [])
        total_dupla = 0
            
    


        for p in pessoas_do_grupo:
            nacionalidades = p.get('nationalities') or []
                # Traduz siglas para nomes usando seu mapa
            nomes = [mapa.get(s, s) for s in nacionalidades]
           
            paises_encontrados.update(nomes)
                
            if len(nacionalidades) > 1:
                  total_dupla += 1


       
        total_por_sigla =  lista_paises_total_api.get(sigla_busca, 0)
       
        linha_tabela = {
                'DATA CAPTURA': datetime.now().strftime("%d/%m/%Y %H:%M"),
                'PAIS_BUSCADO': sigla_busca,
                'TOTAL RETORNO API': total_por_sigla
                }

        tabela_atualizar.append(linha_tabela)

    #remodio do for, pois estava reprocessando depois de conluir
    executar(self, todas_pessoas, detalhes, mapa, contador_por_pais,tabela_atualizar,lista_paises_unicos,id_insert_return[0],id_geral_url_interpol)

def executar(self, todas_pessoas, lista_detalhes_pesquisa, mapa, contador_por_pais,tabela_atualizar,lista_paises_unicos,id_insert_return,id_geral_url_interpol):

    futures = []
    falhas_ids = []
    
    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:

            for de, list_url_person in zip(todas_pessoas, lista_detalhes_pesquisa):
                futures.append(
                    executor.submit(
                        processar_pessoa,self,
                        de,
                        list_url_person,
                        mapa,
                        lista_paises_unicos,
                        id_insert_return
                    )
                )

            for future in as_completed(futures):
                resultado = future.result()
                 # atualiza contador (thread-safe aqui no main)
                if resultado:
                    sigla = resultado.get("sigla", "N/I")

                    match resultado.get("acao"):
                        case "INSERT":
                            contador_por_pais[sigla]["INSERT"] += 1

                        case "UPDATE":
                            contador_por_pais[sigla]["UPDATE"] += 1

                        case "UPDATE_NAME":
                            contador_por_pais[sigla]["UPDATE_NAME"] += 1

                        case "ERROR":
                            falhas_ids.append(resultado['dados_error'])
                            contador_por_pais[sigla]["ERROR"] += 1

                        case _:
                            contador_por_pais[sigla]["NA"] += 1


            print(f"MEU CONTADOR PREENCHDIDO {contador_por_pais}")
            ClassLogger.logger.info(f"MEU CONTADOR PREENCHDIDO {contador_por_pais}")

    # sem_inset = True        
    
    for linha in tabela_atualizar:
        pais = linha['PAIS_BUSCADO']
                #MUNDAR PRA A
        linha['QTA A INSERIR'] = contador_por_pais[pais]["INSERT"]
        linha['QTA J/N BASE'] = contador_por_pais[pais]["NA"]
        linha['QTA ERROR'] = contador_por_pais[pais]["ERROR"]
        linha['QTA INSERIDO'] = contador_por_pais[pais]["INSERT"]

        # if contador_por_pais[pais]["INSERT"] > 0:
        #     sem_inset = False
    for linha in tabela_atualizar:
        print(f"minha linhas {linha['QTA A INSERIR']}")
        print(f"minha linhas {type(linha['QTA A INSERIR'])}")
        if linha['QTA A INSERIR'] == 0:
            obs = f"SEM ALTERACAO NOS DADOS {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            obs_interpol = f"SEM CONSULTA INDIVIDUAL {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

            alter_status(self, id_insert_return, obs)
            alter_status(self, id_geral_url_interpol, obs_interpol)


    pd.set_option('display.max_rows', 100)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)  
    
    minha_tabela_montada = pd.DataFrame(tabela_atualizar)
    minha_tabela_montada = minha_tabela_montada.fillna(0)
    print(f"Minha quantidade a ser processada {len(minha_tabela_montada)}")
    print(f"MINHA TABELA PARA ATUALIZAR {minha_tabela_montada}")
    
    if falhas_ids is not None:
       tabela_error = pd.DataFrame(falhas_ids)
       tabela_error = tabela_error.fillna(0) 
       convertida_error =  tabela_error.to_html(index=False, border=1, justify='center')
       corpo_error = f"Lista de dados com error :<br> {convertida_error}"
    
        

    
    convertida = minha_tabela_montada.to_html(index=False, border=1, justify='center')
    
    corpo = f"""
        <h2 style="color:green;">Captura dos dados interpol</h2>
        <p>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

        {convertida}
        """

    html_final = f"""
        <html>
        <body>

        {corpo}

        <hr>

        {corpo_error if corpo_error else "<p>Sem erros encontrados</p>"}

        </body>
        </html>
        """

    result_email = enviar_email_all(html_final)




def alter_status(self, id, obs = None):
    with ConectionClass.DbConnect(self.config, auto_commit=False) as conn_status:
          cursor_initil = conn_status.cursor()
          if obs is None:
                lista_update = {'status': self.true, 'alter_id': id ,'obs' : None} 
          else: 
                lista_update = {'status': self.true, 'alter_id': id, 'obs' : obs}
                update_info_process(self,lista_update,cursor_initil,conn_status)
          conn_status.commit()
          cursor_initil.close()

       


  
def processar_pessoa(self, de, list_url_person, mapa,lista_paises_unicos,id_insert_return):
    falha_ids =[]
    
    
    with self.db.get_connection() as conn:

        try:
            lista_paises = de.get('nationalities') or []
            sigla = lista_paises[0] if lista_paises else "N/I"
            lista_paises_chaves = de.get('nationalities') or []

            entity_id = de.get('entity_id')
            entity_id = entity_id.replace('/', '-') if entity_id else None

            name_person = remover_acentos(
                f"{de.get('forename')} {de.get('name')}"
            ).strip()

            data_ajustada = tratar_entrada(de.get('date_of_birth'))

            naturalidade = (
                list_url_person.get('place_of_birth')
                or mapa.get(list_url_person.get('country_of_birth_id'))
                or "N/I"
            ).upper()

            #removo os acentos
            naturalidade = remover_acentos(naturalidade) 

            thumbnail = de.get('_links', {}).get('thumbnail', {}).get('href') if de.get('_links', {}).get('thumbnail', {}).get('href') else "N/I"

            pais_procurado = [
                mapa.get(w.get('issuing_country_id'), w.get('issuing_country_id'))
                for w in list_url_person.get('arrest_warrants', [])
            ]

            pais_procurado = ', '.join(pais_procurado).upper() if pais_procurado else "N/I"

            data_captura = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            person_singla = next((p for p in lista_paises_chaves if p in lista_paises_unicos), 'N/I')
        
            # ---------------------------
            #  1. BUSCA POR ID
            # ---------------------------
            if entity_id:
                exist_id = search_data_interpol(conn, entity_id)
                print(f'qual o resultando que tenho aqui {exist_id}')


                if exist_id:
                    update_data_interpol(
                        conn,
                        entity_id,
                        naturalidade,
                        thumbnail,
                        pais_procurado,
                        data_captura
                    )

                    return {"acao": "UPDATE", "sigla": person_singla}

            # ---------------------------
            #  2. BUSCA POR NOME
            # ---------------------------
            exist_name = exists_by_name(conn, name_person)

            if exist_name:
                update_id_interpol(conn, name_person, entity_id)
                return {"acao": "UPDATE_NAME", "sigla": person_singla}

            # ---------------------------
            #  3. INSERT
            # ---------------------------
            lista_paises_nome = [mapa.get(p, p) for p in lista_paises]
            pais_limpo = ','.join(lista_paises_nome) if lista_paises_nome else "N/I"

            sexo = list_url_person.get('sex_id')

            crime_lista = [
                remover_acentos(w.get('charge', '')).strip()
                for w in (list_url_person.get('arrest_warrants') or [])
            ]
            crime = ", ".join(crime_lista) if crime_lista else "N/I"

            idioma = ", ".join(
                item.strip("[] ").strip()
                for item in (list_url_person.get('languages_spoken_ids') or [])
            ) or "N/I"

            result_insert = insert_data_interpol_new(
                conn,
                nome=name_person,
                nascimento=data_ajustada,
                nacionalidade=pais_limpo.upper(),
                naturalidade=naturalidade,
                id_interpol=entity_id,
                sexo=sexo,
                acusacao=crime.upper(),
                idioma=idioma.upper(),
                thumbnail=thumbnail or "N/I",
                data_consulta=datetime.now().strftime("%Y-%m-%d"),
                hora_consulta=datetime.now().strftime("%H:%M:%S"),
                person_sigla_unico=person_singla,
                pais_procurado=pais_procurado
            )
              
            if result_insert['status'] == "sucesso":

                alter_status(self, id_insert_return)
                obs_interpol_success = 'SUCESSO EM CONSULTAR OS IDS INDIVIDUAL INTERPOL'
                alter_status(self, id_insert_return,obs_interpol_success)
                                    # inser_new_registro +=1
                    # contador_por_pais[result_insert['person_sigla_unico']]["QTINSERT"] += 1
                return {"acao": "INSERT", "sigla": result_insert['person_sigla_unico']}
            else:
                falha_ids.append(result_insert)
                return {"acao": "ERROR", "sigla": result_insert['person_sigla_unico'] ,"dados_error" : falha_ids}
                                    # falha_ +=1
                
           

            # return {"acao": "INSERT", "sigla": person_singla}

        except Exception as e:
            print(f"Erro ao processar {entity_id}: {e}")
            return {"acao": "ERRO", "sigla": person_singla}
