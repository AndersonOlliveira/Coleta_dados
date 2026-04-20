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








def process_from_name(self):
    #pego os dados e faco a busca
    ClassLogger.logger.info(f"INIICIANDO A BUSCA E ATUALIZACAO DOS DADOS POR NOME")
    des = []
    mapa = {}
    siglas = []
    id_insert_return =[]
    todas_pessoas =[]
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
                
    with open(lista_coutries) as lista_coutrie:
          lista_decodificadas = json.load(lista_coutrie)
            #  print(lista_decodificadas)
          for pais in lista_decodificadas:
              codigo_pais = pais.get('value')
              nome_pais = pais.get('name')
              mapa[codigo_pais] = nome_pais
    # lista_get_name = dict

    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
          future_busca = executor.submit(get_lista_name_base_interpol, self)
          lista_get_name = future_busca.result()
        #  print(f"MINHA LISTA DO GET {lista_get_name}")
          if lista_get_name:
                for lista_name in lista_get_name:
                    nome = remover_acentos(remover_conhetes(lista_name.get('nome', ''))).strip() if lista_name.get('nome') else None
                

                
                    if nome:
                        tres_primeiras = nome[:3].upper()
                        lista_tres_primera_letras.append(tres_primeiras)
                        params = f"&resultPerPage={self.qtPage}&page={self.indicePage}"
                        lista_singlas_name = f"{self.servidor_get_from_name}={tres_primeiras}{params}"
                        siglas.append(lista_singlas_name)

    # params = f"&resultPerPage={self.qtPage}&page={self.indicePage}"
    # lista_singlas_name = f"{self.servidor_get_from_name}=KAB{params}"
    # siglas.append(lista_singlas_name)                    

            # remove duplicados
          siglas_unicas = list(set(siglas))
          letras_unicas = list(set(lista_tres_primera_letras))

        
    with ConectionClass.DbConnect(self.config, auto_commit=False) as conn_status:
         cursor_initil = conn_status.cursor()
         lista_insert = {'periodizacao': self.periodo ,'siglas' : (', '.join(letras_unicas)) , 'url': self.servidor_get_from_name, 'data_captura': datetime.now().strftime("%Y-%m-%d")} 
         print(f"minha lista para insert {lista_insert}")
         id_insert_return.append(insert_interpol(self,lista_insert,cursor_initil,conn_status))
             
           

         conn_status.commit()
            
         cursor_initil.close()
         time.sleep(0.5)

    # print(f"MINHA LISTA DE URL PARA BUSCA POR NOME {len(siglas_unicas)}")
    
    # return
    # print(f"MINHA QUANTIDADE A SER BUSCADA DE NOMES UNICOS {len(siglas_unicas)}")
    # for lote in dividir_lotes(siglas_unicas, self.batch_size_verify):
    #     print(f"MINHA LISTA DE URL PARA BUSCA POR NOME {len(lote)}")
    #     # print(f"meu lote de pesquisa {lote}")
       
    # return
    for lote in dividir_lotes(siglas_unicas , self.batch_size_verify):
        futures = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
        #     des = list(executor.map(
        #     lambda url: push_new_resquests(url, self.time_sleps),
        #     siglas_unicas
        # ))
            futures = [
                    executor.submit(push_new_resquests, url,  self.max_workers)
                    for url in lote
                ]

            for future in as_completed(futures):
                    try:
                        result = future.result()
                        des.append(result)
                    except Exception as e:
                        ClassLogger.logger.error(f"Erro ao processar a URL: {str(e)}")
    
    
    
    # print(f"MEU RETORNO DO DES {des}")
    # return 
    if des:
        ClassLogger.logger.info(f"MINHA LISTA DE RETORNO DO DES {len(des)}")
        try:

            # return 
            for bloco in des:
                print(f"DEBUG BLOCO: {bloco} | tipo: {type(bloco)}")
                if not isinstance(bloco, dict):
                    print(f"Bloco inválido: {bloco}")
                
                # if isinstance(bloco, str):
                #    bloco = json.loads(bloco)
                pessoas_detalhes = bloco.get('_embedded', {}).get('notices', [])
            
                todas_pessoas.extend(pessoas_detalhes)
        except Exception as e:
            
            ClassLogger.logger.error(f"Erro ao processar os Dados BUSCA POR 3 PRIMEIRA LETRAS : {str(e)} ::: DADOS ERROS{des}")
        
        
        print(f"minha lisata {len(todas_pessoas)}")
        print(f"MINHA SINGLAS PARA pessoas_detalhes {pessoas_detalhes}")
        # return 
        # return
        for pessoa in todas_pessoas:
            lista_url = pessoa.get('_links', {}).get('self', {}).get('href')

            print(f"Minha lista de url individual primeira chamada {lista_url}")
        
            if lista_url:
               lista_urls_pesquisa.append(lista_url)
        #CHAMO A API QUE VEM NO RETORNO DA CHAMADA DA PAIS, NELE JA ME ROTANA O LINK COM O ID INDIVIDUAL PARA A PESSOA , COLOCANDO O RESULTANDO DENTRO DE DETALHES PARA POPULAR ABAIXO
        # return
        
    
        print(f"Minha lista de url {json.dumps(lista_urls_pesquisa, indent=4)}")
        print(f"Minha lista de url {len(lista_urls_pesquisa)}")
        # return 
    
        for lote in dividir_lotes(lista_urls_pesquisa , self.batch_size_verify):
             with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            #     lista_detalhes_pesquisa = list(executor.map(
            #     lambda url: push_new_resquests(url, self.time_sleps),
            #     lista_urls_pesquisa
            # ))
                futures = [
                    executor.submit(push_new_resquests, url, self.max_workers)
                    for url in lote
                ] 
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        lista_detalhes_pesquisa.append(result)
                    except Exception as e:
                        ClassLogger.logger.error(f"Erro ao processar a URL: {str(e)}")
                        
            
        # print(f"Detalhes das pessoas: {json.dumps(lista_detalhes_pesquisa, indent=4)}")

        # return
    #remodio do for, pois estava reprocessando depois de conluir
    executar(self, todas_pessoas, lista_detalhes_pesquisa, mapa, contador_por_pais,tabela_atualizar,id_insert_return[0],letras_unicas)

def executar(self, todas_pessoas, lista_detalhes_pesquisa, mapa, contador_por_pais,tabela_atualizar,id_insert_return,letras_unicas):

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
    
    for pais_sigla in letras_unicas:
        tabela_atualizar.append({
            'PAIS_BUSCADO': pais_sigla,
            'QTA J/N BASE': 0
    })

    new_tabel = []

    for pais, totais in contador_por_pais.items():
        
        nova_linha = {
                'DATA CAPTURA': datetime.now().strftime("%d/%m/%Y %H:%M"), 
                'PAIS_BUSCADO': pais,
                'QTA SINGLAS': totais["INSERT"],
                'QTA J/N BASE':  totais["NA"],
                'QTA ATUALIZADO':  totais["UPDATE"],
                'QTA ERROR':     totais["ERROR"], 
                'QTA INSERIDO': max(0, totais["INSERT"] - totais["NA"])
            }
            
        new_tabel.append(nova_linha)

        
    tabela_atualizar = new_tabel



    for linha in tabela_atualizar:
            print(f"minha linhas {linha['QTA INSERIDO']}")
            print(f"minha linhas {type(linha['QTA INSERIDO'])}")
            if linha['QTA INSERIDO'] == 0:
                obs_interpol = f"SEM ALTERACAO NOS DADOS TRES PRIMEIRA LETRAS  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                  
                alter_status(self, id_insert_return,obs_interpol)
                ClassLogger.logger.info(f"SEM ALTERACAO NOS DADOS {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
                ClassLogger.logger.info(f"SEM CONSULTA INDIVIDUAL {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")



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
        <h2 style="color:green;">Busca dados por nome por 3 primeiras letras </h2>
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

    return enviar_email_all(html_final)



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



def processar_pessoa(self, de, list_url_person, mapa,id_insert_return):
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

            thumbnail = de.get('_links', {}).get('thumbnail', {}).get('href')

            pais_procurado = [
                mapa.get(w.get('issuing_country_id'), w.get('issuing_country_id'))
                for w in list_url_person.get('arrest_warrants', [])
            ]

            pais_procurado = ', '.join(pais_procurado).upper() if pais_procurado else "N/I"

            data_captura = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            person_singla = lista_paises_chaves[0] if lista_paises_chaves else 'N/I'
            # person_singla = next((p for p in lista_paises_chaves if p in lista_paises_unicos), 'N/I')
        
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
                person_sigla_unico=','.join([person_singla]),
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
