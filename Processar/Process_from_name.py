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
from Model.ClassModel import buscar_teste, insert_interpol, update_info_process,search_data_interpol,exists_by_name,insert_base_interpol,update_data_interpol,update_id_interpol,get_lista_name_base_interpol
from functions.funcoes import remover_acentos, remover_conhetes, tratar_entrada, path_arquivo, dividir_lotes








def process_from_name(self):
    #pego os dados e faco a busca
    ClassLogger.logger.info(f"INIICIANDO A BUSCA E ATUALIZACAO DOS DADOS POR NOME")
    des = []
    mapa = {}
    lista = []
    siglas = []
    falhas_ids =[]
    id_insert_return =[]
    ids_sucesso = []
    todas_pessoas =[]
    tabela_atualizar = []
    lista_urls_pesquisa = []
    lista_detalhes_pesquisa =[]
    lista_tres_primera_letras =[]
    contador_por_pais = defaultdict(lambda: {
    "INSERT": 0,
    "NA": 0,
    "ERROR":0,
    "QTINSERT": 0
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

        
        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                with self.db.get_connection() as conn:
                    for de, list_url_person in zip(todas_pessoas, lista_detalhes_pesquisa):
                        print(f"meu resultado do for?{de}")
                        print(f"meu resultado do list_url_person?{list_url_person}")
                        lista_paises_chaves = de.get('nationalities') or []
                        # tratar a data que veio a veio quebrada
                        data_ajustada = de.get('date_of_birth') if de.get('date_of_birth') else None
                        print(f'MINHA DATA PRIMEIRO ESTAGIIO:: data_ajustada {data_ajustada}')
                        data_ajustada = tratar_entrada(data_ajustada)
                        print(f'MINHA DATA:: data_ajustada {data_ajustada}')
                        # lista_paises_unicos.append(lista_paises_chaves)
                        print(f"meu resultado do lista_paises_chaves?{lista_paises_chaves}")
                        person_singla = lista_paises_chaves[0] if lista_paises_chaves else 'N/I'
                        entity_id = de.get('entity_id').replace('/','-') if de.get('entity_id') else None
                        name_person = remover_acentos("{} {}".format(de.get('forename'), de.get('name'))).strip()
                        naturalidade = (list_url_person.get('place_of_birth') or mapa.get(list_url_person.get('country_of_birth_id')) or "N/I").upper()
                        thumbnail = de.get('_links', {}).get('thumbnail', {}).get('href') 
                        print(f"dados encontrados: {de.get('place_of_birth')} + NOME PESON {name_person} + naturalidade dois: {naturalidade}  ")
                        pais_procurado = [mapa.get(wanted.get('issuing_country_id'),wanted.get('issuing_country_id')) for wanted in list_url_person.get('arrest_warrants', [])]
                        pais_procurado = ', '.join(pais_procurado).upper() if pais_procurado else "N/I"
                        print(f'meu pais procurado {pais_procurado}')
                        data_captura = datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
                        exist_id = False
                        exist_name = False
                       
                                # return
                                #pego os ids da interpol para verificar só que vou ter uma dupla verificacao, pelo o id e pelo o nome
                                # person_singla = list(set(lista_paises_chaves) & set(lista_paises_unicos)) #COM O METODO SET
                        if entity_id:
                            # future_busca = executor.submit(search_data_interpol, conn, entity_id)
                            future_busca = executor.submit(search_data_interpol, self, entity_id)
                            exist_id = future_busca.result()
                            print(f"QUAL E MEU RESULADO AQUI? {exist_id}")

                            if exist_id: # aqui atualizo sempre que vinher os dados
                                print(f"MEU ID EXISTE NA BASE {entity_id} || nome: {name_person}  vou atualizar os dados COM A NACIONALIDE ALTERADA PARA {naturalidade}  E O PAIS PROCURADO {pais_procurado}")
                                executor.submit(update_data_interpol, conn, entity_id, naturalidade,thumbnail,pais_procurado,data_captura)

                        if not exist_id:
                            #colocar theads aqui
                            print(f"QUAIS OS IDS BUSCADO {entity_id} ||| nome: {name_person}")
                            future_busca_name = executor.submit(exists_by_name,self,name_person)
                            exist_name = future_busca_name.result()
                        if exist_name:
                            #faco o update para o id da interpol para a busca ser mais acertiva 
                            executor.submit(update_id_interpol, conn, name_person , entity_id)

                        if not exist_id and not exist_name:
                            print(f"VOU INSEIR O ID {entity_id} | nome: {name_person}")

                                        
                            contador_por_pais[person_singla]["INSERT"] += 1
                            print(f"VOU INSEIR O ID {entity_id} | nome: {name_person} + {person_singla}")
                                        
                                        # novos_registros +=1
                            lista_paises = de.get('nationalities') or []
                            print(f"{lista_paises} MEUS DADOSSS")
                            nomes_paises = [mapa.get(pais, pais) for pais in lista_paises]
                            print(f"{nomes_paises} depois do get?")
                            pais_limpo = ','.join(nomes_paises) if nomes_paises else "N/I"
                            sexo = list_url_person.get('sex_id') if list_url_person else None
                            # crime =  [remover_acentos(warrant.get('charge')).strip() for warrant in de.get('arrest_warrants', [])] if de else None
                            crime_lista = [remover_acentos(warrant.get('charge', '')).strip() for warrant in (list_url_person.get('arrest_warrants') or [])]
                            
                            crime = ", ".join(crime_lista) if crime_lista else "N/I"
                            # idiona = [remover_conhetes(lang) for lang in de.get('languages_spoken_ids', [])] if de else None
                            idiona = ", ".join(item.strip("[] ").strip() for item in list_url_person.get('languages_spoken_ids', []) or []) if list_url_person and list_url_person.get('languages_spoken_ids') else "N/I"
                                        

                                    
                            lista.append({
                                 'nome_completo': name_person,
                                 'data_nascimento': data_ajustada,
                                 'nacionalidade': pais_limpo.upper(),
                                 'naturalidade': naturalidade.upper(),
                                 'id_interpol': entity_id,
                                 'sexo': sexo, 
                                 'acusacao': crime.upper(),
                                 'idiona': idiona.upper(),
                                 'thumbnail': thumbnail if thumbnail else "N/I", #COMENTEADO PARA NÃO APRESENTAR EM TELA,
                                  'data_consulta': datetime.now().strftime("%Y-%m-%d"),
                                  'hora_consulta': datetime.now().strftime("%H:%M:%S"),
                                  'country_wanted': pais_procurado,
                                  'person_sigla_unico' : ','.join([person_singla])
                                })

                        else:
                            print(f"vou pular {entity_id} || nome: {name_person}  que pais ???{lista_paises_chaves}")
                            print(f"vou pular {entity_id} | nome: {name_person} + {person_singla}")
                            contador_por_pais[person_singla]["NA"] +=1

        
        except Exception as e:
            ClassLogger.logger.error(f"Erro ao processar entidade: {str(contador_por_pais)}")


    # Popular tabela_atualizar com os países únicos encontrados
    for pais_sigla in letras_unicas:
        tabela_atualizar.append({
            'PAIS_BUSCADO': pais_sigla,
            'QTA SINGLAS': 0,
            'QTA J/N BASE': 0
        })

    for linha in tabela_atualizar:
            pais = linha['PAIS_BUSCADO']
                #MUNDAR PRA A
            linha['QTA SINGLAS'] = contador_por_pais[pais]["INSERT"] - contador_por_pais[pais]["NA"]
            linha['QTA J/N BASE'] = contador_por_pais[pais]["NA"]


    pd.set_option('display.max_rows', 100)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    df = pd.DataFrame(lista)
    tabela_atualizar_ = pd.DataFrame(tabela_atualizar)
    
    
    print(f"Minha quantidade a ser processada {len(df)}")
    print(f"MINHA TABELA PARA ATUALIZAR {tabela_atualizar_ }")
    print(f"Minha lista do contador {len(df)}")
        
    # ClassLogger.logger.info(f"MINHA LISTA DO DF json {json.dumps(lista, indent=4)}")
    # ClassLogger.logger.info(f"MINHA LISTA DO DF {tabela_atualizar_.to_dict(orient='records')}")

    # return

    if len(df) > 0:

            # alter_status(self, id_insert_return[0])
        obs_interpol_success = 'SUCESSO EM CONSULTAR OS FORENAME INTERPOL'
        alter_status(self, id_insert_return[0],obs_interpol_success)
            
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            
            futures = [
                    executor.submit(insert_base_interpol,self,registro)
                        for registro in lista
                ]
                        
            for future in as_completed(futures):
                result = future.result()


                print(f"tenho acesso as siglas {result}")
                    # print(f"tenho acesso as siglas {result['person_sigla_unico']}")
                            
                if result['status'] == "sucesso":
                                # inser_new_registro +=1
                        contador_por_pais[result['person_sigla_unico']]["QTINSERT"] += 1
                        ids_sucesso.append(result)
                else:
                        falhas_ids.append(result)
                                    # falha_ +=1
                        contador_por_pais[result['person_sigla_unico']]["ERROR"] += 1


    else:
        obs = f"SEM ALTERACAO NOS DADOS {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        obs_interpol = f"SEM ALTERACAO NOS DADOS TRES PRIMEIRA LETRAS  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            # alter_status(self, id_insert_return[0],obs)
        alter_status(self, id_insert_return[0],obs_interpol)
        ClassLogger.logger.info(f"SEM ALTERACAO NOS DADOS {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
        ClassLogger.logger.info(f"SEM CONSULTA INDIVIDUAL {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")

    new_tabel = []

    for pais, totais in contador_por_pais.items():
        
        nova_linha = {
                'DATA CAPTURA': datetime.now().strftime("%d/%m/%Y %H:%M"), 
                'PAIS_BUSCADO': pais,
                'QTA SINGLAS': totais["INSERT"],
                'QTA J/N BASE':  totais["NA"],
                'QTA ERROR':     totais["ERROR"], 
                'QTA INSERIDO': max(0, totais["QTINSERT"] - totais["NA"])
            }
            
        new_tabel.append(nova_linha)

        
    tabela_atualizar = new_tabel
    ClassLogger.logger.info(f"TABELA DE RESUMO POR PAIS 'SIGLAS' {tabela_atualizar}")
    minha_tabela_montada = pd.DataFrame(tabela_atualizar)
        
        # return
    corpo_error = ""

    if falhas_ids:
        tabela_error = pd.DataFrame(falhas_ids)
        tabela_error = tabela_error.fillna(0) 
        convertida_error =  tabela_error.to_html(index=False, border=1, justify='center')
        corpo_error = f"Lista de dados com error :<br> {convertida_error}"
        print(f"Lista de dados com error :<br> {convertida_error}")


        

    convertida = minha_tabela_montada.to_html(index=False, border=1, justify='center')
        
    corpo = f"""
            <h2 style="color:green;">Busca dados por nome por 3 primeiras letras </h2>
            <p>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            {convertida}"""
    html_final = f"""
            <html>
            <body>
            {corpo}
            <hr>{corpo_error if corpo_error else "<p>Sem erros encontrados</p>"}

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
