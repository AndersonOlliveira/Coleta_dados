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
from Model.ClassModel import buscar_teste, insert_interpol, update_info_process,search_data_interpol,exists_by_name,insert_base_interpol,update_data_interpol,update_id_interpol
from functions.funcoes import remover_acentos, remover_conhetes, tratar_entrada, path_arquivo,dividir_lotes,dividir_lotes_tratar



# def trata_json(dados):
def trata_json(self,caminho_countries, retorno_api,id_insert_return):
    # print(json.dumps(caminho_countries,  indent=4))
    tratamento = []
    lista = []
    falhas_ids = []
 
      #imcremetador
    contador_paises = Counter()
    dupla_nacionalidade = 0
    pais_busca = None
    mapa = {}
    lista_urls = []
    lista_paises_unicos = []
    lista_paises_total_api = {}
    tabela_atualizar =[]
    todas_pessoas = []
    paises_buscas = ''
    tabela_final = []
    nome_traduzido = set()
    grupos_por_pais = defaultdict(list)
    id_insert_return_detalhe = []
    result_busca = []
    novos_registros = 0
    registros_pulados = 0
    detalhes = []
    contador_por_pais = defaultdict(lambda: {
    "INSERT": 0,
    "NA": 0,
    "ERROR":0,
    "QTINSERT": 0
    })
   



    
    caminho_countress = path_arquivo()
    
    tratamento = retorno_api

    

    lista_coutries = caminho_countress
            
    with open(lista_coutries) as lista_coutrie:
         lista_decodificadas = json.load(lista_coutrie)
         for pais in lista_decodificadas:
             codigo_pais = pais.get('value')
             nome_pais = pais.get('name')
             mapa[codigo_pais] = nome_pais


    for bloco in tratamento:
        print(f"DEBUG BLOCO: {bloco} | tipo: {type(bloco)}")
        # print(f"DEBUG BLOCO: {bloco} | tipo: {type(bloco)}")
        # return
        if not isinstance(bloco, dict):
                print(f"Bloco inválido: {bloco}")
                # link = bloco.get('_links').get('self', {}).get('href', {})
                # s = urlparse(link)
                # params = parse_qs(s.query)
                # lista_paises_unicos.append(params.get('nationality'))
                # list_teste = params.get('nationality')

                # contador_por_pais[list_teste]["NA"] += 1
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
         lista_url_interpol = {'periodizacao': self.periodo , 'siglas': 'null', 'url': self.servidor_push_expecifg_id, 'data_captura': datetime.now().strftime("%Y-%m-%d")} 
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
        #     detalhes = list(executor.map(
        #     lambda url: push_new_resquests(url, self.time_sleps),
        #     lista_urls
        # ))
                futures = [
                executor.submit(push_new_resquests, url, self.max_workers) for url in lote]

                for future in as_completed(futures):
                    try:
                        result = future.result()
                        detalhes.append(result)

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

      
        # print(f"quais sao os grupos_por_pais apresentados? {grupos_por_pais} ")
    for sigla_busca in lista_paises_unicos:
    # for sigla_busca, pessoas_do_grupo in grupos_por_pais.items():
        paises_encontrados = set()
        pessoas_do_grupo = grupos_por_pais.get(sigla_busca, [])
        total_dupla = 0
            
    


        for p in pessoas_do_grupo:
            nacionalidades = p.get('nationalities') or []
                # Traduz siglas para nomes usando seu mapa
            nomes = [mapa.get(s, s) for s in nacionalidades]
            # list_total = [mapa.get(s, s) for s in lista_paises_total_api]
           
            paises_encontrados.update(nomes)
                
            if len(nacionalidades) > 1:
                  total_dupla += 1


       
        total_por_sigla =  lista_paises_total_api.get(sigla_busca, 0)
        nome_sigla_traduzido = mapa.get(sigla_busca, sigla_busca)
       
        linha_tabela = {
                'DATA CAPTURA': datetime.now().strftime("%d/%m/%Y %H:%M"),
                'PAIS_BUSCADO': sigla_busca,
                # 'TOTAL ENCONTRADO': len(pessoas_do_grupo),
                #'COM DUPLA NACIONALIDADE': total_dupla,
                # 'PAISES NA LISTA': nome_sigla_traduzido if not paises_encontrados else ", ".join(sorted(paises_encontrados)), 
                'TOTAL RETORNO API': total_por_sigla
                }

        tabela_atualizar.append(linha_tabela)
        lista_urls , self.batch_size_verify
    lista_total =[]
    falhas_total =[]
    contador_por_paiss =[]

    for lote_pessoas, lote_detalhes in dividir_lotes_tratar(todas_pessoas, detalhes, 50):
        futures = []

        with ThreadPoolExecutor(max_workers=3) as executor:
            for pessoa, detalhe in zip(lote_pessoas, lote_detalhes):
                futures.append(
                        executor.submit(processar_pessoa,self, pessoa, detalhe,mapa,contador_por_pais,lista_paises_unicos,id_insert_return,id_geral_url_interpol,tabela_atualizar)
                )

            for future in as_completed(futures):
                try:
                    retorno_processo = future.result()
                except Exception as e:
                    print(f"Erro geral: {e}")

        print(f"Lote finalizado com {len(lote_pessoas)} registros")
        time.sleep(2)


    # print(f"{retorno_processo}")
    # return    
    # "status": "novo" if (not exist_id and not exist_name) else "existente",
    # "lista": lista,
    # "falhas_ids": falhas_ids,
    # "contador_por_pais": contador_por_pais,
    # 'tabela_atualizar': tabela_atualizar
    if retorno_processo:
            lista_total.extend(retorno_processo.get("lista", []))
            falhas_total.extend(retorno_processo.get("falhas_ids", []))
            contador_por_paiss.extend(retorno_processo.get("contador_por_pais", []))
            tabela_atualizar.extend(retorno_processo.get("tabela_atualizar", []))

            df = pd.DataFrame(lista_total)
            minha_tabela_montada = pd.DataFrame(tabela_atualizar)
            minha_tabela_montada = minha_tabela_montada.fillna(0) 
           
           

            if falhas_total is not None:
                    tabela_error = pd.DataFrame(falhas_total)
                    tabela_error = tabela_error.fillna(0) 
                    convertida_error =  tabela_error.to_html(index=False, border=1, justify='center')
                    corpo_error = f"Lista de dados com error :<br> {convertida_error}"
                    

    convertida = minha_tabela_montada.to_html(index=False, border=1, justify='center')
            
            # if len(df) > 0:
    corpo = f"""
                <h2 style="color:green;">Captura dos dados interpol</h2>
                <p>{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>

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

          

        # with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
        #     with self.db.get_connection() as conn:
        #         for pessoa, detalhe in zip(todas_pessoas, detalhes):
        #             print(f"ESTOU SAINDO AQUI? DEPOIS DO MEU ATUALIZAR? ")

        #             entity_id = pessoa.get('entity_id').replace('/','-') if pessoa.get('entity_id') else None
        #             name_person = remover_acentos("{} {}".format(pessoa.get('forename'), pessoa.get('name'))).strip()
        #             lista_paises_chaves = pessoa.get('nationalities') or []
        #             naturalidade = (detalhe.get('place_of_birth') or mapa.get(detalhe.get('country_of_birth_id')) or "N/I").upper()
        #             thumbnail = pessoa.get('_links', {}).get('thumbnail', {}).get('href') 
        #             pais_procurado = [mapa.get(wanted.get('issuing_country_id'),wanted.get('issuing_country_id')) for wanted in detalhe.get('arrest_warrants', [])]
        #             pais_procurado = ', '.join(pais_procurado).upper() if pais_procurado else "N/I"
        #             # tratar a data que veio a veio quebrada
        #             data_ajustada = pessoa.get('date_of_birth') if pessoa.get('date_of_birth') else None
        #             data_ajustada = tratar_entrada(data_ajustada)
        #             data_captura = datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
        #             exist_id = False
        #             exist_name = False
        #             # return
        #             #pego os ids da interpol para verificar só que vou ter uma dupla verificacao, pelo o id e pelo o nome
        #             person_singla = next((p for p in lista_paises_chaves if p in lista_paises_unicos), 'N/I')
        #             # person_singla = list(set(lista_paises_chaves) & set(lista_paises_unicos)) #COM O METODO SET
        #             if entity_id:
        #                 future_busca = executor.submit(search_data_interpol, conn, entity_id)
        #                 exist_id = future_busca.result()

        #                 if exist_id: # aqui atualizo sempre que vinher os dados
        #                     executor.submit(update_data_interpol, conn, entity_id, naturalidade,thumbnail, pais_procurado,data_captura)

        #             if not exist_id:
        #                 #colocar theads aqui
        #                 future_busca_name = executor.submit(exists_by_name,conn,name_person)
        #                 exist_name = future_busca_name.result()
        #                 if exist_name:
        #                     #faco o update para o id da interpol para a busca ser mais acertiva 
        #                     executor.submit(update_id_interpol, conn, name_person , entity_id)

        #                 # exist_name = exists_by_name(conn,name_person)

                
        #             if not exist_id and not exist_name:
                        
        #                 contador_por_pais[person_singla]["INSERT"] += 1
                        
        #                 # novos_registros +=1
        #                 lista_paises = pessoa.get('nationalities') or []
        #                 nomes_paises = [mapa.get(pais, pais) for pais in lista_paises]
        #                 pais_limpo = ','.join(nomes_paises) if nomes_paises else "N/I"
        #                 sexo = detalhe.get('sex_id') if detalhe else None
        #                 # crime =  [remover_acentos(warrant.get('charge')).strip() for warrant in detalhe.get('arrest_warrants', [])] if detalhe else None
        #                 crime_lista = [remover_acentos(warrant.get('charge', '')).strip() for warrant in (detalhe.get('arrest_warrants') or [])]
        #                 crime = ", ".join(crime_lista) if crime_lista else "N/I"
        #                 # idiona = [remover_conhetes(lang) for lang in detalhe.get('languages_spoken_ids', [])] if detalhe else None
        #                 idiona = ", ".join(item.strip("[] ").strip() for item in detalhe.get('languages_spoken_ids', []) or []) if detalhe and detalhe.get('languages_spoken_ids') else "N/I"
                        

                    
        #                 lista.append({
        #                         'nome_completo': name_person,
        #                         'data_nascimento': data_ajustada,
        #                         'nacionalidade': pais_limpo.upper(),
        #                         'naturalidade': naturalidade.upper(),
        #                         'id_interpol': entity_id,
        #                         'sexo': sexo, 
        #                         'acusacao': crime.upper(),
        #                         'idiona': idiona.upper(),
        #                         'thumbnail': thumbnail if thumbnail else "N/I", #COMENTEADO PARA NAÓ APRESENTAR EM TELA,
        #                         'data_consulta': datetime.now().strftime("%Y-%m-%d"),
        #                         'hora_consulta': datetime.now().strftime("%H:%M:%S"),
        #                         'person_sigla_unico' : person_singla,
        #                         'country_wanted': pais_procurado
        #                 })

        #             else:
        #                 print(f"vou pular {entity_id} || nome: {name_person}  que pais ???{lista_paises_chaves}")
        #                 print(f"vou pular {entity_id} | nome: {name_person} + {person_singla}")
        #                 contador_por_pais[person_singla]["NA"] += 1
                        # registros_pulados +=1

    # info_dados_registros = {
    #    "qta_registros_atualizados" :  int(novos_registros),
    #     "qta_registro_nao_atualizados" : int(registros_pulados) 
    #  }
    
    # tabela_atualizar.append(info_dados_registros)
    # for linha in tabela_atualizar:
    #     pais = linha['PAIS_BUSCADO']
    #     #MUNDAR PRA A
    #     linha['QTA A INSERIR'] = contador_por_pais[pais]["INSERT"]
    #     linha['QTA J/N BASE'] = contador_por_pais[pais]["NA"]
        
    # df = pd.DataFrame(lista)


    # if len(df) > 0:
       
        

    #     # update_info_process(self, id_insert_return[0])
    #     alter_status(self, id_insert_return[0])
    #     obs_interpol_success = 'SUCESSO EM CONSULTAR OS IDS INDIVIDUAL INTERPOL'
    #     alter_status(self, id_geral_url_interpol,obs_interpol_success)



    #     with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
          
    #         futures = [
    #             executor.submit(insert_base_interpol,self,registro)
    #                   for registro in lista
    #          ]
                    
    #         for future in as_completed(futures):
    #             result = future.result()
                
                
    #             if result['status'] == "sucesso":
    #                         # inser_new_registro +=1
    #                contador_por_pais[result['person_sigla_unico']]["QTINSERT"] += 1
    #             else:
    #                falhas_ids.append(result)
    #                         # falha_ +=1
    #                contador_por_pais[result['person_sigla_unico']]["ERROR"] += 1


    # else:
    #     obs = f"SEM ALTERACAO NOS DADOS {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"
    #     obs_interpol = f"SEM CONSULTA INDIVIDUAL {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"
    #     alter_status(self, id_insert_return[0],obs)
    #     alter_status(self, id_geral_url_interpol,obs_interpol)
    
    # for linha in tabela_atualizar:
    #     pais = linha['PAIS_BUSCADO']
    #     linha['QTA ERROR'] = contador_por_pais[pais]["ERROR"]
    #     linha['QTA INSERIDO'] = contador_por_pais[pais]["QTINSERT"]

    
  
    
    # minha_tabela_montada = pd.DataFrame(tabela_atualizar)
    # minha_tabela_montada = minha_tabela_montada.fillna(0) 

  
    # if falhas_ids is not None:
    #    tabela_error = pd.DataFrame(falhas_ids)
    #    tabela_error = tabela_error.fillna(0) 
    #    convertida_error =  tabela_error.to_html(index=False, border=1, justify='center')
    #    corpo_error = f"Lista de dados com error :<br> {convertida_error}"
    

    # convertida = minha_tabela_montada.to_html(index=False, border=1, justify='center')
    
    # # if len(df) > 0:
    # corpo = f"""
    #     <h2 style="color:green;">Captura dos dados interpol</h2>
    #     <p>{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>

    #     {convertida}
    #     """

    # html_final = f"""
    #     <html>
    #     <body>

    #     {corpo}

    #     <hr>

    #     {corpo_error if corpo_error else "<p>Sem erros encontrados</p>"}

    #     </body>
    #     </html>
    #     """

    # result_email = enviar_email_all(html_final)

    #     # print(f"Resultado do enviar e-mail {result_email}")

    #     #envia a quantiade para 
    # return result_email





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

       



    # for pessoa in tratamento.get('_embedded', {}).get('notices', []):
            
          
    #     # print(plenessoa.get('_links', {}).get('thumbnail',{}).get('href'))
    #         # paises = str(pessoa.get('nationalities', "")).strip("[]")

    #         lista_paises = pessoa.get('nationalities') or []
    #         nomes_paises = [mapa.get(pais, pais) for pais in lista_paises]
    #         pais_limpo = ' '.join(nomes_paises) if nomes_paises else "Nenhuma nacionalidade encontrada"
    #         # print(pais_limpo)
           
                    
    #         # print(f"minha url individual : {pessoa.get('_links', {}).get('self',{}).get('href', {})}")
    #         new_url = pessoa.get('_links', {}).get('self',{}).get('href', {})
    #         print(f"minha url individual : {new_url}")
            

            
    #         # teste_resquest = push_request(self,new_url)
    #         # teste_resquest = push_request(self,None,new_url)

    #         # print(f"Meu resultado o que tenho aqui? {teste_resquest}")
    #         # print(f"Meu resultado da chamada, vou tentar acessar os dados vindo da api individual {teste_resquest.get('sex_id')}")
           
                
                
    #         lista.append({
    #             'primeiro_nome': pessoa.get('name'),
    #             'nome_completo': "{} {}".format(pessoa.get('name'), pessoa.get('forename')),
    #             'nome_do_meio': pessoa.get('forename'),
    #             'data_nascimento': pessoa.get('date_of_birth').replace('/','-'),
    #             'nacionalidade': pais_limpo.upper(),
    #             'id_interpol': pessoa.get('entity_id').replace('/','-'),
    #              # 'thumbnail': pessoa.get('_links', {}).get('thumbnail', {})
    #             'thumbnail': pessoa.get('_links', {}).get('thumbnail', {}).get('href')
    #         })
       




    
    # df = pd.DataFrame(lista)
       

      
       
    # print(f"Minha quantiade processado {len(df)}")
    # print(df)

    # return lista

    # print(df.get("nome_completo"))



        # aqui normalizo de acordo com o que vem da api transformando em tabela
    # dados_tratamento = pd.json_normalize(
    #       tratamento['_embedded']['notices'])
    
     
      
    # print(dados_tratamento)
    # print(dados_tratamento['_links.thumbnail.href'])
    # print(dados_tratamento['_links.images.href'])

# def busca_singlas(links):
#         grupos_por_pais

#         url_busca = links
#         s = urlparse(url_busca)
#         params = parse_qs(s.query)
#             #  sigla (ex: 'AM') ou 'Desconhecido'
#         sigla_buscas = params.get('nationality', ['Desconhecido'])[0]
            
#             # Guarda a pessoa no grupo desse país
#         grupos_por_pais[sigla_buscas].append(pessoa)

      
#         # print(f"quais sao os grupos_por_pais apresentados? {grupos_por_pais} ")
#     for sigla_busca in lista_paises_unicos:
#     # for sigla_busca, pessoas_do_grupo in grupos_por_pais.items():
#         paises_encontrados = set()
#         pessoas_do_grupo = grupos_por_pais.get(sigla_busca, [])
#         total_dupla = 0


def processar_pessoa(self, pessoa, detalhe,mapa,contador_por_pais,lista_paises_unicos,id_insert_return,id_geral_url_interpol,tabela_atualizar):
    # tabela_atualizar =[]
    lista = []
    falhas_ids =[]
    
    with self.db.get_connection() as conn:

         entity_id = pessoa.get('entity_id').replace('/','-') if pessoa.get('entity_id') else None
         name_person = remover_acentos("{} {}".format(pessoa.get('forename'), pessoa.get('name'))).strip()
         lista_paises_chaves = pessoa.get('nationalities') or []
         naturalidade = (detalhe.get('place_of_birth') or mapa.get(detalhe.get('country_of_birth_id')) or "N/I").upper()
         thumbnail = pessoa.get('_links', {}).get('thumbnail', {}).get('href') 
         pais_procurado = [mapa.get(wanted.get('issuing_country_id'),wanted.get('issuing_country_id')) for wanted in detalhe.get('arrest_warrants', [])]
         pais_procurado = ', '.join(pais_procurado).upper() if pais_procurado else "N/I"
                    # tratar a data que veio a veio quebrada
         data_ajustada = pessoa.get('date_of_birth') if pessoa.get('date_of_birth') else None
         data_ajustada = tratar_entrada(data_ajustada)
         data_captura = datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
         exist_id = False
         exist_name = False
         # return
                    #pego os ids da interpol para verificar só que vou ter uma dupla verificacao, pelo o id e pelo o nome
         person_singla = next((p for p in lista_paises_chaves if p in lista_paises_unicos), 'N/I')
                    # person_singla = list(set(lista_paises_chaves) & set(lista_paises_unicos)) #COM O METODO SET
         if entity_id:
                future_busca = search_data_interpol(conn, entity_id)
                exist_id = future_busca
                if exist_id: # aqui atualizo sempre que vinher os dados
                       update_data_interpol(conn, entity_id, naturalidade,thumbnail, pais_procurado,data_captura)

         if not exist_id:
                        #colocar theads aqui
            future_busca_name = exists_by_name(conn,name_person)
            exist_name = future_busca_name.result()
            if exist_name:
                            #faco o update para o id da interpol para a busca ser mais acertiva 
                 update_id_interpol(conn, name_person , entity_id)

                        # exist_name = exists_by_name(conn,name_person)

                
         if not exist_id and not exist_name:
                        
            contador_por_pais[person_singla]["INSERT"] += 1
                        
                        # novos_registros +=1
            lista_paises = pessoa.get('nationalities') or []
            nomes_paises = [mapa.get(pais, pais) for pais in lista_paises]
            pais_limpo = ','.join(nomes_paises) if nomes_paises else "N/I"
            sexo = detalhe.get('sex_id') if detalhe else None
                        # crime =  [remover_acentos(warrant.get('charge')).strip() for warrant in detalhe.get('arrest_warrants', [])] if detalhe else None
            crime_lista = [remover_acentos(warrant.get('charge', '')).strip() for warrant in (detalhe.get('arrest_warrants') or [])]
            crime = ", ".join(crime_lista) if crime_lista else "N/I"
            # idiona = [remover_conhetes(lang) for lang in detalhe.get('languages_spoken_ids', [])] if detalhe else None
            idiona = ", ".join(item.strip("[] ").strip() for item in detalhe.get('languages_spoken_ids', []) or []) if detalhe and detalhe.get('languages_spoken_ids') else "N/I"
                        

                    
            lista.append({
                'nome_completo': name_person,
                'data_nascimento': data_ajustada,
                'nacionalidade': pais_limpo.upper(),
                'naturalidade': naturalidade.upper(),
                'id_interpol': entity_id,
                'sexo': sexo, 
                'acusacao': crime.upper(),
                'idiona': idiona.upper(),
                'thumbnail': thumbnail if thumbnail else "N/I", #COMENTEADO PARA NAÓ APRESENTAR EM TELA,
                'data_consulta': datetime.now().strftime("%Y-%m-%d"),
                'hora_consulta': datetime.now().strftime("%H:%M:%S"),
                'person_sigla_unico' : person_singla,
                'country_wanted': pais_procurado
            })

         else:
             print(f"vou pular {entity_id} || nome: {name_person}  que pais ???{lista_paises_chaves}")
             print(f"vou pular {entity_id} | nome: {name_person} + {person_singla}")
             contador_por_pais[person_singla]["NA"] += 1
  
    
    # tabela_atualizar.append(info_dados_registros)
    for linha in tabela_atualizar:
        pais = linha['PAIS_BUSCADO']
        #MUNDAR PRA A
        linha['QTA A INSERIR'] = contador_por_pais[pais]["INSERT"]
        linha['QTA J/N BASE'] = contador_por_pais[pais]["NA"]
        
    df = pd.DataFrame(lista)



    if len(df) > 0:
         # update_info_process(self, id_insert_return[0])
        alter_status(self, id_insert_return[0])
        obs_interpol_success = 'SUCESSO EM CONSULTAR OS IDS INDIVIDUAL INTERPOL'
        alter_status(self, id_geral_url_interpol,obs_interpol_success)



        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
          
            futures = [
                executor.submit(insert_base_interpol,self,registro)
                      for registro in lista
             ]
                    
            for future in as_completed(futures):
                result = future.result()
                
                
                if result['status'] == "sucesso":
                            # inser_new_registro +=1
                   contador_por_pais[result['person_sigla_unico']]["QTINSERT"] += 1
                else:
                   falhas_ids.append(result)
                            # falha_ +=1
                   contador_por_pais[result['person_sigla_unico']]["ERROR"] += 1


    else:
        obs = f"SEM ALTERACAO NOS DADOS {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"
        obs_interpol = f"SEM CONSULTA INDIVIDUAL {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"
        alter_status(self, id_insert_return[0],obs)
        alter_status(self, id_geral_url_interpol,obs_interpol)
    
    for linha in tabela_atualizar:
        pais = linha['PAIS_BUSCADO']
        linha['QTA ERROR'] = contador_por_pais[pais]["ERROR"]
        linha['QTA INSERIDO'] = contador_por_pais[pais]["QTINSERT"]

    
  
    
    minha_tabela_montada = pd.DataFrame(tabela_atualizar)
    minha_tabela_montada = minha_tabela_montada.fillna(0) 


    
    return {
    "status": "novo" if (not exist_id and not exist_name) else "existente",
    "lista": lista,
    "falhas_ids": falhas_ids,
    "contador_por_pais": contador_por_pais,
    'tabela_atualizar': tabela_atualizar
}

  
   