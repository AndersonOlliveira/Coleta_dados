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
from Model.ClassModel import get_data_match_name_base
from Model.ClassModel import buscar_teste, insert_interpol, update_info_process,search_data_interpol,exists_by_name,insert_base_interpol,update_data_interpol,update_id_interpol







def process_from_name(self):
    #pego os dados e faco a busca
    print(f'ESTOU CHAMANDO AQUI')
    mapa = {}
    lista = []
    falhas_ids =[]
    siglas = []

    lista_paises_unicos = []
    nome_traduzido = set()
    todas_pessoas =[]
    lista_urls_pesquisa = []
    lista_paises_total_api = {}
    grupos_por_pais = defaultdict(list)
    id_insert_return_detalhe = []
    contador_por_pais = defaultdict(lambda: {
    "INSERT": 0,
    "NA": 0,
    "ERROR":0,
    "QTINSERT": 0
    })

    

    caminho_countress = Path('Arquivos/countries.json')
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
          future_busca = executor.submit(get_data_match_name_base, self)
          lista_get_name = future_busca.result()
        #  print(f"MINHA LISTA DO GET {lista_get_name}")
          for lista_name in lista_get_name:
                nome = lista_name.get('nome', '')
                
                # if nome:
                #     tres_primeiras = nome[:3].upper()
                #     params = f"&resultPerPage={self.qtPage}&page={self.indicePage}"
                #     lista_singlas_name = f"{self.servidor_get_from_name}='SID'{params}"
                #     siglas.append(lista_singlas_name)

    params = f"&resultPerPage={self.qtPage}&page={self.indicePage}"
    lista_singlas_name = f"{self.servidor_get_from_name}=KAB{params}"
    siglas.append(lista_singlas_name)                    

    # remove duplicados
    siglas_unicas = list(set(siglas))

    print(f"QUAL E MINHA SIGLA {siglas_unicas}")
    # print(f"MINHA QUANTIDADE A SER PROCESSADO {len(siglas_unicas)})"
    # return


   
    




  
  
    # siglas_unicas = 'https://ws-public.interpol.int/notices/v1/red?&forename=SID&resultPerPage=160'


    # # print(f"MINHA SINGLAS PARA CHAMADA {siglas_unicas}")
    # # return 
    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
        des = list(executor.map(
        lambda url: push_new_resquests(url, self.time_sleps),
        siglas_unicas
    ))
    
    
    
   
    # return 
    for bloco in des:
        pessoas_detalhes = bloco.get('_embedded', {}).get('notices', [])
      
    todas_pessoas.extend(pessoas_detalhes)

    # print(f"MINHA todas_pessoas PARA CHAMADA {json.dumps(todas_pessoas, indent=4)}")

    # return
    
    for pessoa in todas_pessoas:
        lista_url = pessoa.get('_links', {}).get('self', {}).get('href')

        print(f"Minha lista de url individual primeira chamada {lista_url}")
    
        if lista_url:
           lista_urls_pesquisa.append(lista_url)
    #CHAMO A API QUE VEM NO RETORNO DA CHAMADA DA PAIS, NELE JA ME ROTANA O LINK COM O ID INDIVIDUAL PARA A PESSOA , COLOCANDO O RESULTANDO DENTRO DE DETALHES PARA POPULAR ABAIXO
    # return
    
   
    # print(f"Minha lista de url {json.dumps(lista_urls_pesquisa, indent=4)}")
    # return 
    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
        lista_detalhes_pesquisa = list(executor.map(
        lambda url: push_new_resquests(url, self.time_sleps),
        lista_urls_pesquisa
    ))
        
    
    # print(f"Detalhes das pessoas: {json.dumps(lista_detalhes_pesquisa, indent=4)}")

    # return

 
    try:
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            with self.db.get_connection() as conn:
                for de, list_url_person in zip(todas_pessoas, lista_detalhes_pesquisa):
                    print(f"meu resultado do for?{de}")
                    print(f"meu resultado do list_url_person?{list_url_person}")
                    lista_paises_chaves = de.get('nationalities') or []
                    # lista_paises_unicos.append(lista_paises_chaves)
                    print(f"meu resultado do lista_paises_chaves?{lista_paises_chaves}")
                    person_singla = lista_paises_chaves[0] if lista_paises_chaves else 'N/I'

                            
                    entity_id = de.get('entity_id').replace('/','-') if de.get('entity_id') else None
                    name_person = remover_acentos("{} {}".format(de.get('forename'), de.get('name'))).strip()
                    naturalidade = (de.get('place_of_birth') or mapa.get(de.get('country_of_birth_id')) or "N/I").upper()
                    thumbnail = de.get('_links', {}).get('thumbnail', {}).get('href') 
                    print(f"dados encontrados: {de.get('place_of_birth')} + NOME PESON {name_person} + naturalidade dois: {naturalidade}  ")
                    pais_procurado = [mapa.get(wanted.get('issuing_country_id'),wanted.get('issuing_country_id')) for wanted in list_url_person.get('arrest_warrants', [])]
                    pais_procurado = ', '.join(pais_procurado).upper() if pais_procurado else "N/I"
                    print(f'meu pais procurado {pais_procurado}')    
                    exist_id = False
                    exist_name = False
                            # return
                            #pego os ids da interpol para verificar só que vou ter uma dupla verificacao, pelo o id e pelo o nome
                            # person_singla = list(set(lista_paises_chaves) & set(lista_paises_unicos)) #COM O METODO SET
                    if entity_id:
                        future_busca = executor.submit(search_data_interpol, conn, entity_id)
                        exist_id = future_busca.result()
                        print(f"QUAL E MEU RESULADO AQUI? {exist_id}")

                        if exist_id: # aqui atualizo sempre que vinher os dados
                           executor.submit(update_data_interpol, conn, entity_id, naturalidade,thumbnail,pais_procurado)

                    if not exist_id:
                        #colocar theads aqui
                       print(f"QUAIS OS IDS BUSCADO {entity_id} ||| nome: {name_person}")
                       future_busca_name = executor.submit(exists_by_name,conn,name_person)
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
                                        'data_nascimento': de.get('date_of_birth').replace('/','-') if de.get('date_of_birth') else None,
                                        'nacionalidade': pais_limpo.upper(),
                                        'naturalidade': naturalidade.upper(),
                                        'id_interpol': entity_id,
                                        'sexo': sexo, 
                                        'acusacao': crime.upper(),
                                        'idiona': idiona.upper(),
                                        'thumbnail': thumbnail if thumbnail else "N/I", #COMENTEADO PARA NAÓ APRESENTAR EM TELA,
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

    pd.set_option('display.max_rows', 100)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    df = pd.DataFrame(lista)
   
  
    print(f"Minha quantidade a ser processada {len(df)}")
    print(f"{df}")
    print(f"Minha lista do contador {len(df)}")
    print(f"{contador_por_pais}")

    # return

    if len(df) > 0:
       
        

        # update_info_process(self, id_insert_return[0])
        #alter_status(self, id_insert_return[0])
        obs_interpol_success = 'SUCESSO EM CONSULTAR OS IDS INDIVIDUAL INTERPOL'
        #alter_status(self, id_geral_url_interpol,obs_interpol_success)



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
                else:
                   falhas_ids.append(result)
                            # falha_ +=1
                   contador_por_pais[result['person_sigla_unico']]["ERROR"] += 1


         #funcao que esta funcioanndo 
        # with self.db.get_connection() as conn:
        #       conn.autocommit = False
        #       for registro in lista:
        #         sucesso = insert_base_interpol(self,registro,conn, falhas_ids)
        #         if sucesso:
        #             inser_new_registro +=1
        #         else:
        #             falha_ +=1



    
        #         conn.commit()
    else:
        obs = f"SEM ALTERACAO NOS DADOS {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"
        obs_interpol = f"SEM CONSULTA INDIVIDUAL {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"
      #  alter_status(self, id_insert_return[0],obs)
       # alter_status(self, id_geral_url_interpol,obs_interpol)






              



def remover_acentos(texto):
    
    if texto is None:
        texto = ''
    else:
        texto = str(texto)
    # Normaliza o texto para NFD
    texto_normalizado = unicodedata.normalize('NFD', texto)
    # Codifica para ascii, ignora erros e decodifica de volta para utf-8
    texto_normalizado =  texto_normalizado.encode('ascii', 'ignore').decode('utf-8')

    return re.sub(r'\s+', ' ',texto_normalizado).strip()

def remover_conhetes(texto):
   
   return  re.sub(r'[\[\]]', '', texto)

