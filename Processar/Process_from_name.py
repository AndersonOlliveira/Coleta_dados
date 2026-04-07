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



    
    
    
    

    siglas = []

    for lista_name in lista_get_name:
        nome = lista_name.get('nome', '')
        
        if nome:
            tres_primeiras = nome[:3].upper()
            params = f"&resultPerPage={self.qtPage}&page={self.indicePage}"
            lista_singlas_name = f"{self.servidor_get_from_name}={tres_primeiras}{params}"
            siglas.append(lista_singlas_name)
                          

    # remove duplicados
    siglas_unicas = list(set(siglas))

    # print(siglas_unicas)
    print(f"MINHA QUANTIDADE A SER PROCESSADO {len(siglas_unicas)}")



    
    
    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
        des = list(executor.map(
        lambda url: push_new_resquests(url, self.time_sleps),
        siglas_unicas
    ))
    
    
    
    
    
        #agoro verfico se exite se nao existir vou inserir

    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
        with self.db.get_connection() as conn:
            for de in des:
               print(f"meu resultado do for?{de}")
               entity_id = de.get('entity_id').replace('/','-') if de.get('entity_id') else None
               name_person = remover_acentos("{} {}".format(de.get('forename'), de.get('name'))).strip()
               naturalidade = (de.get('place_of_birth') or mapa.get(de.get('country_of_birth_id')) or "N/I").upper()
               thumbnail = de.get('_links', {}).get('thumbnail', {}).get('href') 
               print(f"dados encontrados: {de.get('place_of_birth')} + NOME PESON {name_person} + naturalidade dois: {naturalidade}  ")
              
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
                        executor.submit(update_data_interpol, conn, entity_id, naturalidade,thumbnail)

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
                    lista_paises = pessoa.get('nationalities') or []
                    print(f"{lista_paises} MEUS DADOSSS")
                    nomes_paises = [mapa.get(pais, pais) for pais in lista_paises]
                    print(f"{nomes_paises} depois do get?")
                    pais_limpo = ','.join(nomes_paises) if nomes_paises else "N/I"
                    sexo = detalhe.get('sex_id') if detalhe else None
                    # crime =  [remover_acentos(warrant.get('charge')).strip() for warrant in detalhe.get('arrest_warrants', [])] if detalhe else None
                    crime_lista = [remover_acentos(warrant.get('charge', '')).strip() for warrant in (detalhe.get('arrest_warrants') or [])]
                    crime = ", ".join(crime_lista) if crime_lista else "N/I"
                    # idiona = [remover_conhetes(lang) for lang in detalhe.get('languages_spoken_ids', [])] if detalhe else None
                    idiona = ", ".join(item.strip("[] ").strip() for item in detalhe.get('languages_spoken_ids', []) or []) if detalhe and detalhe.get('languages_spoken_ids') else "N/I"
                    

                   
                    lista.append({
                            # 'primeiro_nome': pessoa.get('name'),
                            # 'nome_completo': "{} {}".format(pessoa.get('forename'), pessoa.get('name')),
                            'nome_completo': name_person,
                            # 'nome_do_meio': pessoa.get('forename'),
                            'data_nascimento': pessoa.get('date_of_birth').replace('/','-') if pessoa.get('date_of_birth') else None,
                            'nacionalidade': pais_limpo.upper(),
                            'naturalidade': naturalidade.upper(),
                            'id_interpol': entity_id,
                            'sexo': sexo, 
                            'acusacao': crime.upper(),
                            'idiona': idiona.upper(),
                            'thumbnail': thumbnail if thumbnail else "N/I", #COMENTEADO PARA NAÓ APRESENTAR EM TELA,
                            'data_consulta': datetime.now().strftime("%Y-%m-%d"),
                            'hora_consulta': datetime.now().strftime("%H:%M:%S"),
                            'person_sigla_unico' : person_singla
                    })














              



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

