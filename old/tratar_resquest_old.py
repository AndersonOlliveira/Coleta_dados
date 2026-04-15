
import json
from Logs import ClassLogger
import os
import pandas as pd
from ..Processar.Request import push_request,push_new_resquests
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