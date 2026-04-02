import json
from Logs import ClassLogger
import os
import pandas as pd
from .Request import push_request,push_new_resquests
from concurrent.futures import ThreadPoolExecutor
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
from Model.ClassModel import buscar_teste, insert_interpol, update_info_process,search_data_interpol,exists_by_name,insert_base_interpol




# def trata_json(dados):
def trata_json(self,caminho_countries, retorno_api,id_insert_return):
    # print(json.dumps(caminho_countries,  indent=4))
    tratamento = []
    lista = []
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
    contador_por_pais = defaultdict(lambda: {
    "INSERT": 0,
    "NA": 0
    })
   



    caminho = os.path.join(os.path.dirname(__file__), 'arquivo_registros.json')
    caminho_countress = Path('Arquivos/countries.json')
    
    tratamento = retorno_api

    # print(f"MEU ID DA INSERCAO RETORNADO {id_insert_return[0]}")
    # print(id_insert_return)

    # return 
    # lista_coutries = caminho_countries
    lista_coutries = caminho_countress
            
    with open(lista_coutries) as lista_coutrie:
         lista_decodificadas = json.load(lista_coutrie)
        #  print(lista_decodificadas)
         for pais in lista_decodificadas:
             codigo_pais = pais.get('value')
             nome_pais = pais.get('name')
             mapa[codigo_pais] = nome_pais


    print(f"MEU TRATAMENTO RETORNO API {json.dumps(tratamento, indent=4)}")
    for bloco in tratamento:
        pessoas = bloco.get('_embedded', {}).get('notices', [])
        link = bloco.get('_links').get('self', {}).get('href', {})
        total_api = bloco.get('total')
        # print(f"meu total? {total_api}")
        if pessoas:
            for pessoa in pessoas:
                # print(f"Minha pessoa : {json.dumps(pessoa, indent=4)}")
                pessoa['link_busca'] = link
                pessoa['total_api'] = total_api
                if link not in lista_paises_unicos:
                   s = urlparse(link)
                   params = parse_qs(s.query)

                #    print(f"meus params {params}")
                
                   lista_paises_unicos.append(params.get('nationality'))
                   list_teste = params.get('nationality')
        

                for result_pais in list_teste:
                    lista_paises_total_api[result_pais] = total_api
        else: 
            s = urlparse(link)
            params = parse_qs(s.query)

            # print(f"meus params {params}")
                
            lista_paises_unicos.append(params.get('nationality'))
            list_teste = params.get('nationality')
            # lista_paises_unicos.append(params.get('nationality'))

            for result_pais in list_teste:
                lista_paises_total_api[result_pais] = total_api
            
            # lista_paises_total_api[params.get('nationality')] = total_api

            # lista_paises_total_api.append((params.get('nationality'), total_api))

        todas_pessoas.extend(pessoas)
            
   
    # print(lista_paises_total_api)
    # print(json.dumps(todas_pessoas,indent=4))

    # alter_status(self, id_insert_return[0])

    # return 

   # PEGO OS PAISEIS QUE FORAM SOLITADOS NA BUSCA , REMOVO AS DUPLICATAS PARA REALIZAR A CONTAGEM E APRESENTAR NA TABELA FINAL
    lista_paises_unicos = list(dict.fromkeys(sub[0].strip() for sub in lista_paises_unicos))
    # lista_paises_total_api = list(dict.fromkeys(info_api.strip() for info_api in lista_paises_total_api))
    nome_traduzido.update([mapa.get(s, s) for s in lista_paises_total_api])
   
    
    # lista_urls = set(lista_urls)
    # print(lista_paises_total_api)
    # print(json.dumps(todas_pessoas,  indent=4))
    # return 



  
   

    for pessoa in todas_pessoas:
        lista_url = pessoa.get('_links', {}).get('self', {}).get('href')
        # print(f"Minha lista de url individual primeira chamada {lista_url}")
    
        if lista_url:
           lista_urls.append(lista_url)
    #CHAMO A API QUE VEM NO RETORNO DA CHAMADA DA PAIS, NELE JA ME ROTANA O LINK COM O ID INDIVIDUAL PARA A PESSOA , COLOCANDO O RESULTANDO DENTRO DE DETALHES PARA POPULAR ABAIXO
    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
        detalhes = list(executor.map(
        lambda url: push_new_resquests(url, self.time_sleps),
        lista_urls
    ))
    


    # print(f"Minha lista de url individual {detalhes}")
    pd.set_option('display.max_colwidth', None) # PARA VISUALIZAR EM ESCALA GRANDE 

   
    
    for pessoa in todas_pessoas:
       
        url_busca = pessoa.get('link_busca', "")
        s = urlparse(url_busca)
        params = parse_qs(s.query)
            #  sigla (ex: 'AM') ou 'Desconhecido'
        sigla_buscas = params.get('nationality', ['Desconhecido'])[0]
            
            # Guarda a pessoa no grupo desse país
        grupos_por_pais[sigla_buscas].append(pessoa)

      
        # print(f"quais sao os grupos_por_pais apresentados? {grupos_por_pais} ")
    for sigla_busca in lista_paises_unicos:
    # for sigla_busca, pessoas_do_grupo in grupos_por_pais.items():
        paises_encontrados = set()
        pessoas_do_grupo = grupos_por_pais.get(sigla_busca, [])
        total_dupla = 0
            
            # print(f"quais sao os sigla_busca apresentados? {sigla_busca} ")
            # print(f"quais sao os pessoas_do_grupo apresentados? {pessoas_do_grupo} ")
            # print(grupos_por_pais)



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
       
        print(f"minha lista totals {nome_traduzido}")

        print(f"quais sao os sigla_busca apresentados? {sigla_busca} - Total por sigla: {total_por_sigla}") 
        linha_tabela = {
                'DATA CAPTURA': datetime.now().strftime("%d/%m/%Y %H:%M"),
                'PAIS BUSCADO': sigla_busca,
                'TOTAL ENCONTRADO': len(pessoas_do_grupo),
                #'COM DUPLA NACIONALIDADE': total_dupla,
                # 'PAISES NA LISTA': nome_sigla_traduzido if not paises_encontrados else ", ".join(sorted(paises_encontrados)), 
                'TOTAL RETORNO API': total_por_sigla
                }

        tabela_atualizar.append(linha_tabela)

    # minha_tabela_montada = pd.DataFrame(tabela_atualizar)

    # # print(f"Minha quantidade processada {len(df)}")
    # # print(df)

    # print(f"MINHA TABELA MONTADA")
    # print(f"{minha_tabela_montada}")
 
     ## PARA DEBUG
     
    # return
    result_busca = []
    novos_registros = 0
    registros_pulados = 0

    # print(json.dumps(todas_pessoas, indent=4))
    # print(grupos_por_pais)

    # return
  
    
    with self.db.get_connection() as conn:
        for pessoa, detalhe in zip(todas_pessoas, detalhes):

            # print()

            entity_id = pessoa.get('entity_id').replace('/','-') if pessoa.get('entity_id') else None
            name_person = remover_acentos("{} {}".format(pessoa.get('forename'), pessoa.get('name'))).strip()
            lista_paises_chaves = pessoa.get('nationalities') or []

            #controlador de status 
            exist_id = False
            exist_name = False
            
            # print(f"meu nome para busca {name_person}")

            # return
             #pego os ids da interpol para verificar só que vou ter uma dupla verificacao, pelo o id e pelo o nome
            person_singla = next((p for p in lista_paises_chaves if p in lista_paises_unicos), 'N/I')
            # person_singla = list(set(lista_paises_chaves) & set(lista_paises_unicos)) #COM O METODO SET
            if entity_id:
            #    dados_busca = search_data_interpol(conn,entity_id)
                exist_id = search_data_interpol(conn,entity_id)
                print(f"QUAL E MEU RESULADO AQUI? {exist_id}")

            if not exist_id:
                print(f"QUAIS OS IDS BUSCADO {entity_id} ||| nome: {name_person}")
                exist_name = exists_by_name(conn,name_person)

           
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
                    
                # idiona = ", ".join(item.strip("[] ").strip() for item in detalhe.get('languages_spoken_ids', []) or []) if detalhe else None


                #faco uma busca para ver se o id não esta inserido já
                    

                # print(f"crime localizado?: {crime}")
                # print(type(idiona))
                lista.append({
                        # 'primeiro_nome': pessoa.get('name'),
                        # 'nome_completo': "{} {}".format(pessoa.get('forename'), pessoa.get('name')),
                        'nome_completo': name_person,
                        # 'nome_do_meio': pessoa.get('forename'),
                        'data_nascimento': pessoa.get('date_of_birth').replace('/','-') if pessoa.get('date_of_birth') else None,
                        'nacionalidade': pais_limpo.upper(),
                        'id_interpol': entity_id,
                        'sexo': sexo, 
                        'acusacao': crime,
                        'idiona': idiona,
                        'thumbnail': pessoa.get('_links', {}).get('thumbnail', {}).get('href'), #COMENTEADO PARA NAÓ APRESENTAR EM TELA,
                        'data_consulta': datetime.now().strftime("%Y-%m-%d"),
                        'hora_consulta': datetime.now().strftime("%H:%M:%S")
                })

            else:
                print(f"vou pular {entity_id} || nome: {name_person}  que pais ???{lista_paises_chaves}")
                print(f"vou pular {entity_id} | nome: {name_person} + {person_singla}")
                contador_por_pais[person_singla]["NA"] += 1
                # registros_pulados +=1

    # info_dados_registros = {
    #    "qta_registros_atualizados" :  int(novos_registros),
    #     "qta_registro_nao_atualizados" : int(registros_pulados) 
    #  }
    
    # tabela_atualizar.append(info_dados_registros)
    for linha in tabela_atualizar:
        pais = linha['PAIS BUSCADO']

        linha['QTA INSERIDOS'] = contador_por_pais[pais]["INSERT"]
        linha['QTA J/N BASE'] = contador_por_pais[pais]["NA"]
        
    print(f"meu contatodor para insercáo de novos registros {novos_registros}")
    print(f"meu contatodor para dados pulados  {registros_pulados}")
    df = pd.DataFrame(lista)
    minha_tabela_montada = pd.DataFrame(tabela_atualizar)

    
    minha_tabela_montada = minha_tabela_montada.fillna(0) 

    print(f"Minha quantidade processada {len(df)}")
    inser_new_registro = 0
    if len(df) > 0:
       
        falha_ = 0

        # update_info_process(self, id_insert_return[0])
        alter_status(self, id_insert_return[0])
        with self.db.get_connection() as conn:
              conn.autocommit = False
              for registro in lista:
                sucesso = insert_base_interpol(self,registro,conn)
                if sucesso:
                    inser_new_registro +=1
                else:
                    falha_ +=1


    
                conn.commit()
    else:
        obs = f"SEM ALTERACAO NOS DADOS {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"
        alter_status(self, id_insert_return[0],obs)
    # print(df)

    # print(f"{minha_tabela_montada}")
    print(f"TOTAL DE REGISTROS INSERIDOS :::::{inser_new_registro}")
    # if minha_tabela_montada:
    convertida =  minha_tabela_montada.to_html(index=False, border=1, justify='center')
    corpo = f"Captura dos dados interpol {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} <br><br> {convertida}"
    enviar_email_all(corpo)

       



    # for pessoa in tratamento.get('_embedded', {}).get('notices', []):
            
          
    #     # print(pessoa.get('_links', {}).get('thumbnail',{}).get('href'))
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



def alter_status(self, id, obs = None):


     with ConectionClass.DbConnect(self.config, auto_commit=False) as conn_status:
             cursor_initil = conn_status.cursor()
             if obs is None:
                lista_update = {'status': self.true, 'alter_id': id} 
             else: 
                lista_update = {'status': self.true, 'alter_id': id, 'obs' : obs}

             update_info_process(self,lista_update,cursor_initil,conn_status)
             conn_status.commit()
             cursor_initil.close()

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