import json
from Logs import ClassLogger
import os
import pandas as pd
from .Request import push_request,push_new_resquests
from concurrent.futures import ThreadPoolExecutor
import unicodedata
import re
from pathlib import Path
from collections import Counter
from datetime import datetime



# def trata_json(dados):
def trata_json(self,caminho_countries, retorno_api):
    # print(json.dumps(caminho_countries,  indent=4))
    tratamento = []
    lista = []
    caminho = os.path.join(os.path.dirname(__file__), 'arquivo_registros.json')
    caminho_countress = Path('Arquivos/countries.json')
   
    # print("Files in current directory:", os.listdir(os.getcwd()))
    # print("acchei?", caminho)
    # with open(caminho) as arquivo:
    #     # 2. Carregar o conteúdo
    #     # dados = json.load(arquivo)

    #     # print(json.dumps(dados,  indent=4))
    #     dados_decoficados = json.load(arquivo)

    #     tratamento = dados_decoficados


    #     print('como fica a visualizacao dos meus dados')
   # NESTE _LINKS ele e o acesso por pagina dos dados, pois o resultado e de 20 resultado por pagina
   # COMO O RESULTADO E 160 REGISTRO POR AI, DEFINI JÁ PASASNDO OS DADOS DENTRO 
    # print(tratamento.get('_links'))
    # print(len(tratamento.get('_links')))


    # if len(tratamento.get('_links') > 0):
    #     print(f"a lista esta vazia!")
        # AQUI ACESSO O LINKS DAS PAGINA PARA QUE EU PASSA CONSULTAR OS OUTROS RETORNOS SO E LISTADO 160 RESULTADO POR PAGINA
    mapa = {
    # "DZ": "Argélia",
    # "IN": "Índia",
    # "RU": "Rússia",
    # "UA": "Ucrânia",
    # "FR": "FRANCA",
    # "BR": "BRASIL",
    }

   
    print("como vem o retorno da api?")
    print(type(retorno_api))
    print(json.dumps(retorno_api,  indent=4))

    # dados_decoficados = list(json.load(retorno_api))
    # print("meu dados_decoficados da api\n")
    # print(dados_decoficados)

    # tratamento = dados_decoficados

    tratamento = retorno_api
    # lista_coutries = caminho_countries
    lista_coutries = caminho_countress
            
    with open(lista_coutries) as lista_coutrie:
         lista_decodificadas = json.load(lista_coutrie)
         print(lista_decodificadas)
         for pais in lista_decodificadas:
             codigo_pais = pais.get('value')
             nome_pais = pais.get('name')
             mapa[codigo_pais] = nome_pais


    print('meu lista de mapa')         
    print(mapa)         


    
    
    
    
    lista_urls = []
    tabela_atualizar =[]
    pessoas = tratamento[0].get('_embedded', {}).get('notices', [])
    
    for pessoa in pessoas:
        lista_url = pessoa.get('_links', {}).get('self', {}).get('href')
        print(f"Minha lista de url individual {lista_url}")
    
        if lista_url:
           lista_urls.append(lista_url)
           
    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
        detalhes = list(executor.map(
        lambda url: push_new_resquests(url, self.time_sleps),
        lista_urls
    ))
    


    print(f"Minha lista de url individual {detalhes}")
    pd.set_option('display.max_colwidth', None) # PARA VISUALIZAR EM ESCALA GRANDE 

     #imcremetador
    contador_paises = Counter()
    dupla_nacionalidade = 0
       
    # for pessoa in pessoas:
    #     lista_paises = pessoa.get('nationalities') or []
    
    #     contador_paises.update(lista_paises)

    
    for pessoa in pessoas:
        lista_paises = pessoa.get('nationalities') or []

        if len(lista_paises) == 1:
        #    contador_paises.update(lista_paises)
           contador_paises[lista_paises[0]] +=1
        elif len(lista_paises) > 1:
            dupla_nacionalidade += 1
   
   
    
    paises_unicos = set()
    for pessoa in pessoas:
        lista_paises = pessoa.get('nationalities') or []
        for sigla in lista_paises:
            nome_pais = mapa.get(sigla, sigla)
            paises_unicos.add(nome_pais)

            
    paises_str = ", ".join(sorted(paises_unicos))


    linha_tabela = {
    'DATA CAPTURA': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    'TOTAL': len(pessoas),
    'DUPLA NACIONALIDADE': dupla_nacionalidade,
    'PAISES': paises_str
    }

    tabela_atualizar.append(linha_tabela)


   
    # linha_tabela = {
    #     'DATA CAPTURA': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    #     'DUPLA NACIONALIDADE': dupla_nacionalidade,
    #     'TOTAL': len(pessoa)
    # }

    # for sigla, total in contador_paises.items():
    #     nome_pais = mapa.get(sigla, sigla)
    #     nomes = [mapa.get(p, p) for p in nacionalidades]
        
    #     linha_tabela[nome_pais] = total
        
    #     tabela_atualizar.append(linha_tabela)

    for pessoa, detalhe in zip(pessoas, detalhes):
        lista_paises = pessoa.get('nationalities') or []
        nomes_paises = [mapa.get(pais, pais) for pais in lista_paises]
        pais_limpo = ','.join(nomes_paises) if nomes_paises else "N/I"

        #contar a quantide de registros por pais 
        # total_registros_pais = [mapa.get(pais, pais) for pais in pessoa.get('nationalities', [])]

        # tabela_atualizar.append({
        #      pais_limpo : {'TOTAL_REGISTROS': len(total_registros_pais)}
        # })
        
      

        sexo = detalhe.get('sex_id') if detalhe else None
        # crime =  [remover_acentos(warrant.get('charge')).strip() for warrant in detalhe.get('arrest_warrants', [])] if detalhe else None
        crime_lista = [remover_acentos(warrant.get('charge', '')).strip() for warrant in (detalhe.get('arrest_warrants') or [])]
        crime = ", ".join(crime_lista) if crime_lista else "N/I"
        # idiona = [remover_conhetes(lang) for lang in detalhe.get('languages_spoken_ids', [])] if detalhe else None
        idiona = ", ".join(item.strip("[] ").strip() for item in detalhe.get('languages_spoken_ids', []) or []) if detalhe and detalhe.get('languages_spoken_ids') else "N/I"
        
        # idiona = ", ".join(item.strip("[] ").strip() for item in detalhe.get('languages_spoken_ids', []) or []) if detalhe else None


      

        print(f"crime localizado?: {crime}")
        print(type(idiona))

        lista.append({
            # 'primeiro_nome': pessoa.get('name'),
            'nome_completo': "{} {}".format(pessoa.get('name'), pessoa.get('forename')),
            # 'nome_do_meio': pessoa.get('forename'),
            'data_nascimento': pessoa.get('date_of_birth').replace('/','-') if pessoa.get('date_of_birth') else None,
            'nacionalidade': pais_limpo.upper(),
            'id_interpol': pessoa.get('entity_id').replace('/','-') if pessoa.get('entity_id') else None,
            'sexo': sexo, 
            'acusacao': crime,
            'idiona': idiona,
            'thumbnail': pessoa.get('_links', {}).get('thumbnail', {}).get('href')
        })




    
    
    



    df = pd.DataFrame(lista)
    minha_tabela_montada = pd.DataFrame(tabela_atualizar)

    print(f"Minha quantidade processada {len(df)}")
    print(f"{minha_tabela_montada}")



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
    # Normaliza o texto para NFD
    texto_normalizado = unicodedata.normalize('NFD', texto)
    # Codifica para ascii, ignora erros e decodifica de volta para utf-8
    texto_normalizado =  texto_normalizado.encode('ascii', 'ignore').decode('utf-8')

    return re.sub(r'\s+', ' ',texto_normalizado).strip()

def remover_conhetes(texto):
   
   return  re.sub(r'[\[\]]', '', texto)

  # # url_completa = pessoa.get('_links', {}).get('self', {}).get('href', {})
        # url_completas = pessoa.get('link_busca', {})
        # # print(f"minha url completa : {url_completa}")
        # print(f"minha url url_completas : {url_completas}")
        
        # # 2. Faz o "parse" da URL para separar os parâmetros
        # url_fragmentada = urlparse(url_completas)
        # parametros = parse_qs(url_fragmentada.query)

        # # 3. Extrai o valor de 'nationality'
        # # O parse_qs retorna uma lista, por isso usamos [0]
        # pais_busca_principal = parametros.get('nationality')

        # print(f"pais achado primerio ? {pais_busca}")
        # print(f"pais achado primerio ? {pais_busca_principal}")


          # tabela_final.append(linha)

        # df = pd.DataFrame(tabela_final)
            
        # for sigla in lista_paises:
        #     nome_pais = mapa.get(sigla, sigla)
        #     paises_unicos.add(nome_pais)
        #     paises_str = ", ".join(sorted(paises_unicos))
        #     url_do_tratamento_dos_dados = pessoa.get('link_busca', {})
        #     # print(f"minha url completa : {url_completa}")
        #     print(f"minha url url_do_tratamento_dos_dados : {url_do_tratamento_dos_dados}")
            
        #     # 2. Faz o "parse" da URL para separar os parâmetros
        #     s = urlparse(url_do_tratamento_dos_dados)
        #     print(f"minha url S : {s}")
        #     parametros_HTTP = parse_qs(s.query)
        #     print(f"minha url parametros_HTTP : {parametros_HTTP}")

        
        #     pais_busca_principalS = parametros_HTTP.get('nationality')

        #     print(f"pais achado pais_busca_principalS ? {pais_busca_principalS}")

        #     print(f"pais achado parametros_HTTP segundoooo ? {parametros_HTTP}")
            
        #     linha_tabela = {
        #     'DATA CAPTURA': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        #     'TOTAL': len(todas_pessoas),
        #     'DUPLA NACIONALIDADE': dupla_nacionalidade,
        #     'PAISES': paises_str,
        #     'PAIS BUSCADO': pais_busca_principalS
        #     }


        
    # for pessoa in todas_pessoas:

    #     print(f"minha pessoas {pessoa}")

    #     lista_paises = pessoa.get('nationalities') or []
    #     print(f"MINHA LISTA DE PAISES DO PESSOA {lista_paises}")
       
    
    #     if len(lista_paises) == 1:
    #         contador_paises[lista_paises[0]] +=1
    #     elif len(lista_paises) > 1:
    #         dupla_nacionalidade += 1
    
    
def d():
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                with self.db.get_connection() as conn:
                    for pessoa, detalhe in zip(todas_pessoas, detalhes):
                        print(f"INICIANDO O PRCCESSAMENTO DOS DADOS PESSOAS E DETALHES")

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
                        #pego os ids da interpol para verificar só que vou ter uma dupla verificacao, pelo o id e pelo o nome
                        person_singla = next((p for p in lista_paises_chaves if p in lista_paises_unicos), 'N/I')
                        if entity_id:
                            # future_busca = executor.submit(search_data_interpol, conn, entity_id)
                            future_busca = executor.submit(search_data_interpol, self, entity_id)
                            exist_id = future_busca.result()

                            if exist_id: # aqui atualizo sempre que vinher os dados
                                executor.submit(update_data_interpol, conn, entity_id, naturalidade,thumbnail, pais_procurado,data_captura)

                        if not exist_id:
                            #colocar theads aqui
                            future_busca_name = executor.submit(exists_by_name,self,name_person)
                            # future_busca_name = executor.submit(exists_by_name,conn,name_person)
                            exist_name = future_busca_name.result()
                            if exist_name:
                                #faco o update para o id da interpol para a busca ser mais acertiva 
                                executor.submit(update_id_interpol, conn, name_person , entity_id)

                        
                    
                        if not exist_id and not exist_name:
                            
                            
                            
                            lista_paises = pessoa.get('nationalities') or []
                            nomes_paises = [mapa.get(pais, pais) for pais in lista_paises]
                            pais_limpo = ','.join(nomes_paises) if nomes_paises else "N/I"
                            sexo = detalhe.get('sex_id') if detalhe else None
                            crime_lista = [remover_acentos(warrant.get('charge', '')).strip() for warrant in (detalhe.get('arrest_warrants') or [])]
                            crime = ", ".join(crime_lista) if crime_lista else "N/I"
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
                            contador_por_pais[person_singla]["INSERT"] += 1

                        else:
                            print(f"Pulo este dado {entity_id} || nome: {name_person}  que pais ???{lista_paises_chaves}")
                            print(f"Pulo este dado {entity_id} | nome: {name_person} + {person_singla}")
                            contador_por_pais[person_singla]["NA"] += 1
                            

        for linha in tabela_atualizar:
            pais = linha['PAIS_BUSCADO']
            #MUNDAR PRA A
            linha['QTA A INSERIR'] = contador_por_pais[pais]["INSERT"]
            linha['QTA J/N BASE'] = contador_por_pais[pais]["NA"]
        
        pd.set_option('display.max_rows', 100)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_colwidth', None)  
        df = pd.DataFrame(lista)
    
    
    




        if len(df) > 0:
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

                    print(f"MEU RESULT {result}")
                    if result is None:
                        continue # Pula para o próximo se for None
                    
                    
                    if result['status'] == "sucesso":
                                # inser_new_registro +=1
                    contador_por_pais[result['person_sigla_unico']]["QTINSERT"] += 1
                    else:
                    falhas_ids.append(result)
                                # falha_ +=1
                    contador_por_pais[result['person_sigla_unico']]["ERROR"] += 1


        else:
            obs = f"SEM ALTERACAO NOS DADOS {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            obs_interpol = f"SEM CONSULTA INDIVIDUAL {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            alter_status(self, id_insert_return[0],obs)
            alter_status(self, id_geral_url_interpol,obs_interpol)
        

        
        for linha in tabela_atualizar:
            pais = linha['PAIS_BUSCADO']
            linha['QTA ERROR'] = contador_por_pais[pais]["ERROR"]
            linha['QTA INSERIDO'] = contador_por_pais[pais]["QTINSERT"]

        
    
        
        minha_tabela_montada = pd.DataFrame(tabela_atualizar)
        minha_tabela_montada = minha_tabela_montada.fillna(0)
        print(f"Minha quantidade a ser processada {len(minha_tabela_montada)}")
        print(f"MINHA TABELA PARA ATUALIZAR {minha_tabela_montada}")

        # return

    
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

        
        return result_email