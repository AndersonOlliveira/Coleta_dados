import json
from Logs import ClassLogger
import os
import pandas as pd
from .Request import push_request,push_new_resquests
from concurrent.futures import ThreadPoolExecutor
import unicodedata
import re
from pathlib import Path


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

    for pessoa, detalhe in zip(pessoas, detalhes):
        lista_paises = pessoa.get('nationalities') or []
        nomes_paises = [mapa.get(pais, pais) for pais in lista_paises]
        pais_limpo = ','.join(nomes_paises) if nomes_paises else "N/I"

        sexo = detalhe.get('sex_id') if detalhe else None
        # crime =  [remover_acentos(warrant.get('charge')).strip() for warrant in detalhe.get('arrest_warrants', [])] if detalhe else None
        crime_lista = [remover_acentos(warrant.get('charge', '')).strip() for warrant in (detalhe.get('arrest_warrants') or [])
]
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

    print(f"Minha quantidade processada {len(df)}")
    print(f"{df}")



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

