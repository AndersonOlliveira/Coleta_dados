import json
from Logs import ClassLogger
import os
import pandas as pd



# def trata_json(dados):
def trata_json():
    
    
    

    tratamento = []
    lista = []
    caminho = os.path.join(os.path.dirname(__file__), 'arquivo.json')
    # print("Files in current directory:", os.listdir(os.getcwd()))
    # print("acchei?", caminho)
    with open(caminho) as arquivo:
        # 2. Carregar o conteúdo
        # dados = json.load(arquivo)

        # print(json.dumps(dados,  indent=4))
        dados_decoficados = json.load(arquivo)

        tratamento = dados_decoficados


        print('como fica a visualizacao dos meus dados')
    print(tratamento.get('_links'))
    print(len(tratamento.get('_links')))


    # if len(tratamento.get('_links') > 0):
    #     print(f"a lista esta vazia!")
        # AQUI ACESSO O LINKS DAS PAGINA PARA QUE EU PASSA CONSULTAR OS OUTROS RETORNOS SO E LISTADO 160 RESULTADO POR PAGINA
    


    pd.set_option('display.max_colwidth', None)
    for pessoa in tratamento.get('_embedded', {}).get('notices', []):
        # print(pessoa.get('_links', {}).get('thumbnail',{}).get('href'))
        lista.append({
            'nome': pessoa.get('name'),
            'forename': pessoa.get('forename'),
            'data_nascimento': pessoa.get('date_of_birth'),
            'nacionalidade': pessoa.get('nationalities'),
            'entity_id': pessoa.get('entity_id'),
           # 'thumbnail': pessoa.get('_links', {}).get('thumbnail', {})
             'thumbnail': pessoa.get('_links', {}).get('thumbnail', {}).get('href')
        })
       

    df = pd.DataFrame(lista)
       

    # print(df)



        # aqui normalizo de acordo com o que vem da api transformando em tabela
    # dados_tratamento = pd.json_normalize(
    #       tratamento['_embedded']['notices'])
    
     
      
    # print(dados_tratamento)
    # print(dados_tratamento['_links.thumbnail.href'])
    # print(dados_tratamento['_links.images.href'])



     