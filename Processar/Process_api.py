from Logs import ClassLogger
from Conexao import  ConectionClass
from .Request import push_request 
from typing import Dict, List,Optional, Tuple
import json 
from .Tratar_json import trata_json
import os
from pathlib import Path
from functions.funcoes import remover_acentos, remover_conhetes, tratar_entrada






def process_api(self):
    
    #envio a lista de paises para busca
    # caminho_countres = os.path.join(os.path.dirname(__file__), 'Arquivos/countries.json')
    caminho_countries = Path('Arquivos/countries_poucos.json')

    print(f"meu caminho : {caminho_countries}")

    if caminho_countries.is_file():
        print(f"ARQUIVO COM A LISTA DE PAISES EXISTE DENTRO DO PROJETO POSSO DAR SEGUIMENTO COM O PROCESSO!")

        dados_interpol = dict()
            
            
        ClassLogger.logger.info(f"ESTOU SAINDO AQUI PARA FAZER A CONSULTA {self.batch_size}")
            
            #para chamar a api e exeturar os dados
        retorno_api , id_insert_return = push_request(self,caminho_countries)


        # print(json.dumps(retorno_api , indent=4))
        # print(id_insert_return )

        # return


        # print(f"meu retorno da api : {retorno_api}")
        # with open(caminho_countries) as countries:
        #      dados_decoficados = json.load(countries)
            

        #      print(dados_decoficados)
        # 
        result_lista = trata_json(self,caminho_countries,retorno_api,id_insert_return)
        
       
        # if result_lista:

            #mou montar o processo para atualizar os dados e colocar ativo e inativo
 

    

    # ClassLogger.logger.info(f"Tenho dados vindo aqui do request {retorno_api}")