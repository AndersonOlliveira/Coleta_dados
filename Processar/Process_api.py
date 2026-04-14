from Logs import ClassLogger
from Conexao import  ConectionClass
from .Request import push_request 
from typing import Dict, List,Optional, Tuple
import json 
from .Tratar_json import trata_json
import os
from pathlib import Path







def process_api(self):
    
    #envio a lista de paises para busca
    # caminho_countres = os.path.join(os.path.dirname(__file__), 'Arquivos/countries.json')
    caminho_countries = Path('Arquivos/countries_poucos.json')

    print(f"meu caminho : {caminho_countries}")
    # print(f"meu caminho : {len(caminho_countries)}")

    if caminho_countries.is_file():
        print(f"Arquivo com a lista de paises para a busca na api")
        ClassLogger.logger.info(f"ESTOU SAINDO AQUI PARA FAZER A CONSULTA {self.batch_size}")
            
            #para chamar a api e exeturar os dados
        retorno_api , id_insert_return = push_request(self,caminho_countries)


       
        result_lista = trata_json(self,caminho_countries,retorno_api,id_insert_return)
        
    