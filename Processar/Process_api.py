from Logs import ClassLogger
from Conexao import  ConectionClass
from .Request import push_request 
from typing import Dict, List,Optional, Tuple
import json 
from .Tratar_json import trata_json
import os
from pathlib import Path







def process_api(self):
    
    # caminho do arquivos para envio dos paises
    caminho_countries = Path('Arquivos/countries_poucos.json')

    
    
    # lista com os paises precisar existir para realizar a busca e trazer os dados
    if caminho_countries.is_file():
         #para chamar a api e exeturar os dados
        retorno_api , id_insert_return = push_request(self,caminho_countries)
        
        return trata_json(self,caminho_countries,retorno_api,id_insert_return)


        
    