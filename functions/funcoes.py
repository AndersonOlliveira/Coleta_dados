
from datetime import datetime, date
import unicodedata
import re
import os

# from collections import defaultdict
from Conexao import ConectionClass,ConectionPool
from pathlib import Path



def tratar_entrada(valor):
    
    valor_limpo = str(valor).replace('-', '/').strip()
    try:
        # Tenta tratar como data completa primeiro
        return datetime.strptime(str(valor_limpo), "%Y/%m/%d").date()
    except ValueError:
        try:
            # Se falhar, tenta tratar apenas como o ano
            if valor is None:
                return None
                return "0000-00-00"
            ano = int(valor)
            return date(ano, 1, 1)
        except (ValueError, TypeError):
            return None
            # return "0000-00-00"


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


def path_arquivo():
    caminho_countress = Path('Arquivos/countries.json')
    return caminho_countress


def path_arquivo_auxiliar():
    caminho = os.path.join(os.path.dirname(__file__), 'arquivo_registros.json')
  
    return caminho


def dividir_lotes(lista, tamanho):
    for i in range(0, len(lista), tamanho):
        yield lista[i:i + tamanho]


def dividir_lotes_tratar(lista1, lista2, tamanho):
    for i in range(0, len(lista1), tamanho):
        yield lista1[i:i+tamanho], lista2[i:i+tamanho]