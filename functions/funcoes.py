
from datetime import datetime, date
import unicodedata
import re
# from collections import defaultdict
from Conexao import ConectionClass,ConectionPool



def tratar_entrada(valor):
    try:
        # Tenta tratar como data completa primeiro
        return datetime.strptime(str(valor), "%Y/%m/%d").date()
    except ValueError:
        try:
            # Se falhar, tenta tratar apenas como o ano
            ano = int(valor)
            return date(ano, 1, 1)
        except ValueError:
            return "Valor inválido"


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



