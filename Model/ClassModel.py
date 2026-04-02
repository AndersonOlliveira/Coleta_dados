# from Conexao.Conection import conexao
from tabulate import tabulate
import time
import json
from typing import Dict, List, Optional, Tuple
import threading
from Logs import ClassLogger
# from Conexao import DbConnect
from Conexao import ConectionClass
from psycopg2.extras import RealDictCursor
from datetime import datetime
from psycopg2.pool import ThreadedConnectionPool



def insert_interpol(self,registro: Dict, cursor, connection):



    query = """
           INSERT INTO fontes_download.interpol_download 
               (periodizacao, data_captura, link_captura,parametros_captura)
           VALUES 
               (%s, %s, %s,%s) RETURNING id; """

    print(f"Registro a ser inserido: {registro}")
    # return
    try:
        cursor.execute(query, (
            registro['periodizacao'],
            registro['data_captura'],
            registro['url'],
            registro['siglas']
            # registro['sucesso']
        ))
     
        
       
      
        with self.lock:
          
            self.batch_counter_status1 += 1
           
            if self.batch_counter_status1 >= 1:
               connection.commit()
               self.batch_counter_status1 = 0

               # RETORNO DO ID PARA REALIZAR O UPDATE
               novo_id = cursor.fetchone()[0]


               ClassLogger.logger.info(f"Status atualizado para ::  - id retornado {novo_id} ")


               return novo_id




               

    except Exception as e:
     ClassLogger.logger.error(f"Erro ao atualizar status para ::  - {str(e)}")


#inserir o lote dos registos
def insert_base_interpol(self, registro: dict, conn):



    # print(registro['nome_completo'])
    # return 



    query = """
           INSERT INTO public.interpol_dados 
               (nome, sexo, nascimento,nacionalidade,idioma,acusacao,foto,data_consulta_fonte,hora_consulta_fonte,nome_buscado)
           VALUES  (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s) """

    print(f"Registro a ser inserido: {registro}")
    print(f"{registro['nome_completo']}")
    print(f"{registro['sexo']}")
    print(f"{registro['data_nascimento']}")
    print(f"{registro['nacionalidade']}")
    print(f"{registro['idiona']}")
    print(f"{registro['acusacao']}")
    print(f"{registro['thumbnail']}")
    print(f"{registro['data_consulta']}")
    print(f"{registro['hora_consulta']}")
    print(f"{registro['id_interpol']}")

    print(query)
    print((
    registro['nome_completo'],
    registro['sexo'],
    registro['data_nascimento'],
    registro['nacionalidade'],
    registro['idiona'],
    registro['acusacao'],
    registro['thumbnail'],
    registro['data_consulta'],
    registro['hora_consulta'],
    registro['id_interpol']
))
    
    # return
    try:
       with conn.cursor() as cursor:
            cursor.execute(query, (
                registro['nome_completo'],
                registro['sexo'],
                registro['data_nascimento'],
                registro['nacionalidade'],
                registro['idiona'],
                registro['acusacao'],
                registro['thumbnail'],
                registro['data_consulta'],
                registro['hora_consulta'],
                registro['id_interpol']
            ))

            return cursor.rowcount > 0
            
            ClassLogger.logger.info(f"Status atualizado par ")
    except Exception as e:
       ClassLogger.logger.error(f"falha em inserir os dados na base  - {repr(e)}")
       return False


def update_info_process(self,registro: Dict, cursor, connection):



    query = """UPDATE fontes_download.interpol_download  SET 
                  processado = %s  WHERE id = %s ;"""

    print(f"Registro a ser inserido: {registro}")
    # return


    set_parts = ["processado = %s"]
    params = [registro['status']]
    if registro['obs'] is not None:
        set_parts.append("obs = %s")
        params.append(registro['obs'])
    query = f"UPDATE fontes_download.interpol_download SET {', '.join(set_parts)} WHERE id = %s"
    params.append(registro['alter_id'])
    
    try:
        cursor.execute(query, tuple(params))
     
        
       
      
        with self.lock:
          
            self.batch_counter_status1 += 1
           
            if self.batch_counter_status1 >= 1:
               connection.commit()
               self.batch_counter_status1 = 0

            ClassLogger.logger.info(f"Status atualizado do id {registro['alter_id']}  com o Status {registro['status']} {datetime.now().strftime("%d/%m/%Y %H:%M")} ")

    except Exception as e:
     ClassLogger.logger.error(f"Erro ao atualizar status para ::  - {str(e)}")




def buscar_teste(self):

            query = ("""SELECT * FROM fontes_download.interpol_download """)
            
            # params = []

            # if self.idProcesso is not None:
            # query += 'AND p.processo_id = %s'
            # params.append(self.idProcesso)
                        
            # query += " ORDER BY random() LIMIT %s;";
            # params.append(self.batch_size)

                
            # classLogger.logger.info(query)
            # classLogger.logger.warn(f"[DEBUG SQL] Query gerada:\n{query}")

            try:
                
                ClassLogger.logger.warn(f"[DEBUG SQL] Parâmetros: ")
            
                with ConectionClass.DbConnect(self.config) as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                        cursor.execute(query)
                        # cursor.execute(query, tuple(params))
                        registros = cursor.fetchall()
                        ClassLogger.logger.info(f"Capturados {len(registros)} registros para processamento.")
                        return [dict(registro) for registro in registros]
            
         
            except Exception as e:
                   ClassLogger.logger.error(f"Falha em caputrar os dados - {str(e)}")


            
            
                


# def search_data_interpol(self,idinterpol, cursor,conection):
def search_data_interpol(conn,idinterpol):

            print(f"meus intens do self {idinterpol}")
            
            #retornando um boleano
#CAMPO VAI SER TROCADO PARA ID INTERPOL
            query = ("""SELECT EXISTS(
                        SELECT 1 FROM public.interpol_dados WHERE nome_buscado = %s) as exists
                     """)
            
            ClassLogger.logger.warn(f"[DEBUG SQL] Parâmetros: {idinterpol} ")
            ClassLogger.logger.warn(f"[DEBUG SQL] query: {query} ")
            #ClassLogger.logger.warn(f"[DEBUG SQL] self.db.get_connection: {db.get_connection} ")

            try:
        
                # with self.db.get_connection() as conn:
                     with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                        # cursor.execute(query)
                        cursor.execute(query,(idinterpol,))

                        
                        retorno = cursor.fetchone()['exists']
                        print(f"meu resultado do cursor {retorno}")



                        
                        return retorno
                        print(f"O QUE TENHO DENTO DA VARIAVEL RESULT {registros}")
                        ClassLogger.logger.info(f"Capturados {len(registros)} registros para processamento.")
                        # return [dict(registro) for registro in registros]
                    
            except Exception as e:
                     ClassLogger.logger.error(f"Falha em caputrar os dados o erro vem aqui? - {str(e)}")
                     print(f"que erro foi capturado na busca - {str(e)}")


def exists_by_name(conn, person):
            
            print('estou chegando aqui no by nome?')
            print(f'meu nome {person}')

            query = """
                SELECT EXISTS(
                    SELECT 1 
                    FROM public.interpol_dados 
                    WHERE UPPER(nome) = UPPER(%s)) as exists
            """
            try:
                
                # with self.db.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                        cursor.execute(query, (person,))
                        resultado = cursor.fetchone()['exists']
                        print(f"qual e o resultado {resultado}")
                        return resultado
                                
            except Exception as e:
                ClassLogger.logger.error(f"Falha em caputrar os dados o erro vem aqui? - {str(e)}")
                print(f"que erro foi capturado na busca - {str(e)}")