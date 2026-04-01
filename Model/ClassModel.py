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


def update_info_process(self,registro: Dict, cursor, connection):



    query = """UPDATE fontes_download.interpol_download  SET 
                  processado = %s  WHERE id = %s ;"""

    print(f"Registro a ser inserido: {registro}")
    # return
    try:
        cursor.execute(query, (
            registro['status'],
            registro['alter_id']
        ))
     
        
       
      
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
def search_data_interpol(self,idinterpol):

            print(f"meus intens do self {self}")

            query = ("""SELECT * FROM public.interpol_dados""")
            
            params = []

            if idinterpol is not None:
               query += " WHERE nome_buscado = '%s'"
               params.append(idinterpol)
            
            ClassLogger.logger.warn(f"[DEBUG SQL] Parâmetros: {params} ")
            ClassLogger.logger.warn(f"[DEBUG SQL] query: {query} ")

            try:
        
                with ConectionClass.DbConnect(self.config) as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                         cursor.execute(query)
                        # cursor.execute(query, tuple(params))
                         registros = cursor.fetchall()
                         ClassLogger.logger.info(f"Capturados {len(registros)} registros para processamento.")
                         return [dict(registro) for registro in registros]
                    
            except Exception as e:
                     ClassLogger.logger.error(f"Falha em caputrar os dados - {str(e)}")

