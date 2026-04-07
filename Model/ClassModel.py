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
# def insert_base_interpol(self, registro: dict, conn, falha_ids):
def insert_base_interpol(self, registro):
   
    query = """
           INSERT INTO public.interpol_dados 
               (nome, sexo, nascimento,nacionalidade,idioma,acusacao,foto,data_consulta_fonte,hora_consulta_fonte,nome_buscado,naturalidade)
           VALUES  (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

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
    print(f"{registro['naturalidade']}")
    print(f"minha person sigla {registro['person_sigla_unico']}")

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
    registro['id_interpol'],
    registro['naturalidade'],
    registro['person_sigla_unico']
))
    
    # return
    try:
         
     with self.db.get_connection() as conn:
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
                registro['id_interpol'],
                registro['naturalidade'],
            ))

         return {
                    "id": registro['id_interpol'],
                    "status": "sucesso",
                    "person_sigla_unico": registro['person_sigla_unico']
                } 
       
            # cursor.rowcount > 0
            
         ClassLogger.logger.info(f"INSERIDO ")
    except Exception as e:
        # ClassLogger.logger.error(f"falha em inserir os dados na base  - {repr(e)}")

    #    lista_nao_inserido.append({registro['id_interpol']})
    #    falha_ids.append({
    #        "id": registro['id_interpol'],
    #        "erro": str(e)
    #    })
        return {
                "id": registro['id_interpol'],
                "status": "erro",
                "person_sigla_unico": registro['person_sigla_unico'],
                'error': str(e)
            } 

    #    return False

def update_data_interpol(conn,id, nat, thumb):
    
    query = """UPDATE public.interpol_dados SET 
                  naturalidade = %s , foto = %s WHERE nome_buscado = %s ;"""
    


    print(f"Registro a ser ida na: {id}")
    print(f"Registro a ser nat info: {nat}")
    print(f"Registro a ser nat thumb: {thumb}")
     
    try:
         
         with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (nat,thumb,id))

                if cursor.rowcount > 0:
                    print(f"TIVE SUCESSO EM ATUALIZAR")
                    return {
                        "status": "sucesso"
                    }
                else:
                    print(f"TIVE FALHA EM ATUALIZAR")
                    return {
                        "status": "erro",
                        "error": "No rows updated"
                    }
    
    except Exception as e:
            ClassLogger.logger.error(f"Erro ao atualizar a NATURALIDADE :: {str(e)}")
           
            return {
                    "status": "erro",
                    "error": str(e)
                }

       
def update_id_interpol(conn,name_person, id):
    
    query = """UPDATE public.interpol_dados SET 
                  nome_buscado = %s  WHERE nome = %s ;"""

    print(f"Registro a ser name_person na: {name_person}")
    print(f"Registro a ser nat info: {id}")
     
    try:
         
         with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (id,name_person))
             
           
                return {
                    "status": "sucesso"
                }
    
    except Exception as e:
            ClassLogger.logger.error(f"Erro ao atualizar o campo nome Buscado :: {str(e)}")
           
            return {
                    "status": "erro",
                    "error": str(e)
                }

       
              




def update_info_process(self,registro: Dict, cursor, connection):



    query = """UPDATE fontes_download.interpol_download  SET 
                  naturalidade = %s  WHERE id = %s ;"""

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

    except Exception as  e:
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

# PEGOS OS DADOS PARA CHAMAR A API E ATUALIZAR PARA VER ESTA ATIVO E INATIVO
# def push_data_interpol(conn):
     
#             print('PEGO OS DADOS PARA FAZER A PESQUISA POR NOME PARA PEGAR O CPF ONDE E SOMENTE BRALISEIRO')
          

#             query = """
#                SELECT UPPER(nome), nascimento, nome_buscado AS ID_INTERPOL, nacionalidade, id as id_tabela FROM public.interpol_dados where nacionalidade  LIKE '%BRAZIL%' AND nome_buscado is not null
#                GROUP BY nome,nascimento,nome_buscado,nacionalidade,id ORDER BY nome """
#             try:
                
#                 # with self.db.get_connection() as conn:
#                 with conn.cursor(cursor_factory=RealDictCursor) as cursor:
#                         cursor.execute(query,)
#                         resultado = cursor.fetchAll()
#                         print(f"qual e o resultado {resultado}")
#                         return resultado
                                
#             except Exception as e:
#                 ClassLogger.logger.error(f"Falha em caputrar os dados o erro vem aqui? - {str(e)}")
#                 print(f"que erro foi capturado na busca - {str(e)}")




# def get_data_match_name(conn, name,ano):
     
     
#       query = """SELECT UPPER(nome), nascimento, nome_buscado AS ID_INTERPOL, nacionalidade, id as id_tabela FROM public.interpol_dados where nacionalidade  
#                   LIKE '%BRAZIL%' AND nome_buscado is null
#                   GROUP BY nome,nascimento,nome_buscado,nacionalidade,id ORDER BY nome"""
      
#       try:
                    
#             with conn.cursor(cursor_factory=RealDictCursor) as cursor:
#                             cursor.execute(query, (name,ano))
#                             resultado = cursor.fetchone()
#                             print(f"qual e o resultado {resultado}")
#                             return resultado
                                    
#       except Exception as e:
#                     ClassLogger.logger.error(f"Falha em caputrar os dados o erro vem aqui? - {str(e)}")
#                     print(f"que erro foi capturado na busca - {str(e)}")



def get_data_match_name_base(self) -> List[Dict]: 
     
     
      query = """SELECT UPPER(nome) as nome, nascimento, nome_buscado AS ID_INTERPOL, nacionalidade, id as id_tabela FROM public.interpol_dados where nacionalidade  
                  LIKE '%BRAZIL%' AND nome_buscado is null
                  GROUP BY nome,nascimento,nome_buscado,nacionalidade,id ORDER BY nome"""
      
      try:
                    
            with self.db.get_connection() as conn:
                 with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                      cursor.execute(query,)
                      registros = cursor.fetchall()
                
                      if not registros:
                        return None
            
            
                
                 ClassLogger.logger.info(f"Capturados {len(registros)} registros para processamento.")
                 return [dict(registro) for registro in registros]
                                    
      except Exception as e:
                    ClassLogger.logger.error(f"Falha em caputrar os dados o erro vem aqui?dsdasdas - {str(e)}")
                    print(f"que erro foi capturado na busca - {str(e)}")


# def search_from_name_interpol(self):
      
#         query = """SELECT cntcpfcgc as cpf, cntnom as nome,
#                     cntfisncm as nascimento 
#                     FROM 
#                     cnt, cntfis 
#                     WHERE 
#                     cntid = cntfiscnt
#                     AND 
#                     UPPER(cntnom) = %s  
#                     AND 
#                     length(cntcpfcgc) = 11 
#                     AND cntfisncm = %s"""
      
#         try:
                    
#             with conn.cursor(cursor_factory=RealDictCursor) as cursor:
#                             cursor.execute(query, (name,ano))
#                             resultado = cursor.fetchone()
#                             print(f"qual e o resultado {resultado}")
#                             return resultado
                                    
#     except Exception as e:
#                     ClassLogger.logger.error(f"Falha em caputrar os dados o erro vem aqui? - {str(e)}")
#                     print(f"que erro foi capturado na busca - {str(e)}")