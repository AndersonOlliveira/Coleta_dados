import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter, defaultdict
from datetime import datetime
from Mail.ClassMail import enviar_email_all


from Model.ClassModel import get_data_match_name_base,search_from_name_interpol,push_cpf
from Mail.ClassMail import enviar_email_all




def process_match_name(self):
  
    
    contador_por_matchName = defaultdict(lambda: {"QTUPDATE": 0, "ERROR": 0})
    falhas_ids = []
   

    # with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
    #       future_interpol = executor.submit(get_data_match_name_base, self)
    #       lista_name_braisil = future_interpol.result()

    lista_name_braisil = get_data_match_name_base(self)

   
   
    if lista_name_braisil:  
        #MINHA WORKES
        lista_sucesso = []
        with ThreadPoolExecutor(max_workers=self.max_workers_conn) as executor:
            futures = [
                 executor.submit(search_from_name_interpol,self,lista_names.get('nome'), lista_names.get('data_nascimento'),lista_names.get('ID_INTERPOL'),lista_names.get('id_tabela'))
                       for lista_names in lista_name_braisil
                    ]
            
            for future in as_completed(futures):
                result = future.result()
                lista_sucesso.append(result)

        




        for result_lista in lista_sucesso:
                  
            if result_lista['status'] == "sucesso":
                print(f"MINHA LISTA DE SUCESSO!")
                print(f"MINHA LISTA DE SUCESSO! {result_lista['ID_COLUNA_INTERPOL']}")

            

                with ThreadPoolExecutor(max_workers=self.max_workers_conn) as executor:
                    contador_por_matchName[result_lista['ID_COLUNA_INTERPOL']]["QTUPDATE"] += 1
                        #SE EXISTIR VOU ATUALIAZR O NOVO REGISTRO COM O ID DA TABELA
                    future_interpol = executor.submit(push_cpf,self,result_lista['CPF'], result_lista['ID_COLUNA_INTERPOL'])
                    udpate_match_name = future_interpol.result()
                    # udpate_match_name = push_cpf(self,result['CPF'], result['ID_COLUNA_INTERPOL'])
                    print(f"MINHA ATUALIZAÇÃO DO CPF {udpate_match_name}")
            else:
                falhas_ids.append(result_lista)
                                                   
                contador_por_matchName[result_lista['ID_COLUNA_INTERPOL']]["ERROR"] += 1

            




        #print(contador_por_matchName)
        # return
       

                            
      
                


                
            
        
        if falhas_ids:
                tabela_error = pd.DataFrame(falhas_ids)
                tabela_error = tabela_error.fillna(0) 
                convertida_error =  tabela_error.to_html(index=False, border=1, justify='center')
                corpo_error = f"Lista de dados com error :<br> {convertida_error}"
                corpo = f"""
                <h2 style="color:green;">Match Names Não Encontrados Base ProScore</h2>
                <p>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>"""
                html_final = f"""<html><body> {corpo}
                <hr>
                {corpo_error if corpo_error else "<p>Sem erros encontrados</p>"}
                </body></html>
                """
                
                
                result_email = enviar_email_all(html_final)

            
                return result_email