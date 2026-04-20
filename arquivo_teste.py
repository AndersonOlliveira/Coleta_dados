import random
import string
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import smtplib
from Mail.ClassMail import enviar_email_all


TOTAL_REGISTROS = 100
MAX_THREADS = 2

dados = []

def gerar_dado(i):
    nome = ''.join(random.choices(string.ascii_uppercase, k=5))
    idade = random.randint(18, 70)
    email = f"{nome.lower()}@teste.com"
    data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return {
        "id": i,
        "nome": nome,
        "idade": idade,
        "email": email,
        "data": data
    }

# 🔹 Gerando dados com threads
def gerar_dados_thread():
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        resultados = list(executor.map(gerar_dado, range(1, TOTAL_REGISTROS + 1)))
    return resultados

# 🔹 Criar tabela HTML
def montar_tabela_html(dados):
    html = """
    <html>
    <body>
        <h2>Relatório de Dados</h2>
        <table border="1" style="border-collapse: collapse;">
            <tr>
                <th>ID</th>
                <th>Nome</th>
                <th>Idade</th>
                <th>Email</th>
                <th>Data</th>
            </tr>
    """

    for d in dados:
        html += f"""
            <tr>
                <td>{d['id']}</td>
                <td>{d['nome']}</td>
                <td>{d['idade']}</td>
                <td>{d['email']}</td>
                <td>{d['data']}</td>
            </tr>
        """

    html += """
        </table>
    </body>
    </html>
    """

    return html

# 🔹 Execução principal
if __name__ == "__main__":
    dados = gerar_dados_thread()
    html = montar_tabela_html(dados)
    
    # Opcional: salvar para teste
    with open("teste.html", "w", encoding="utf-8") as f:
        f.write(html)

    enviar_email_all(html)

    print("Processo finalizado!")