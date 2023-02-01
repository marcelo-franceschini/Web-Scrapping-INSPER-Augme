# IMPORTS AIRFLOW
from airflow.decorators import dag, task
import pendulum

# IMPORTS CÓDIGO
import requests
import json
from pandas import read_excel
from os import path
from os import mkdir
import re
from urllib.request import pathname2url
from augme_utils.vanadio import emailVanadio

# ALTERAR
PATH_EXCEL = r"/home/data/Lista Codigos e Fiduciário.xlsx"
PATH_DOWNLOAD = r"/home/data/downloads/oliveira"
# NÃO ALTERAR
URL_TO_IGNORE = r"https://www.oliveiratrust.com.br/portal/leitor/#"


@dag(
    "Oliveira",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["WebScrapping", "Oliveira"],
)
def oliveira():
    @task()
    def get_ifs_from_excel(PATH_EXCEL, ag_fid):
        """
        Lê os ativos a partir do arquivo de Excel
        """
        df = read_excel(PATH_EXCEL, skiprows=1, usecols="B,C")
        return list(df[df["Fiduciario"] == ag_fid]["Codigo"])

    @task()
    def get_ids_from_website():
        """
        Faz a requisição GET para pegar todos os ativos do site
        """
        papeis = {}
        params = {
            "indexador": "TODOS",
            "order": "nome",
            "page": "0",
            "search": "",
            "type": "todos",
        }
        response = requests.get(
            "https://api.oliveiratrust.com.br/v1/titulos/todos", params=params
        ).json()
        return response

    @task()
    def filtra_ativos(ativos_excel, ativos_site):
        """
        Filtra os ativos do site com os ativos do Excel
        """
        return list(
            map(
                lambda x: (x["tit"], x["tipo"], x["cod_sirsan"]),
                filter(lambda x: x["cod_sirsan"] in ativos_excel, ativos_site),
            )
        )

    @task()
    def download_documents(ativos_filtrados):
        downloaded_files = []
        for ativo in ativos_filtrados:
            # Pega o código de Download do ativo
            download_id = requests.get(
                f"https://api.oliveiratrust.com.br/v1/titulos/{ativo[0]}"
            ).json()[0]["codigo_operacao"]
            documentos = requests.get(
                f"https://api.oliveiratrust.com.br/v1/titulos/{ativo[1].lower()}/downloads/{download_id}"
            ).json()
            if len(documentos) != 0:
                folder_agente_ativo = path.join(PATH_DOWNLOAD, ativo[2])
                # Verifica se a pasta AGENTE/ATIVO existe
                if not path.exists(folder_agente_ativo):
                    mkdir(folder_agente_ativo)
                for documento in documentos:
                    folder_agente_ativo_tipodoc = path.join(
                        folder_agente_ativo, documento["subitem"].strip()
                    )
                    # Verifica se a pasta AGETE/ATIVO/TIPODOCUMENTO
                    if not path.exists(folder_agente_ativo_tipodoc):
                        mkdir(folder_agente_ativo_tipodoc)
                    file_name = (
                        documento["descricao"] + "-" + path.basename(documento["link"])
                    )
                    file_path = path.join(
                        folder_agente_ativo_tipodoc, re.sub("[\/]", "", file_name)
                    )
                    # Verifica se o arquivo existe
                    if not path.exists(file_path):
                        # Download
                        with open(file_path, "wb") as file:
                            file.write(
                                requests.get(
                                    documento["link"].replace(URL_TO_IGNORE, "")
                                ).content
                            )
                        downloaded_files.append(file_path)
        return downloaded_files

    @task()
    def prepare_email_body(new_files):
        email_body = "Novos arquivos foram salvos no diretório:<br><br><br>"
        for new_file_path in new_files:
            email_body += f'<a href="{pathname2url(path.abspath(new_file_path))}">{path.basename(new_file_path)}</a><br>'
        return email_body

    @task()
    def send_new_files_email(email_body):
        with emailVanadio.AugmeMail() as mailbox:
            # Colocar os emails aqui e assunto aqui
            mailbox.send_mail(receiver="", subject="OLIVEIRA", body=email_body)

    ativos_excel = get_ifs_from_excel(PATH_EXCEL, "OLIVEIRA TRUST DTVM ")
    ativos_site = get_ids_from_website()
    ativos_filtrados = filtra_ativos(ativos_excel, ativos_site)
    new_files = download_documents(ativos_filtrados)
    if new_files:
        send_new_files_email(prepare_email_body(new_files))


oliveira()
