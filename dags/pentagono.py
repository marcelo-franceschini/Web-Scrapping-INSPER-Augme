# IMPORTS AIRFLOW
from airflow.decorators import dag, task
import pendulum

# IMPORTS CÓDIGO
import requests
from bs4 import BeautifulSoup
from pandas import read_excel
from os import path, mkdir
from urllib.request import pathname2url
from augme_utils.vanadio import emailVanadio


# ALTERAR
PATH_EXCEL = r"/home/data/Lista Codigos e Fiduciário.xlsx"
PATH_DOWNLOAD = r"/home/data/downloads/pentagono"
# NÃO ALTERAR
DOWNLOAD_BASE_URL = "https://pentagonotrustee.com.br/Site/DownloadBinario?id="


@dag(
    "Pentagono",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["WebScrapping", "Pentagono"],
)
def pentagono():
    
    @task()
    def get_ifs_from_excel(PATH_EXCEL, ag_fid):
        """
        Lê os ativos a partir do arquivo de Excel
        """
        df = read_excel(PATH_EXCEL, skiprows=1, usecols="B,C")
        return list(df[df["Fiduciario"] == ag_fid]["Codigo"])

    @task()
    def get_ids_from_website(ativos):
        documentos = []
        for ativo in ativos:
            soup = BeautifulSoup(
                requests.get(
                    f"https://pentagonotrustee.com.br/Site/DetalhesEmissor?ativo={ativo}&aba=tab-2&tipo=1"
                ).content
            )
            documentos.extend(
                list(
                    map(
                        lambda x: (
                            ativo,
                            x["title"],
                            DOWNLOAD_BASE_URL + x["onclick"][16:-1],
                        ),
                        soup.find_all("a", {"style": "color: dimgrey"}),
                    )
                )
            )
        return documentos

    @task()
    def download_documents(documentos):
        downloaded_files = []
        for documento in documentos:
            # Verifica se a a pasta AGENTE/ATIVO existe
            folder_agente_ativo = path.join(PATH_DOWNLOAD, documento[0])
            if not path.exists(folder_agente_ativo):
                mkdir(folder_agente_ativo)
            # Verifica se arquivo existe
            file_path = path.join(folder_agente_ativo, documento[1])
            if not path.exists(file_path):
                # Download
                with open(file_path, "wb") as file:
                    file.write(requests.get(documento[2], stream=True).content)
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
            mailbox.send_mail(receiver="", subject="PENTAGONO", body=email_body)

    ativos_excel = get_ifs_from_excel(PATH_EXCEL, "PENTAGONO DTVM")
    documentos = get_ids_from_website(ativos_excel)
    new_files = download_documents(documentos)
    if new_files:
        send_new_files_email(prepare_email_body(new_files))


pentagono()
