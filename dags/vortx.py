# IMPORTS AIRFLOW
from airflow.decorators import dag, task
import pendulum

# IMPORTS CÓDIGO
from pandas import read_excel
from json import loads
import requests
from os import mkdir, path
import re
from urllib.request import pathname2url
from augme_utils.vanadio import emailVanadio


# ALTERAR
PATH_EXCEL = r"/home/data/Lista Codigos e Fiduciário.xlsx"
PATH_DOWNLOAD = r"/home/data/downloads/vortx"


@dag(
    "Vortx",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["WebScrapping", "Vortx"],
)
def vortx():
    @task()
    def get_ifs_from_excel(PATH_EXCEL, ag_fid):
        """
        Lê os ativos a partir do arquivo de Excel
        """
        df = read_excel(PATH_EXCEL, skiprows=1, usecols="B,C")
        return list(df[df["Fiduciario"] == ag_fid]["Codigo"])

    @task()
    def get_ids_from_website():
        ativos = []
        for tipo_ativo in range(1, 4):
            params = {"tipoOper": str(tipo_ativo)}
            ativos.extend(
                loads(
                    requests.get(
                        "https://vxinforma.vortx.com.br/WsSite/OperacoesListar.php",
                        params=params,
                    ).content.decode("utf-8-sig")
                )["data"]
            )
        return ativos

    @task()
    def filtra_ativos(ativos_excel, ativos_site):
        """
        Filtra os ativos do site com os ativos do Excel
        """
        return list(
            map(
                lambda x: (x["codigo"], x["codIf"]),
                filter(lambda x: x["codIf"] in ativos_excel, ativos_site),
            )
        )

    @task()
    def download_documents(ativos_filtrados):
        downloaded_files = []
        for ativo in ativos_filtrados:
            documentos = requests.get(
                f"https://apis.vortx.com.br/vxsite/api/operacao/{ativo[0]}/documentos-por-tipo"
            ).json()
            # Verifica se a pasta AGENTE/ATIVO existe
            folder_agente_ativo = path.join(PATH_DOWNLOAD, ativo[1])
            if not path.exists(folder_agente_ativo):
                mkdir(folder_agente_ativo)
            for tipo_doc in documentos:
                # Verifica se a pasta AGETE/ATIVO/TIPODOCUMENTO
                folder_agente_ativo_tipodoc = path.join(
                    folder_agente_ativo, re.sub("[\/]", "", tipo_doc["type"])
                )
                if not path.exists(folder_agente_ativo_tipodoc):
                    mkdir(folder_agente_ativo_tipodoc)
                for documento in tipo_doc["documents"]:
                    # Verifica se o arquivo existe
                    file_name = path.basename(documento["url"])
                    file_path = path.join(folder_agente_ativo_tipodoc, file_name)
                    if not path.exists(file_path):
                        # Download
                        with open(file_path, "wb") as file:
                            file.write(requests.get(documento["url"]).content)
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
            mailbox.send_mail(receiver="", subject="VORTX", body=email_body)

    ativos_excel = get_ifs_from_excel(PATH_EXCEL, "VORTX DTVM")
    ativos_site = get_ids_from_website()
    ativos_filtrados = filtra_ativos(ativos_excel, ativos_site)
    new_files = download_documents(ativos_filtrados)
    if new_files:
        send_new_files_email(prepare_email_body(new_files))


vortx()
