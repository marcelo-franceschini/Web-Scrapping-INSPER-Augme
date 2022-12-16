# IMPORTS AIRFLOW
from airflow.decorators import dag, task
import pendulum

# IMPORTS CÓDIGO
import requests
import re
from os import mkdir, path
from pandas import read_excel

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
    def create_folder_alert_download(ativos_filtrados):
        for ativo in ativos_filtrados:
            # Pega o código de Download do ativo
            download_id = requests.get(
                f"https://api.oliveiratrust.com.br/v1/titulos/{ativo[0]}"
            ).json()[0]["codigo_operacao"]
            documentos = requests.get(
                f"https://api.oliveiratrust.com.br/v1/titulos/{ativo[1].lower()}/downloads/{download_id}"
            ).json()
            if len(documentos) != 0:
                # Verifica se a pasta AGENTE/ATIVO existe
                folder_agente_ativo = path.join(PATH_DOWNLOAD, ativo[2])
                if not path.exists(folder_agente_ativo):
                    mkdir(folder_agente_ativo)
                for documento in documentos:
                    # Verifica se a pasta AGETE/ATIVO/TIPODOCUMENTO
                    folder_agente_ativo_tipodoc = path.join(
                        folder_agente_ativo, documento["subitem"].strip()
                    )
                    if not path.exists(folder_agente_ativo_tipodoc):
                        mkdir(folder_agente_ativo_tipodoc)
                    file_name = (
                        documento["descricao"] + "-" + path.basename(documento["link"])
                    )
                    file_path = path.join(
                        folder_agente_ativo_tipodoc, re.sub("[\/]", "", file_name)
                    )
                    # Verifica se o arquivo existe
                    if path.exists(file_path):
                        # Enviar e-mail
                        pass
                    else:
                        # Download
                        with open(file_path, "wb") as file:
                            file.write(
                                requests.get(
                                    documento["link"].replace(URL_TO_IGNORE, "")
                                ).content
                            )

    ativos_excel = get_ifs_from_excel(PATH_EXCEL, "OLIVEIRA TRUST DTVM ")
    ativos_site = get_ids_from_website()
    ativos_filtrados = filtra_ativos(ativos_excel, ativos_site)
    create_folder_alert_download(ativos_filtrados)


oliveira()
