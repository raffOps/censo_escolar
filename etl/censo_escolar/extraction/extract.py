import os
import re
import sys
from glob import glob
from time import sleep
from zipfile import ZipFile, BadZipfile
import subprocess

import requests
from google.cloud import storage

if os.getenv("BUCKET"):
    BUCKET = os.getenv("BUCKET")
else:
    BUCKET = "rjr-teste"

CREDENTIALS = "./key.json"


def get_url(year):
    if year == "2020":
        url = "https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_2020.zip"
    elif year in "2019 2018":
        url = f"https://download.inep.gov.br/microdados/microdados_educacao_basica_{year}.zip"
    else:
        url = f"https://download.inep.gov.br/microdados/micro_censo_escolar_{year}.zip"

    return url


def make_request(url):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(f"{year}.zip", 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def test_zip(year):
    ZipFile(f"{year}.zip", 'r')


def download_file(year):
    url = get_url(year)
    print(f"Downloading {url}")
    try:
        make_request(url)
        test_zip(year)
    except (requests.exceptions.ChunkedEncodingError, BadZipfile) as e:
        sleep(100)
        try:
            if f"{year}.zip" in os.listdir():
                os.remove(f"{year}.zip")
            make_request(url)
            test_zip(year)
        except Exception as e:
            raise Exception(f"Download error: {e}")
    except Exception as e:
        raise Exception(f"Download error: {e}")

    print("Download complete")


def unzip_file(year):
    print(f"Unziping")
    with ZipFile(f"{year}.zip", 'r') as zip:
        zip.extractall()

    recursives_zips = [file for file in glob(f"*{year}/DADOS/*")
                       if ".rar" in file or ".zip" in file]
    for recursive_zip_name in recursives_zips:
        subprocess.run(["unar", "-o", f"{year}/DADOS",  recursive_zip_name])
        os.remove(f"{recursive_zip_name}")

    print("Unzip complete")


def upload_files(year):
    print("Uploading files")
    client = storage.Client.from_service_account_json(json_credentials_path=CREDENTIALS)
    bucket = client.get_bucket(BUCKET)
    for file in glob(f"*{year}/DADOS/*.CSV"):
        csv_name = (re.search("DADOS\/(.*)\.", file).group(1) + ".csv").lower()
        print(f"Uploading: gs://{BUCKET}censo-escolar/{year}/{csv_name}")
        blob = bucket.blob(f"censo-escolar/{year}/{csv_name}")
        blob.upload_from_filename(file)

    print("Upload complete")


if __name__ == "__main__":
    year = sys.argv[1]

    download_file(year)
    unzip_file(year)
    upload_files(year)


