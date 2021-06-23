import os
import sys
import requests
from glob import glob
from datetime import datetime
from io import BytesIO
from time import sleep
from zipfile import ZipFile, BadZipfile
from hashlib import md5
import re
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import time

from google.cloud import storage

if os.getenv("CHUNCKSIZE"):
    CHUNCKSIZE = 100000

else:
    CHUNCKSIZE = 100000

if os.getenv("BUCKET"):
    BUCKET = os.getenv("BUCKET")
else:
    BUCKET = "rjr-teste1"

# DICT_URLS = {
#     "2020": "https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_2020.zip",
#     "2019": "https://download.inep.gov.br/microdados/microdados_educacao_basica_2019.zip",
#     "2018": "https://download.inep.gov.br/microdados/microdados_educacao_basica_2018.zip",
#     "2017": "https://download.inep.gov.br/microdados/micro_censo_escolar_2017.zip",
#     "2016": "https://download.inep.gov.br/microdados/micro_censo_escolar_2016.zip",
#     "2015": "https://download.inep.gov.br/microdados/micro_censo_escolar_2015.zip",
#     "2014": "https://download.inep.gov.br/microdados/micro_censo_escolar_2014.zip",
#     "2013": "https://download.inep.gov.br/microdados/micro_censo_escolar_2013.zip",
#     "2012": "https://download.inep.gov.br/microdados/micro_censo_escolar_2012.zip",
#     "2011": "https://download.inep.gov.br/microdados/micro_censo_escolar_2011.zip",
#     "2010": "https://download.inep.gov.br/microdados/micro_censo_escolar_2010.zip"
# }


def get_url(year):
    if year in "2019 2018":
        return f"https://download.inep.gov.br/microdados/microdados_educacao_basica_{year}.zip"
    else:
        return f"https://download.inep.gov.br/microdados/micro_censo_escolar_{year}.zip"


def download_file(year):
    url = get_url(year)
    print(f"Downloading {url}")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open("file.zip", 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk:
                f.write(chunk)
    print("Download complete")


def unzip_file(year):
    print(f"Unziping")
    with ZipFile(f"{year}.zip", 'r') as zip:
        zip.extractall()
    print("Unzip complete")

# def check_files():
#     hashes = open(os.listdir(".*MD5.*.txt")[0])
#     for file in os.listdir("*.csv"):
#         md5(file).hexdigest()


def csv_to_parquet(compression):
    start_time = time.time()
    for file in glob("./microdados_educacao_basica_2019/DADOS/*.CSV"):
        parquet_name = f"{compression}/" + re.search("DADOS\/(.*)\.", file).group(1) + ".parquet"
        pqwriter = None
        for i, df in enumerate(pd.read_csv(file, chunksize=CHUNCKSIZE)):
            table = pa.Table.from_pandas(df)
            # for the first chunk of records
            if i == 0:
                # create a parquet write object giving it an output file
                pqwriter = pq.ParquetWriter(parquet_name, table.schema, compression=compression)
            pqwriter.write_table(table)

        # close the parquet writer
        if pqwriter:
            pqwriter.close()
        print(f"{parquet_name} criado")

    print("\n\n")
    print(compression)
    print("--- %s seconds ---" % (time.time() - start_time))

# def upload_files():
#     for file in glob("./*.parquet"):
#         cl


if __name__ == "__main__":
    year = sys.argv[1]
    compression = "snappy"
    download_file(year)
    unzip_file(year)
    #csv_to_parquet(compression)


