import os
import re
import sys
from glob import glob
from time import sleep
from zipfile import ZipFile, BadZipfile

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import patoolib
import requests
from google.cloud import storage

if os.getenv("CHUNCKSIZE"):
    CHUNCKSIZE = 100000

else:
    CHUNCKSIZE = 100000

if os.getenv("BUCKET"):
    BUCKET = os.getenv("BUCKET")
else:
    BUCKET = "rjr-teste"

# if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
#     CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
# else:
#     CREDENTIALS = "key.json"

CREDENTIALS = "key.json"


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
    os.remove(f"{year}.zip")

    recursives_zips = [file for file in glob(f"*{year}/DADOS/*")
                       if ".rar" in file or ".zip" in file]
    for recursive_zip_names in recursives_zips:
        patoolib.extract_archive(f"{recursive_zip_names}", outdir=f"{year}/DADOS")
        os.remove(f"{recursive_zip_names}")

    print("Unzip complete")

# def check_files():
#     hashes = open(os.listdir(".*MD5.*.txt")[0])
#     for file in os.listdir("*.csv"):
#         md5(file).hexdigest()


def convert_object_columns_to_str(df):
    object_columns = [column for column, dtype in
                      list(zip(df.dtypes.index,  df.dtypes))
                      if dtype == object]

    df[object_columns] = df[object_columns].astype(str)

    return df


def csv_to_parquet(year, compression):
    for file in glob(f"*{year}/DADOS/*.CSV"):
        parquet_name = f"{year}_" + re.search("DADOS\/(.*)\.", file).group(1) + ".parquet"
        pqwriter = None
        schema = None
        for i, df in enumerate(pd.read_csv(file, chunksize=CHUNCKSIZE, encoding="latin1", sep="|")):
            df = convert_object_columns_to_str(df)
            try:
                if schema:
                    table = pa.Table.from_pandas(df, schema=schema)
                else:
                    table = pa.Table.from_pandas(df)
                    schema = table.schema
                if i == 0:
                    pqwriter = pq.ParquetWriter(parquet_name, table.schema, compression=compression)
                pqwriter.write_table(table)
            except pa.ArrowTypeError as e:
                print("aqui")

        # close the parquet writer
        if pqwriter:
            pqwriter.close()
        print(f"{parquet_name} criado")


def upload_files(year):
    print("Uploading files")
    client = storage.Client.from_service_account_json(json_credentials_path=CREDENTIALS)
    bucket = client.get_bucket(BUCKET)
    for file in glob(f"*{year}/DADOS/*.CSV"):
        print(file)
        csv_name = re.search("DADOS\/(.*)\.", file).group(1) + ".csv"
        blob = bucket.blob(f"censo-escolar/{year}/{csv_name}")
        blob.upload_from_filename(file)

    print("Upload complete")


if __name__ == "__main__":
    year = sys.argv[1]
    compression = "snappy"
    #download_file(year)
    unzip_file(year)
    #csv_to_parquet(year, compression)
    #upload_files(year)


