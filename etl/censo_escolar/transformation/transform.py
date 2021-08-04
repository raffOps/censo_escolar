import json
import sys
from datetime import datetime
import logging
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import (udf, col)
from pyspark.sql import DataFrame
from google.cloud import storage

spark = SparkSession.builder.appName("censo").getOrCreate()
logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s",
                    level=logging.INFO,
                    datefmt="%y/%m/%d %H:%M:%S")


def load_json(name, bucket_prefix):
    bucket = storage.Client().get_bucket(f"{bucket_prefix}-scripts")
    blob = bucket.blob(f'censo_escolar/transformation/{name}.json')
    maps = json.loads(blob.download_as_string())
    return maps


def mapping(df, map_, column, type_return):
    map_func = udf(lambda key: map_.get(str(key)), type_return())
    df = df.withColumn(column, map_func(col(column)))
    return df


def string_to_date(df, column, year):
    if int(year) > 2014:
        pattern = '%d/%m/%Y'
    else:
        pattern = "%d%b%Y:%H:%M:%S"
    map_func = udf(lambda date: datetime.strptime(date, pattern) if type(date) == str
                                                                else None,
                                DateType())
    df = df.withColumn(column, map_func(col(column)))
    return df


def load_csv(file, bucket_prefix, year, region=None):
    schema = load_json(f"schemas/{file}_schema", bucket_prefix)
    schema = StructType.fromJson(schema)

    if file == "gestor" and int(year) < 2019:
        df = spark.createDataFrame(data=[], schema=schema)
    else:
        if file in ["matricula", "docentes"]:
            file = f"gs://{bucket_prefix}-landing/censo-escolar/{year}/{file}_{region}.csv"
        else:
            file = f"gs://{bucket_prefix}-landing/censo-escolar/{year}/{file}.csv"

        df = spark.read.options(header=True,
                                delimiter="|",
                                encoding="utf8").schema(schema=schema).csv(file)
    return df


def transform_string_columns(df, bucket):
    maps = load_json("maps", bucket)
    string_columns = [column for column in df.columns
                      if column.startswith("TP") or column.startswith("CO")]

    for column in string_columns:
        if column in maps:
            df = mapping(df, maps[column], column, StringType)

    return df


def transform_boolean_columns(df):
    boolean_columns = [column for column in df.columns
                       if column.startswith("IN")]

    mapping_bool = {
        "0": False,
        "1": True
    }

    for column in boolean_columns:
        df = mapping(df, mapping_bool, column, BooleanType)

    return df


def transform_integer_columns(df):
    integer_columns = [column for column in df.columns
                       if column.startswith("NU") or column.startswith("QT")]
    for column in integer_columns:
        df = df.withColumn(column, col(column).cast(IntegerType()))

    return df


def transform_date_columns(df, file, year):
    if file == "escolas":
        df = string_to_date(df, "DT_ANO_LETIVO_INICIO", year)
        df = string_to_date(df, "DT_ANO_LETIVO_TERMINO", year)

    return df


def transform(file, bucket, year, region=None):
    df = load_csv(file, bucket, year, region)
    df = transform_string_columns(df, bucket)
    df = transform_boolean_columns(df)
    df = transform_integer_columns(df)
    df = transform_date_columns(df, file, year)
    return df


def union(dfs):
    return reduce(DataFrame.unionAll, dfs)


def repartition(df, file):
    nrows = df.count()
    if file in ["matriculas", "docentes"]:
        num_partitions = nrows / 2_000_000
    else:
        num_partitions = 1

    return df.repartition(int(num_partitions))


def save(df, name, partitions, project):
    df.write.partitionBy(partitions).parquet(f"gs://{project}-processing/censo-escolar/{name}",
                                             compression="snappy")


def main(project="rjr-dados-abertos", year="2020"):
    regions = ["co", "nordeste", "norte", "sudeste", "sul"]
    partitions = ["NU_ANO_CENSO"]

    logging.info(f"{year} - escolas...")
    escolas = transform("escolas", project, year)
    escolas = repartition(escolas, "escolas")
    save(escolas, "escolas", partitions, project)
    del(escolas)

    logging.info(f"{year} - turmas...")
    turmas = transform("turmas", project, year)
    turmas = repartition(turmas, "turmas")
    save(turmas, "turmas", partitions, project)
    del(turmas)

    if int(year) > 2018:
        logging.info(f"{year} - gestores...")
        gestores = transform("gestor", project, year)
        gestores = repartition(gestores, "gestores")
        save(gestores, "gestores", partitions, project)
        del(gestores)

    logging.info(f"{year} - docentes...")
    docentes = []
    for region in regions:
        docentes.append(transform("docentes", project, year, region))
    docentes = union(docentes)
    docentes = repartition(docentes, "docentes")
    save(docentes, "docentes", partitions, project)
    del(docentes)

    logging.info(f"{year} - matriculas...")
    matriculas = []
    for region in regions:
        matriculas.append(transform("matricula", project, year, region))
    matriculas = union(matriculas)
    matriculas = repartition(matriculas, "matriculas")
    save(matriculas, "matriculas", partitions, project)
    del(matriculas)


if __name__ == "__main__":
    main(*sys.argv[1:])

