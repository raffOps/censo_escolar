import json
import sys
from datetime import datetime
import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import (udf, col, spark_partition_id)
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import rand

from google.cloud import storage

spark = SparkSession.builder.appName("censo").getOrCreate()
logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s",
                    level=logging.INFO,
                    datefmt="%y/%m/%d %H:%M:%S")


def add_prefix_in_columns(df, prefix):
    return df.select([col(column).alias(f"{prefix}_{column}")
                      for column in df.columns])


def load_json(name, bucket_prefix):
    bucket = storage.Client().get_bucket(f"{bucket_prefix}-scripts")
    blob = bucket.blob(f'censo_escolar/transform/{name}.json')
    maps = json.loads(blob.download_as_string())
    return maps


def mapping(df, map_, column, type_return):
    map_func = udf(lambda key: map_.get(key) if type(key) == str
                                else None,
                   type_return())
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


def join_columns(df, file, year):
    if int(year) < 2019 and file == "escolas":
        df = df.withColumn("IN_MANT_ESCOLA_PRIV_ONG_OSCIP",
                           col("IN_MANT_ESCOLA_PRIVADA_ONG") |
                           col("IN_MANT_ESCOLA_PRIVADA_OSCIP"))

        df = df.withColumn("IN_ESGOTO_FOSSA",
                           col("IN_ESGOTO_FOSSA_SEPTICA") |
                           col("IN_ESGOTO_FOSSA_COMUM"))

    df = df.drop("IN_MANT_ESCOLA_PRIVADA_ONG",
                  "IN_MANT_ESCOLA_PRIVADA_OSCIP",
                  "IN_ESGOTO_FOSSA_SEPTICA",
                  "IN_ESGOTO_FOSSA_COMUM")
    return df


def rename_columns(df, file, year):
    if int(year) < 2019 and file == "escolas":
        df = df.withColumn("IN_DORMITORIO_ALUNO", col("IN_ALOJAM_ALUNO"))
        df = df.withColumn("IN_DORMITORIO_PROFESSOR", col("IN_ALOJAM_PROFESSOR"))
        df = df.withColumn("CO_LINGUA_INDIGENA_1", col("CO_LINGUA_INDIGENA"))

    df = df.drop("IN_DORMITORIO_ALUNO", "IN_DORMITORIO_PROFESSOR", "CO_LINGUA_INDIGENA")

    return df


def transform(file, bucket, year, region=None):
    df = load_csv(file, bucket, year, region)
    df = transform_string_columns(df, bucket)
    df = transform_boolean_columns(df)
    df = transform_integer_columns(df)
    df = transform_date_columns(df, file, year)
    df = join_columns(df, file, year)
    df = rename_columns(df, file, year)
    return df


def union(dfs):
    return reduce(DataFrame.unionAll, dfs)


def get_partition_balanced(df, partition_by_columns, desired_rows_per_output_file=1_500_000):
    partition_count = df.groupBy(partition_by_columns).count()
    partition_balanced_data = (
        df
            .join(partition_count, on=partition_by_columns)
            .withColumn(
            'repartition_seed',
            (
                    rand() * partition_count['count'] / desired_rows_per_output_file
            ).cast('int')
        )
            .repartition(*partition_by_columns, 'repartition_seed')
    )
    partition_balanced_data = partition_balanced_data.drop("count", "repartition_seed")
    return partition_balanced_data


def save(df, name, partitions, project):
    df.write.partitionBy(partitions).parquet(f"gs://{project}-processing/censo-escolar/{name}",
                                             compression="snappy",
                                             mode="append")


def main(project="rjr-dados-abertos", year="2020"):
    regions = ["co", "nordeste", "norte", "sudeste", "sul"]

    partitions = ["NU_ANO_CENSO"]
    logging.info(f"{year} - escolas...")
    escolas = transform("escolas", project, year)
    escolas = escolas.repartition(1)
    save(escolas, "escolas", partitions, project)

    logging.info(f"{year} - turmas...")
    turmas = transform("turmas", project, year)
    turmas = turmas.repartition(1)
    save(turmas, "turmas", partitions, project)

    if int(year) > 2018:
        logging.info(f"{year} - gestores...")
        gestores = transform("gestor", project, year)
        gestores = gestores.repartition(1)
        save(gestores, "gestores", partitions, project)

    #partitions = ["NU_ANO_CENSO", "CO_UF"]

    logging.info(f"{year} - docentes...")
    docentes = []
    for region in regions:
        docentes.append(transform("docentes", project, year, region))
    docentes = union(docentes)
    docentes = get_partition_balanced(docentes, partitions)
    save(docentes, "docentes", partitions, project)

    logging.info(f"{year} - matriculas...")
    matriculas = []
    for region in regions:
        matriculas.append(transform("matricula", project, year, region))
    matriculas = union(matriculas)
    matriculas = get_partition_balanced(matriculas, partitions)
    save(matriculas, "matriculas", partitions, project)


if __name__ == "__main__":
    main(*sys.argv[1:])
