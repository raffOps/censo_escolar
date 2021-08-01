import json
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import (udf, col, expr)
from google.cloud import storage

spark = SparkSession.builder.appName("censo").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)


def add_prefix_in_columns(df, prefix):
    return df.select([col(column).alias(f"{prefix}_{column}") for column in df.columns])


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
    map_func = udf(lambda date: datetime.strptime(date, pattern)
    if type(date) == str
    else None, DateType())
    df = df.withColumn(column, map_func(col(column)))
    return df


def load_csv(file, bucket_prefix, year, region=None):
    schema = load_json(f"schemas/{file}_schema", bucket_prefix)
    schema = StructType.fromJson(schema)
    #     except:
    #         schema = StructType.fromJson(json.loads(schema))

    if file == "gestor" and int(year) < 2019:
        df = spark.createDataFrame(data=[], schema=schema)
    else:
        if file in ["matricula", "docentes"]:
            file = f"gs://{bucket_prefix}-landing/censo-escolar/{year}/{file}_{region}.csv"
        else:
            file = f"gs://{bucket_prefix}-landing/censo-escolar/{year}/{file}.csv"

        df = spark \
            .read \
            .options(header=True, delimiter="|", encoding="utf8") \
            .schema(schema=schema) \
            .csv(file)
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


def drop_columns(df, file):
    drops = []
    if file in ["turmas", "matricula", "gestor", "docentes"]:
        drops.extend(["NU_ANO_CENSO",
                      'TP_REGULAMENTACAO',
                      'CO_UF',
                      'IN_MANT_ESCOLA_PRIVADA_ONG',
                      'NU_ANO_CENSO',
                      'CO_MUNICIPIO',
                      'IN_CONVENIADA_PP',
                      'IN_ESPECIAL_EXCLUSIVA',
                      'TP_CATEGORIA_ESCOLA_PRIVADA',
                      'IN_MANT_ESCOLA_PRIVADA_OSCIP',
                      'IN_MANT_ESCOLA_PRIV_ONG_OSCIP',
                      'IN_MANT_ESCOLA_PRIVADA_S_FINS',
                      'IN_MANT_ESCOLA_PRIVADA_SIST_S',
                      'CO_DISTRITO',
                      'IN_EDUCACAO_INDIGENA',
                      'CO_MICRORREGIAO',
                      'TP_DEPENDENCIA',
                      'IN_EJA',
                      'IN_REGULAR',
                      'IN_PROFISSIONALIZANTE',
                      'TP_LOCALIZACAO_DIFERENCIADA',
                      'TP_CONVENIO_PODER_PUBLICO',
                      'TP_LOCALIZACAO',
                      'CO_REGIAO',
                      'CO_MESORREGIAO',
                      'IN_MANT_ESCOLA_PRIVADA_EMP',
                      'IN_MANT_ESCOLA_PRIVADA_SIND',
                      ]
                     )
    if file in ["matricula", "docentes"]:
        drops.extend(["CO_ENTIDADE"])

    if file == 'matricula':
        drops.extend(['NU_DIAS_ATIVIDADE',
                      'NU_DURACAO_TURMA'])

    if file != "turmas":
        drops.extend(["TP_MEDIACAO_DIDATICO_PEDAGO",
                      "TP_TIPO_ATENDIMENTO_TURMA",
                      "TP_TIPO_LOCAL_TURMA"])

    df = df.drop(*drops)

    return df


def transform(file, bucket, year, region=None):
    df = load_csv(file, bucket, year, region)
    df = drop_columns(df, file)
    df = transform_string_columns(df, bucket)
    df = transform_boolean_columns(df)
    df = transform_integer_columns(df)
    df = transform_date_columns(df, file, year)
    return df


def main(project="rjr-dados-abertos", year="2020"):
    regions = ["co", "nordeste", "norte", "sudeste", "sul"]
    partitions = ["E_NU_ANO_CENSO", "E_CO_UF"]

    print(project)
    print(year)

    # if args:
    #     project, year = *args
    # else:
    #     project = "rjr-dados-abertos"
    #     year = "2020"

    escolas = transform("escolas", project, year)
    escolas = add_prefix_in_columns(escolas, "E")
    turmas = transform("turmas", project, year)
    turmas = add_prefix_in_columns(turmas, "T")
    gestores = transform("gestor", project, year)
    gestores = add_prefix_in_columns(gestores, "G")
    for region in regions:
        print(region)
        docentes = transform("docentes", project, year, region)
        docentes = add_prefix_in_columns(docentes, "D")
        matriculas = transform("matricula", project, year, region)
        matriculas = add_prefix_in_columns(matriculas, "M")

        censo = escolas.join(turmas, escolas.E_CO_ENTIDADE == turmas.T_CO_ENTIDADE)
        censo = censo.join(gestores, censo.E_CO_ENTIDADE == gestores.G_CO_ENTIDADE)
        censo = censo.join(docentes, censo.T_ID_TURMA == docentes.D_ID_TURMA)
        censo = censo.join(matriculas, censo.T_ID_TURMA == matriculas.M_ID_TURMA)

        del(docentes)
        del(matriculas)
        censo = censo.drop("T_CO_ENTIDADE", "D_ID_TURMA", "M_ID_TURMA", "G_CO_ENTIDADE")

        censo \
            .write \
            .partitionBy(partitions) \
            .parquet(f"gs://{project}-processing/censo_escolar",
                     compression="snappy", mode="append")


if __name__ == "__main__":
    main(*sys.argv[1:])

