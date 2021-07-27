#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import (udf, col)

from google.cloud import storage

spark = SparkSession.builder.appName("censo").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)


# In[2]:


def load_json(name, bucket):
    bucket = storage.Client().get_bucket(bucket)
    blob = bucket.blob(f'aux_tables/{name}.json')
    maps = json.loads(blob.download_as_string())
    return maps


def mapping(df, map_, column, type_return):
    map_func = udf(lambda key: map_.get(str(key)), type_return())
    df = df.withColumn(column, map_func(col(column)))
    return df

def string_to_date(df, column, year):
    if year > 2014:
        pattern = '%d/%m/%Y'
    else:
        pattern = "%d%b%Y:%H:%M:%S"
    map_func =  udf (lambda date: datetime.strptime(date, pattern) 
                     if type(date) == str 
                     else None, DateType())
    df = df.withColumn(column, map_func(col(column)))
    return df

def load_csv(file, bucket, year, region=None):
    schema = load_json(f"{file}_schema", bucket)
    try:
        schema = StructType.fromJson(schema)
    except:
        schema = StructType.fromJson(json.loads(schema))

    if file == "matricula":
        file = f"gs://{bucket}/landing_zone/censo-escolar/{year}/matricula_{region}.csv"
    else:
        file = f"gs://{bucket}/landing_zone/censo-escolar/{year}/{file}.csv"

    df = spark             .read             .options(header=True, delimiter="|", encoding="utf8")             .schema(schema=schema)             .csv(file)
    return df

def transform_string_columns(df, file, bucket):
    maps = {**load_json(f"{file}_maps", bucket), **load_json("regioes_maps", bucket)}
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

def transform_date_columns(df, file):
    if file == "escolas":
        df = string_to_date(df, "DT_ANO_LETIVO_INICIO", YEAR)
        df = string_to_date(df, "DT_ANO_LETIVO_TERMINO", YEAR)
    return df

def drop_columns(df, file):
    if file == "matricula":
        drops = [
                'CO_CURSO_EDUC_PROFISSIONAL',
                 'TP_MEDIACAO_DIDATICO_PEDAGO',
                 'TP_UNIFICADA',
                 'TP_TIPO_ATENDIMENTO_TURMA',
                 'TP_TIPO_LOCAL_TURMA',
                 'CO_ENTIDADE',
                 'CO_DISTRITO',
                 'TP_DEPENDENCIA',
                 'TP_CATEGORIA_ESCOLA_PRIVADA',
                 'TP_CONVENIO_PODER_PUBLICO',
                 'TP_REGULAMENTACAO'
                ]
    elif file == "turmas":
        drops = [
                "CO_REGIAO",
                "CO_MESORREGIAO",
                "CO_MICRORREGIAO",
                "CO_UF",
                "CO_MUNICIPIO",
                "CO_DISTRITO",
                "TP_DEPENDENCIA",
                "TP_LOCALIZACAO",
                "TP_CATEGORIA_ESCOLA_PRIVADA",
                "TP_CONVENIO_PODER_PUBLICO",
                "TP_REGULAMENTACAO",
                "TP_LOCALIZACAO_DIFERENCIADA"
                ]
    else:
        drops = []
        
    df = df.drop(*drops)
    return df
        

def transform(file, bucket, year, region=None):
        df = load_csv(file, bucket, year, region)
        df = transform_string_columns(df, file, bucket)
        df = transform_boolean_columns(df)
        df = transform_integer_columns(df)
        df = transform_date_columns(df, bucket)
        df = drop_columns(df, file)
        return df


# In[4]:


#regions =  ["co", "nordeste", "norte", "sudeste", "sul"]
regions =  ["sul"]
if __name__ == "__main__":
    if sys.argv[4:]:
        bucket, year = sys.argv[4:]
    else:
        bucket = "rjr-portal-da-transparencia"
        year = "2020"
    escolas = transform("escolas", bucket, year)
    turmas =  transform("turmas", bucket, year)
    for region in regions:
        matriculas = transform("matricula", bucket, year, region)


# In[5]:


escolas_turmas = escolas.join(turmas, escolas.CO_ENTIDADE == turmas.CO_ENTIDADE)


# In[ ]:


escolas


# In[86]:


# if FILE == "matricula":
#     file =  f"{FILE}_{REGION}"
# else:
#     file = FILE
    
# df.write.parquet(f"gs://{BUCKET}/temp/censo-escolar/{YEAR}/{FILE}.parquet")  


# In[87]:


# a = spark \
#         .read \
#         .parquet(f"gs://{BUCKET}/temp/censo-escolar/{YEAR}/{FILE}.parquet")  


# In[ ]:




