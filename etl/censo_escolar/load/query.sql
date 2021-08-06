CREATE SCHEMA IF NOT EXISTS censo_escolar;

CREATE EXTERNAL TABLE IF NOT EXISTS censo_escolar.turmas 
WITH PARTITION COLUMNS (
    NU_ANO_CENSO INT
)
OPTIONS (
    format = "PARQUET",
    uris = ["gs://rjr-dados-abertos-processing/censo-escolar/turmas/*.parquet"],
    hive_partition_uri_prefix="gs://rjr-dados-abertos-processing/censo-escolar/turmas/"
);


CREATE EXTERNAL TABLE IF NOT EXISTS censo_escolar.escolas 
WITH PARTITION COLUMNS (
    NU_ANO_CENSO INT
)
OPTIONS (
    format = "PARQUET",
    uris = ["gs://rjr-dados-abertos-processing/censo-escolar/escolas/*.parquet"],
    hive_partition_uri_prefix="gs://rjr-dados-abertos-processing/censo-escolar/escolas/"
);


CREATE EXTERNAL TABLE IF NOT EXISTS censo_escolar.gestores 
WITH PARTITION COLUMNS (
    NU_ANO_CENSO INT
)
OPTIONS (
    format = "PARQUET",
    uris = ["gs://rjr-dados-abertos-processing/censo-escolar/gestores/*.parquet"],
    hive_partition_uri_prefix="gs://rjr-dados-abertos-processing/censo-escolar/gestores/"
);


CREATE EXTERNAL TABLE IF NOT EXISTS censo_escolar.docentes 
WITH PARTITION COLUMNS (
    NU_ANO_CENSO INT,
    CO_UF STRING
)
OPTIONS (
    format = "PARQUET",
    uris = ["gs://rjr-dados-abertos-processing/censo-escolar/docentes/*.parquet"],
    hive_partition_uri_prefix="gs://rjr-dados-abertos-processing/censo-escolar/docentes/"
);


CREATE EXTERNAL TABLE IF NOT EXISTS censo_escolar.matriculas 
WITH PARTITION COLUMNS (
    NU_ANO_CENSO INT,
    CO_UF STRING
)
OPTIONS (
    format = "PARQUET",
    uris = ["gs://rjr-dados-abertos-processing/censo-escolar/matriculas/*.parquet"],
    hive_partition_uri_prefix="gs://rjr-dados-abertos-processing/censo-escolar/matriculas/"
);


CREATE OR REPLACE TABLE censo_escolar2.matriculas_por_regiao_localizao
(
    Regiao STRING,
    Localizacao STRING,
    Matriculas INTEGER
) AS

SELECT CO_REGIAO, TP_LOCALIZACAO, COUNT(*) FROM `rjr-dados-abertos.censo_escolar2.escolas`
GROUP BY CO_REGIAO, TP_LOCALIZACAO;


CREATE OR REPLACE TABLE `rjr-dados-abertos.censo_escolar2.escolas_com_biblioteca`
(
    REGIAO STRING,
    Porcentagem FLOAT64
) AS
SELECT CO_REGIAO, ROUND((COUNTIF(IN_BIBLIOTECA)/count(*) * 100), 2), from `rjr-dados-abertos.censo_escolar2.escolas`
GROUP BY CO_REGIAO






