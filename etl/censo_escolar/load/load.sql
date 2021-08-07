CREATE SCHEMA IF NOT EXISTS `{PROJECT}.censo_escolar`;

CREATE EXTERNAL TABLE IF NOT EXISTS censo_escolar.turmas 
WITH PARTITION COLUMNS (
    NU_ANO_CENSO INT
)
OPTIONS (
    format = "PARQUET",
    uris = ["gs://{PROJECT}-processing/censo-escolar/turmas/*.parquet"],
    hive_partition_uri_prefix="gs://{PROJECT}-processing/censo-escolar/turmas/"
);


CREATE EXTERNAL TABLE IF NOT EXISTS censo_escolar.escolas 
WITH PARTITION COLUMNS (
    NU_ANO_CENSO INT
)
OPTIONS (
    format = "PARQUET",
    uris = ["gs://{PROJECT}-processing/censo-escolar/escolas/*.parquet"],
    hive_partition_uri_prefix="gs://{PROJECT}-processing/censo-escolar/escolas/"
);


CREATE EXTERNAL TABLE IF NOT EXISTS censo_escolar.gestores 
WITH PARTITION COLUMNS (
    NU_ANO_CENSO INT
)
OPTIONS (
    format = "PARQUET",
    uris = ["gs://{PROJECT}-processing/censo-escolar/gestores/*.parquet"],
    hive_partition_uri_prefix="gs://{PROJECT}-processing/censo-escolar/gestores/"
);


CREATE EXTERNAL TABLE IF NOT EXISTS censo_escolar.docentes 
WITH PARTITION COLUMNS (
    NU_ANO_CENSO INT
)
OPTIONS (
    format = "PARQUET",
    uris = ["gs://{PROJECT}-processing/censo-escolar/docentes/*.parquet"],
    hive_partition_uri_prefix="gs://{PROJECT}-processing/censo-escolar/docentes/"
);


CREATE EXTERNAL TABLE IF NOT EXISTS censo_escolar.matriculas 
WITH PARTITION COLUMNS (
    NU_ANO_CENSO INT
)
OPTIONS (
    format = "PARQUET",
    uris = ["gs://{PROJECT}-processing/censo-escolar/matriculas/*.parquet"],
    hive_partition_uri_prefix="gs://{PROJECT}-processing/censo-escolar/matriculas/"
);
