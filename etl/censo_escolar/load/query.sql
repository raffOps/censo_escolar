CREATE SCHEMA IF NOT EXISTS censo_escolar;

CREATE EXTERNAL TABLE IF NOT EXISTS censo_escolar.turmas OPTIONS (
    format = "PARQUET",
    uris = ["gs://rjr-dados-abertos-processing/censo-escolar/turmas/*.parquet"],
    hive_partition_uri_prefix="gs://rjr-dados-abertos-processing/censo-escolar/turmas"
);


CREATE EXTERNAL TABLE IF NOT EXISTS censo_escolar.escolas OPTIONS (
    format = "PARQUET",
    uris = ["gs://rjr-dados-abertos-processing/censo-escolar/escolas/*.parquet"],
    hive_partition_uri_prefix="gs://rjr-dados-abertos-processing/censo-escolar/escolas"
);

CREATE EXTERNAL TABLE IF NOT EXISTS censo_escolar.gestores OPTIONS (
    format = "PARQUET",
    uris = ["gs://rjr-dados-abertos-processing/censo-escolar/gestores/*.parquet"],
    hive_partition_uri_prefix="gs://rjr-dados-abertos-processing/censo-escolar/gestores"
);


CREATE EXTERNAL TABLE IF NOT EXISTS censo_escolar.docentes OPTIONS (
    format = "PARQUET",
    uris = ["gs://rjr-dados-abertos-processing/censo-escolar/docentes/*.parquet"],
    hive_partition_uri_prefix="gs://rjr-dados-abertos-processing/censo-escolar/docentes"
);

CREATE EXTERNAL TABLE IF NOT EXISTS censo_escolar.matriculas OPTIONS (
    format = "PARQUET",
    uris = ["gs://rjr-dados-abertos-processing/censo-escolar/matriculas/*.parquet"],
    hive_partition_uri_prefix="gs://rjr-dados-abertos-processing/censo-escolar/matriculas"
);





