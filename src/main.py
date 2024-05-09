from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def import_mongo():


    # client = MongoClient("34.28.35.28", 80)
    # client = MongoClient(uri)
    # db = client["elections_db"]
    # collection = db["brasil"]
    url = 'https://cdn.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona/votacao_candidato_munzona_2022.zip'
    work_dir = "./work/"
    destination_file = "votacao_candidato_munzona_2022.zip"

    from urllib import request
    import os

    # Create folder
    folder_is_created = os.path.exists(work_dir)

    if not folder_is_created:
      os.mkdir(work_dir)

    destination_zip_path = os.path.join(work_dir, destination_file)

    # Download the file from `url` and save it locally
    # request.urlretrieve(url, destination_zip_path)

    from zipfile import ZipFile
    import os

    source_csv_filename = 'votacao_candidato_munzona_2022_BRASIL.csv'

    with ZipFile(destination_zip_path, 'r') as zip_file:
        source_csv_directory_path = os.path.join(work_dir, 'extracted')
        # zip_file.extract(source_csv_filename, source_csv_directory_path)

    extracted_file = os.path.join(work_dir, 'extracted', source_csv_filename)

    host = "34.28.35.28"
    port = 80
    username = 'dba'
    password = 'pandas'
    database = 'elections'
    collection = 'general_elections_2022'

    uri = f"mongodb://{username}:{password}@{host}:{port}/{database}.{collection}"

    spark_session = SparkSession.builder.appName('spark').config("spark.mongodb.output.uri", uri).config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0").getOrCreate()

    df: DataFrame = spark_session.read.options(header="true", delimiter=";", encoding="ISO-8859-1", inferSchema=True).csv(extracted_file)

    df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()

    spark_session.stop()

import_mongo()