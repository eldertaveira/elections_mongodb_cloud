from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import os
from urllib import request
from typing import TypedDict

EXTRACTION_FOLDER_PATH = './work/'

def load_data_into_mongodb():
    csv_file_path = get_path_for_csv_file()
    check_if_extraction_folder_is_created(EXTRACTION_FOLDER_PATH)

    download_csv_if_not_exists(csv_file_path)

    host = "34.28.35.28"
    port = 80
    username = 'dba'
    password = 'pandas'
    database = 'elections'
    collection = 'general_elections_2022'

    uri = create_connection_string({
        'host': host,
        'port': port,
        'username': username,
        'password': password,
        'database': database,
        'collection': collection
    })

    spark_session = SparkSession.builder.appName('spark').config("spark.mongodb.output.uri", uri).config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0").getOrCreate()

    df: DataFrame = spark_session.read.options(header="true", delimiter=";", encoding="ISO-8859-1", inferSchema=True).csv(csv_file_path)

    df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()

    spark_session.stop()


def get_path_for_csv_file():
    source_csv_filename = 'votacao_candidato_munzona_2022_BRASIL.csv'
    extracted_file = os.path.join(EXTRACTION_FOLDER_PATH, 'extracted', source_csv_filename)

    return extracted_file


def check_if_extraction_folder_is_created(folder_path: str):
    folder_is_created = os.path.exists(folder_path)

    if not folder_is_created:
      os.mkdir(folder_path)


def download_csv_if_not_exists(csv_path: str):
   file_is_extracted = os.path.exists(csv_path)

   if not file_is_extracted:
        work_dir = EXTRACTION_FOLDER_PATH
        destination_file = "votacao_candidato_munzona_2022.zip"
        url = 'https://cdn.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona/votacao_candidato_munzona_2022.zip'
        destination_zip_path = os.path.join(work_dir, destination_file)

        download_zip_file(url, destination_zip_path)

        csv_extracted_path = os.path.join(work_dir, 'extracted')

        unzip_file(destination_zip_path, csv_extracted_path)


def download_zip_file(url: str, destination_path: str):
    request.urlretrieve(url, destination_path)


def unzip_file(source_path: str, extract_path):
    from zipfile import ZipFile

    with ZipFile(source_path, 'r') as zip_file:
        zip_file.extract(source_path, extract_path)


class ConnectionOptions(TypedDict):
    host: str
    port: int
    username: str
    password: str
    database: str
    collection: str

def create_connection_string(options: ConnectionOptions):
    username = options['username']
    password = options['password']
    host = options['host']
    port = options['port']
    database = options['database']
    collection = options['collection']

    connection_string = f"mongodb://{username}:{password}@{host}:{port}/{database}.{collection}"

    return connection_string


load_data_into_mongodb()
