from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import os
from urllib import request
from typing import TypedDict
import pymongo
import traceback

EXTRACTION_FOLDER_PATH = './work/'

def load_data_into_mongodb():
    username = "root"
    password = "example"
    host = "localhost"

    database_name = "elections"
    collection_name = "general_elections_2022"

    data_lake_connection_string = create_connection_string({
        'username': username,
        'password': password,
        'host': host,
        'port': 80,
        'collection': None,
        'database': None
    })

    data_warehouse_connection_string = create_connection_string({
        'username': username,
        'password': password,
        'host': host,
        'port': 443,
        'collection': None,
        'database': None
    })

    print(" 🛢️Criando Usuários e Collections...")

    create_mongodb_database_and_collection(data_lake_connection_string, database_name, collection_name)
    create_mongodb_database_and_collection(data_warehouse_connection_string, database_name, collection_name)

    create_user_owner_for_database(data_lake_connection_string, database_name, "dba", "pandas")
    create_user_owner_for_database(data_warehouse_connection_string, database_name, "dba", "pandas")

    print(" 📂 Criando pasta...")

    csv_file_path = get_path_for_csv_file()
    check_if_extraction_folder_is_created(EXTRACTION_FOLDER_PATH)

    print(" ⌛ Baixando arquivo .zip e extraindo .csv...")

    download_csv_if_not_exists(csv_file_path)

    host = "localhost"
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

    print("✍ 🖋️ Inserindo dados no MongoDB...")

    df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()

    print("✍ ✅ Dados escritos no MongoD!!")

    print(" 🧹 Limpando...")

    spark_session.stop()

def create_mongodb_database_and_collection(url: str, database_name: str, collection_name: str):
    try:
        client = pymongo.MongoClient(url)

        if database_name in client.list_database_names():
            return

        database = client[database_name]

        if collection_name in database.list_collection_names():
            return

        database[collection_name].insert_one({"example": 'example'})
        database[collection_name].delete_many({})

        if collection_name in database.list_collection_names():
            print(f"Database '{database_name}' and Collection '{collection_name}' created successfully.")
    except Exception:
        print(f"Failed to create database {database_name} or collection '{collection_name}'. Exception:")
        traceback.print_exc()

def create_user_owner_for_database(url: str, database_name: str, username: str, password: str):
    client = pymongo.MongoClient(url)
    database = client[database_name]

    usernames = []

    usernames_cursor = client.admin.system.users.find()

    for cursor in usernames_cursor:
        usernames.append(cursor['user'])

    if username in usernames:
        return

    database.command("createUser", username, pwd=password, roles=[{"role": "dbOwner", "db": database_name }])


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
    database: str | None
    collection: str | None

def create_connection_string(options: ConnectionOptions):
    username = options['username']
    password = options['password']
    host = options['host']
    port = options['port']
    database = options['collection']
    collection = options['collection']

    if database == None and collection == None:
        connection_string = f"mongodb://{username}:{password}@{host}:{port}"
        return connection_string

    connection_string = f"mongodb://{username}:{password}@{host}:{port}/{database}.{collection}"

    return connection_string


load_data_into_mongodb()
