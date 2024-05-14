from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import os
from urllib import request
from typing import TypedDict
import pymongo
import traceback

EXTRACTION_FOLDER_PATH = '../work'

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
   work_dir = EXTRACTION_FOLDER_PATH
   destination_file = "votacao_candidato_munzona_2022.zip"
   url = 'https://cdn.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona/votacao_candidato_munzona_2022.zip'
   destination_zip_path = os.path.join(work_dir, destination_file)

   zip_already_downloaded = os.path.exists(destination_zip_path)

   if not zip_already_downloaded:
       download_zip_file(url, destination_zip_path)

   file_is_extracted = os.path.exists(csv_path)

   if not file_is_extracted:
        source_csv_filename = 'votacao_candidato_munzona_2022_BRASIL.csv'

        csv_extracted_path = os.path.join(work_dir, 'extracted')

        unzip_file(source_csv_filename, destination_zip_path, csv_extracted_path)


def download_zip_file(url: str, destination_path: str):
    request.urlretrieve(url, destination_path)


def unzip_file(file: str, source_path: str, extract_path):
    from zipfile import ZipFile

    with ZipFile(source_path, 'r') as zip_file:
        zip_file.extract(file, extract_path)


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
    database = options['database']
    collection = options['collection']

    if database == None and collection == None:
        connection_string = f"mongodb://{username}:{password}@{host}:{port}"
        return connection_string

    connection_string = f"mongodb://{username}:{password}@{host}:{port}/{database}.{collection}"

    return connection_string

