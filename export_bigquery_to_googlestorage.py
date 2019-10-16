#!/usr/bin/env python3

from big_query_manager import *
from sql_table_manager import SqlManager
from google.cloud import bigquery
from google.oauth2 import service_account
import json

if __name__ == "__main__":

    with open("./bigquery_config.json") as json_file:
            config = json.load(json_file)

    key_path = os.path.abspath("./google_api_credentials.json") 
    credentials = service_account.Credentials.from_service_account_file(key_path,
                                                                        scopes=["https://www.googleapis.com/auth/cloud-platform"])
    client = bigquery.Client(credentials=credentials,
                             project=credentials.project_id)

    #Since original table's repository_created_at columns is not of type timestampt it is not queryable
    #So it needs to be cast into timestamp type
    cast_to_timestamp(client=           client,
                      project_id=       config["project_id"],
                      dataset_id=       config["dataset_id"],
                      table_id=         config["table_id"],
                      target_project_id=config["target_project_id"],
                      target_dataset_id=config["target_dataset_id"],
                      target_table_id=  config["target_table_id"],
                      column=           config["column"],
                      target_column=    config["target_column"])

    #Sends query qith given date interval and downloads all files from cloud storage
    get_date_range(client=      client,
                   key_path=    key_path,
                   project_id=  config["target_project_id"],
                   dataset_id=  config["target_dataset_id"],
                   table_id=    config["target_table_id"],
                   bucket_name= config["bucket_name"],
                   from_date=   config["from_date"],
                   to_date=     config["to_date"])

    #If you want to quickly check this function u can comment above two functions and run script again
    create_table_str = create_sql_from_table(client=       client,
                                              dataset_id=   config["target_dataset_id"],
                                              table_id=    config["table_id"],
                                              dest_file=    config["sql_create_script"])

    sqlManager = SqlManager(config_path='./sql_config.json')
    sqlManager.create_database(db_name=config["target_table_id"])
    sqlManager.create_table(create_query=create_table_str)
    #To-do
    # sqlManager.insert_csv_rows(data_dir="./github_timeline_test_2009_01_01_2010_12_30", 
    #                            column_data_path="./column_data.json",
    #                            table_id="github_timeline")
                                        