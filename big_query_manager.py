from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime, timedelta
from google.api_core import exceptions
import os
import json

def validate_credentials():
    """ Check if authentication to BigQuery is possible """

    client = bigquery.Client()
    try:
        client.list_projects()
    except EnvironmentError:
        exit("""
    Error: Unable to access BigQuery, did you set the
    GOOGLE_APPLICATION_CREDENTIALS
    environment variable to the path to your JSON file, like so:
    export GOOGLE_APPLICATION_CREDENTIALS="~/MyProject-1234.json"
            """)

def cast_to_timestamp(client,
                      project_id,
                      dataset_id,
                      table_id,
                      target_project_id,
                      target_dataset_id,
                      target_table_id,
                      column,
                      target_column):
    '''Cast a column to a timestamp type in a new column named target_column and save it in target table'''

    try:
        #check if target_table already exists from previous calls
        dataset_ref = client.dataset(target_dataset_id, project= target_project_id)
        table_ref = dataset_ref.table(target_table_id)
        #If table return, then table already exists
        table = client.get_table(table_ref)
        print('Table {} already exists, no need to cast to timestamp type'.format(table_ref.path))
        return
    except exceptions.NotFound:
        pass

    QUERY = """
            SELECT *,TIMESTAMP(%(column)s) AS %(target_column)s 
            FROM `%(project_id)s.%(dataset_id)s.%(table_id)s`
            """ % {'project_id':project_id, 
                    'dataset_id': dataset_id, 
                    'table_id': table_id,
                    'column': column,
                    'target_column': target_column
                  }
    #A new table, that will have query's result
    target_table_ref = client.dataset(target_dataset_id).table(target_table_id)
    
    job_config = bigquery.QueryJobConfig()
    job_config.destination = target_table_ref
    query_job = client.query(QUERY,
                            location='US',
                            job_config=job_config
                            )
    query_job.result() # Waits for job to complete.

    print('Table {}\'s column is cast to timestamp and saved in client\'s folder'.format(table_id))

def export_table_to_storage(client, 
                            destination_uri,
                            table_ref):
    '''Save table to a Cloud Storage with given destination_uri address'''
    extract_job = client.extract_table(
                    table_ref,
                    destination_uri,
                    location="US",
                    )
    extract_job.result()  # Waits for job to complete.
    print("Exported {}to {}".format(table_ref.path, destination_uri))

def create_query_table( client,
                        sql_query,
                        table_ref):
    '''Query a table and save result in table_ref'''
    job_config = bigquery.QueryJobConfig()
    
    job_config.destination = table_ref
    job_config.create_disposition = "CREATE_IF_NEEDED"
    job_config.write_disposition = "WRITE_EMPTY"
    
    query_job = client.query(
                        sql_query,
                        location='US',
                        job_config=job_config)
    
    query_job.result()
    if query_job.error_result:
        print("Error has occured during export of tables from GCP to Google Storage")
        print(query_job.error_result)

    print("Query results loaded to table {}".format(table_ref.path))    

def download_blob(  bucket_name,
                    prefix,
                    destination_dir,
                    key_path):
    """Gets list of all blobs from bucket and downloads them to destination directory"""
    
    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)

    storage_client = storage.Client.from_service_account_json(key_path)
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)  # Get list of files

    for blob in blobs:
        filename = blob.name.split('/')[-1] #.replace('/', '_') 
        blob.download_to_filename('/'.join(['.',destination_dir,'']) + filename)  # Download

        print('Blob {} downloaded to {}.'.format(filename, destination_dir))

def get_date_range( client,
                    key_path,
                    project_id,
                    dataset_id,
                    table_id,
                    from_date,
                    to_date,
                    bucket_name):
    '''Send query to a table to select rows for given interval (from_date,to_date) and saves them in cloud storage'''

    #date_range_str is used to identify queries of different date intervals,
    #such as directory "table_id_from_date_to_date" will be created for each distinct date interval
    date_range_str = "_".join([table_id,
                        from_date.replace('-','_'),
                        to_date.replace('-','_') ]
                        )
    #gs://bucket_name/data_range_str/table_id_[00000].csv
    destination_uri= '/'.join(['gs://'+bucket_name,date_range_str, str(table_id+'_*.csv' )])

    QUERY = """
            SELECT *
            FROM `%(projectName)s.%(datasetId)s.%(tableId)s`
            WHERE DATE(repository_created_at_timestamp) BETWEEN DATE(%(fromDate)s) 
            AND
               DATE(%(toDate)s)
            """ % {'projectName':project_id, 
                    'datasetId': dataset_id, 
                    'tableId': table_id,
                    'fromDate': '\"'+from_date+'\"',
                    'toDate': '\"'+to_date+'\"' 
                    }

    #create table ref for a result of query
    table_ref = client.dataset(dataset_id).table(date_range_str)

    try:
        #check if table_ref already exists from previous calls
        table = client.get_table(table_ref)
        print('Table {} already exists, proceeding to export table to storage'.format(table_ref))
    except exceptions.NotFound:
        create_query_table(client= client,
                            sql_query= QUERY,
                            table_ref= table_ref)
        
    export_table_to_storage(   client= client,
                    destination_uri= destination_uri,
                    table_ref= table_ref)

    download_blob(  bucket_name= bucket_name,
                    prefix= date_range_str+'/',
                    destination_dir= date_range_str,
                    key_path=key_path)

    return table_ref

def create_sql_from_table(client, dataset_id, table_ref, dest_dir, dest_file):
    '''Queries BigQuery selecting (column_name, datatype), then builds "Create TABLE" script with table schema
        Used for creating table in MySql DB, from BigQuery schema
    '''
    bigquery_to_sql= {'STRING':'VARCHAR(255)',
                    'BOOLEAN':'BOOLEAN',
                    'INTEGER':'INT',
                    'TIMESTAMP':'TIMESTAMP'}

    client = bigquery.Client()
    table = client.get_table(table_ref)

    column_dict = {}
    create_table_query = 'CREATE TABLE '+table_ref.table_id+' (id INT AUTO_INCREMENT PRIMARY KEY'
    for field in table.schema:
        create_table_query += ', '+field.name + ' ' + bigquery_to_sql[field.field_type]
        column_dict[field.name] = field.field_type
    create_table_query += ');'

    if not os.path.isdir(os.path.abspath(dest_dir)):
        try:
            os.mkdir(os.path.abspath(dest_dir))
        except OSError as error: 
            print(error)

    with open(os.path.abspath(dest_dir+'/'+dest_file),"w+") as file:
        # file.write(create_table_query)
        json.dump(json.dumps(column_dict), file)

    return create_table_query