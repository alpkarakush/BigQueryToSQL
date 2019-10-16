# BigQuery-to-SQL

All query information is saved in bigquery_config.json files and can be configured there.
Please procced through all steps for script to work properly:
1) pip install -r requirements.txt
2) install MySql server and write host, user, passwords in sql_config.json.
  If your OS is Ubuntu you can use [this](https://support.rackspace.com/how-to/installing-mysql-server-on-ubuntu/).
  Make sure to set your user's authentication method to "mysql_native_password". The default plugin is "auth_socket".
  CSV data insertion doesn't work for now.
3) Donwload google api credentials for your project
4) Create bucket in Cloud Storage
5) Set target_project_id according to your credentials and bucket_name according to step 4 in biqquery_config.json
6) Set target_dataset_id, target_table_id, target_column in biqquery_config.json how you wish


1) big_query_manager.py<br />
  a)cast_to_timestamp() used for casting "repository_creeated_at" to timestamp type for later querying purposes<br />
  b)export_table_to_storage() used for exporting table to Cloud Storage, if table bigger than 500mb it'll be chunked<br />
  c)create_query_table() query a table and save result in a new table<br />
  d)download_blob() Download directory from Cloud Storage<br />
  e)get_date_range() Query date interval and download results<br />
  f)create_sql_from_table() Create a "Create TABLE" script with table schema from "target_table_id"<br />

