# BigQuery-to-SQL

All query information is saved in bigquery_config.json files and can be configured there.
Please procced through all steps for script to work properly:<br/>

0) Set up virtual environment
1) pip install -r requirements.txt
2) install MySql server and write host, user, passwords in sql_config.json.
  If your OS is Ubuntu you can use [this](https://support.rackspace.com/how-to/installing-mysql-server-on-ubuntu/).
  Make sure to set your user's authentication method to "mysql_native_password". The default plugin is "auth_socket".
  CSV data insertion doesn't work for now.
3) Donwload google api credentials for your project
4) Create bucket in Cloud Storage
5) Set target_project_id according to your credentials and bucket_name according to step 4 in biqquery_config.json
6) Set target_dataset_id, target_table_id, target_column in biqquery_config.json how you wish


big_query_manager.py<br />
* cast_to_timestamp() used for casting "repository_creeated_at" to timestamp type for later querying purposes<br />
* export_table_to_storage() used for exporting table to Cloud Storage, if table bigger than 500mb it'll be chunked<br />
* create_query_table() query a table and save result in a new table<br />
* download_blob() Download directory from Cloud Storage<br />
* get_date_range() Query date interval and download results<br />
* create_sql_from_table() Create a "Create TABLE" script with table schema from "target_table_id"<br />


sql_table_manager.py:  A simple SQL manager suitable for this project's purposes only

TO-DO:<br />
* insert csv files into sql table
