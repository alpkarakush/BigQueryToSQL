# BigQuery-to-SQL

All query information is saved in bigquery_config.json files and can be configured there.
Please procced through all steps for script to work properly:
1) pip install -r requirements.txt
2) install MySql server and write host, user, passwords in sql_config.json
  a)If your OS is Ubuntu you can use [this](https://support.rackspace.com/how-to/installing-mysql-server-on-ubuntu/)
    Make sure to set your user's authentication method to "mysql_native_password". The default plugin is "auth_socket".
  b)CSV data insertion doesn't work for now.
3) Donwload google api credentials for your project
4) Create bucket in Cloud Storage
5) Set target_project_id according to your credentials and bucket_name according to step 4 in biqquery_config.json
6) Set target_dataset_id, target_table_id, target_column in biqquery_config.json how you wish
