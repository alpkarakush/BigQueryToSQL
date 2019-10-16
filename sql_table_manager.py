import mysql.connector
import json
from mysql.connector.errors import Error
import os
import csv

class SqlManager:
    '''A simple Sql Manager for managing creating tables and inserting csv data into table'''

    def __init__(self, config_path=None):
        with open(os.path.abspath(config_path)) as json_file:
            config = json.load(json_file)

        self._host= config['host']
        self._user= config['user']
        self._password= config['password']
        self._db_name = config['database']
        try:
            mydb = mysql.connector.connect( host= self._host,
                                            user= self._user,
                                            passwd= self._password)
            print("Successfully connected to MySql server")
        except Error as err:
            print("Error happened during connection to mysql server")
            print(err)

    def create_database(self, db_name):
        mydb = mysql.connector.connect(
                                      host= self._host,
                                      user= self._user,
                                      passwd= self._password
                                    )
        mycursor = mydb.cursor()
        try:
            mycursor.execute("CREATE DATABASE "+self._db_name)
        except Error as err:
            print(err)
        mycursor.close()
        

    def create_table(self, create_query):
        mydb = mysql.connector.connect(
                                      host= self._host,
                                      user= self._user,
                                      passwd= self._password,
                                      database= self._db_name
                                    )
        mycursor = mydb.cursor()
        try:
            mycursor.execute(create_query)
        except Error as err:
            print(err)
        mycursor.close()

    def insert_csv_rows(self, data_dir, column_data_path, table_id):
        mydb = mysql.connector.connect(
                                      host= self._host,
                                      user= self._user,
                                      passwd= self._password,
                                      database= self._db_name
                                    )
        mycursor = mydb.cursor()

        with open(os.path.abspath(column_data_path), "r") as column_file:
            data = json.load(column_file)
            column_dict = json.loads(data)

        column_names = list(column_dict.keys())

        insert_str = 'INSERT INTO '+table_id+' ('+column_names[0]
        for key in column_names[1:]:
            insert_str +=', '+key
        insert_str += ') VALUES ( %s'
        for key in column_names[1:]:
            insert_str += ', %s'
        insert_str +=');'


        for file in os.listdir(os.path.abspath(data_dir)):
            with open(os.path.abspath(data_dir+"/"+file)) as csv_file:
                reader = csv.reader(csv_file)
                first_row = next(reader)

                for row in reader:
                    mycursor.execute(insert_str, row)
                mydb.commit()


