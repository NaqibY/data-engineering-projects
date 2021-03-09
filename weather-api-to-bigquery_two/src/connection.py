import os
import configparser
import mysql.connector
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage

def get_config(config_path):

    config=configparser.RawConfigParser()
    config.read(config_path)

    return config

'''
+++++++++++++++++++++++++++++++++++++++++
+               database                +
+                                       +
+++++++++++++++++++++++++++++++++++++++++'''

class mysql_connection():
    """
    Established connection to database
    
    Arg:

        1. user
        2. host
        3. database
        4. password
        5. config_path= full path of config ini file
        6. config_section= name of section in config file, as default it set to 'mysql'
    """
    user=None
    host=None
    database=None
    password=None
    config_path=None

    conn=None

    def __init__(self, user, host, database, password, config_path, config_section='mysql'):

        self.user=user
        self.host=host
        self.database=database
        self.password=password
        self.config_path=config_path
        self.config_section=config_section

        config=get_config(config_path)

        conn=mysql.connector.connect(user=config.get(config_section,'user'),
                                    host=config.get(config_section,'host'),
                                    database=config.get(config_section,'database'),
                                    password=config.get(config_section,'password')
                                    )

        self.conn=conn

    def execute(self,query,params=None,autocommit=False):
        """
        Execute query

        Arg:
            1. query
            2. params=None
            3. autocommit=False
        """
        cur=self.conn.cursor()
        cur.execute(query,params)

        if autocommit== True:
            self.conn.commit()

    def close_conn(self):
        self.conn.close()

'''
+++++++++++++++++++++++++++++++++++++++++
+             Google Clouds             +
+                                       +
+++++++++++++++++++++++++++++++++++++++++'''

def gc_connection(config_path, config_section='gc'):
    """
    Established connection to google cloud
    
    Arg:

        1. config_path= full path of config ini file : [project, credential, location, default_query_job_config, client_info, client_option]
        2. config_section= name of section in config file, as default it set to 'gc'

    Return:
        object conn
    """
    # key=os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    # key='/home/naqib/Documents/commanding-day-304314-51cf61997c0c.json'
    config=get_config(config_path)
    credentials=service_account.Credentials.from_service_account_file(config.get(config_section,'credentials'))

    # credential=open(os.environ['GOOGLE_APPLICATION_CREDENTIALS'],'r')
    
    conn=bigquery.Client(project=config.get(config_section,'project'),
                        location=config.get(config_section,'location'),
                        credentials=credentials,
                        default_query_job_config=config.get(config_section,'default_query_job_config'),
                        client_info=config.get(config_section,'client_info'),
                        client_options=config.get(config_section,'client_option')

                            )
    return conn
'''
+++++++++++++++++++++++++++++++++++++++++
+               BigQuery                +
+                                       +
+++++++++++++++++++++++++++++++++++++++++'''

# class bigquery_connection():