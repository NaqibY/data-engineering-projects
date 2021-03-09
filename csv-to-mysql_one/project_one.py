import datetime
import json
import os

from src.connection import connection# import Module.data_handler
import src.files as files
import src.data as data

# this the main scrript that will orchestrate our etl
if __name__=='__main__':
    
    with open('setup/config.json', 'r') as json_file:
        config=json.load(json_file)

    # use mysql config 
    # add informaation in config file as such
        ''' "mysql": {
                        "USER":mysql_username,
                        "HOST":host_address,
                        "DATABASE": names_of_database,
                        "PASSWORD": mysql_password,
                        "download_url":"http://eforexcel.com/wp/wp-content/uploads/2017/07/50000-Sales-Records.zip",
                        "destination_folder":"/home/naqib/Documents/programming/Coursera/project/ETL Python/setup/data/"
        }'''

    db_connection=connection(USER=config['mysql']['USER'],                   # establish target database
                                HOST=config['mysql']['HOST'],
                                DATABASE=config['mysql']['DATABASE'],
                                PASSWORD=config['mysql']['PASSWORD'])

    files.download_data(download_url=config['mysql']['download_url'],                       # begin extraction
                            destination_folder=config['mysql']['destination_folder'])

    files.extract_file(download_url=config['mysql']['download_url'],
                            destination_folder=config['mysql']['destination_folder'])
           
    db_connection.init_query('''DROP TABLE IF EXISTS sales''')                  # before loading to our database, run this query to create new table
    db_connection.init_query(query=(
        '''CREATE TABLE sales(
            id MEDIUMINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            Region varchar(256),
            Country varchar(256),
            `Item Type` varchar(20),
            `Sales Channel` varchar(20),
            `Order Priority` varchar(20),
            `Order Date` DATE NOT NULL,
            `Order ID` int(20) NOT NULL,
            `Ship Date` DATE NOT NULL,
            `Units Sold` int(20) NOT NULL,
            `Unit Price` int(20) NOT NULL,
            `Unit Cost` int(20) NOT NULL,
            `Total Revenue` int(20) NOT NULL,
            `Total Cost` int(20) NOT NULL,
            `Total Profit` int(20) NOT NULL)'''))

    data.csv_load_to_db(filename='50000 Sales Records.csv'                                  # now import data module to perform transform and load
                        ,destination_folder=config['mysql']['destination_folder']
                        ,connection=db_connection) 

    db_connection.close()              # finish
    os.system('clear')

    print('ETL process is complete')

    # read logs file
