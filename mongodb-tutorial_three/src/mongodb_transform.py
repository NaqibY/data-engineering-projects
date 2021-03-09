from numpy.lib.utils import source
import pandas as pd


def transform_inspection(city_inspection_file_path, zips_file_path):
    '''
    transformation function task 1, aggregate between city_inspection and zips where it has data that contains geojson
    and number of population on particular zip code
    
    Args:
        json_path_city_inspection = 'local/path/file.json'

        json_path_zips = 'local/path/file.json'
        
    return:
        list of normalised dataframe, [df_inspection ,df_address, df_zips]
    '''
    df_raw=pd.read_json(city_inspection_file_path)
    df_inspection=df_raw.copy()
    df_inspection['address_id']=df_inspection.index                                   ## create address_id using index
    df_inspection=df_inspection.drop(columns=['_id', 'address'])
    df_inspection['certificate_number']=df_inspection['certificate_number'].astype(str)
     
    nested_address=df_raw['address'].copy()                                            ## Normalise address columns from city_inspection
    df_address=pd.json_normalize(nested_address)            
    df_address=df_address.reset_index()
    df_address['address_id']=df_address.index
    df_address=df_address.reindex(columns=['address_id','city','zip','street','number'])        
    df_address[['zip','number']]=df_address[['zip','number']].replace([''],pd.NA)           # pandas will automatically convert '' to NaN for int instead of <NA>, so doit it manually
    df_address[['city','street']]=df_address[['city','street']].replace([''],[None])        # None for string columns
    df_address[['address_id','street']]=df_address[['address_id','street']].astype(str)     # after that convert it to str so that the none and <NA> remain the same
    df_address[['zip','number']]=df_address[['zip','number']].astype(str)                   # this 4 line of code is use to solve datatype errors when tries convert it to parquet
                                                                                            # since parquet cannot deals with mixtype value: int and string
    df_zips=pd.read_json(zips_file_path)
    df_loc=df_zips['loc'].copy()
    lon=[]
    lat=[]
    for i in df_loc:                                                                    ## normalise zips dataframe
        lon.append(i[0])
        lat.append(i[1])
    df_zips['lon']=lon
    df_zips['lat']=lat
    df_zips=df_zips.rename(columns={'_id':'zip'})
    df_zips=df_zips.drop(columns='loc')
    df_zips=df_zips.reindex(columns=['zip','city','state','pop','lat','lon'])
    df_zips['zip']=df_zips['zip'].astype(str)
    # df_zips[['lon','lat']]=df_zips[['lon','lat']].astype(float)
    df_normalised=[df_inspection, df_address, df_zips]
    return df_normalised

def transform_tripdata(tripdata_file_path):
    '''
    transformation function task 2, format schema for bigquery
    
    Args:
        tripdata_file_path = 'local/path/file.json'

    return:
        tripdata dataframe
    '''
    df_raw=pd.read_json(tripdata_file_path)
    def format_columns(df):                                                             ## Format columns name
        cols=df.columns
        cols=cols.str.lower()
        cols=cols.str.replace(' ','_', regex=True)
        return cols

    df_tripdata=df_raw.copy()
    df_tripdata.columns=format_columns(df_tripdata)
    df_tripdata['starttime']=pd.to_datetime(df_raw['starttime'])                        ## Change dataypes accordingly 
    df_tripdata['stoptime']=pd.to_datetime(df_raw['stoptime'])
    df_tripdata['start_station_id']=df_tripdata['start_station_id'].astype(str)
    df_tripdata['end_station_id']=df_tripdata['end_station_id'].astype(str)
    df_tripdata['bikeid']=df_tripdata['bikeid'].astype(str)
    df_tripdata['end_station_id']=df_tripdata['end_station_id'].astype(str)
    return df_tripdata

# import datetime
# date_now=datetime.datetime.now().date()
# source='/tmp/tripdata-{}.json'

# df=transform_tripdata(source.format(date_now))
# print(df)