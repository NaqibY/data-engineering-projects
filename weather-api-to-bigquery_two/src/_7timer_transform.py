import json
import pandas as pd
import datetime
import argparse

def json_data(json_data):
    """
    json data setup for data aggregate
    
    Arg:

        1. json_data= json object string

    Return:
        dict {FACT_weather : df_fact}
    """
    # if json_data is any:
    data=json_data

    # if json_path is str:
    #     json_file=open(json_path,'rb')
    #     data=json.load(json_file)
    # else: dt=datetime.datetime.now().date

    df_raw=pd.json_normalize(data)
    tim=pd.to_datetime(str(df_raw['init'][0]), format='%Y%m%d%H')

#dataframe from downloaded json

# dataseries table
    df_dataseries=pd.json_normalize(data,'dataseries')
    df_dataseries['time_interval']=list(map(lambda x : tim+pd.Timedelta(hours=x),df_dataseries['timepoint']))
    df_dataseries=df_dataseries.drop(columns=['rh_profile','wind_profile','timepoint'])
    df_dataseries=df_dataseries.set_index('time_interval')
    df_dataseries['wind10m.direction']=df_dataseries['wind10m.direction'].astype(int)


# relative humidity table
    df_rh=pd.json_normalize(data['dataseries'],'rh_profile', meta='timepoint')
    df_rh['time_interval']=list(map(lambda x : tim+pd.Timedelta(hours=x),df_rh['timepoint']))
    df_rh=df_rh.drop(columns='timepoint')
    df_rh=df_rh.set_index('time_interval')

    df_rh.columns=['rh.layer', 'rh']    
    df_rh=df_rh.replace('mb','', regex=True)
    df_rh['rh.layer']=df_rh['rh.layer'].astype(int)

# wind table
    df_wind=pd.json_normalize(data['dataseries'],'wind_profile', meta='timepoint')
    df_wind['time_interval']=list(map(lambda x : tim+pd.Timedelta(hours=x),df_wind['timepoint']))
    df_wind=df_wind.drop(columns='timepoint')
    df_wind=df_wind.set_index('time_interval')

    df_wind.columns=['wind.layer', 'wind.direction', 'wind.speed']
    df_wind=df_wind.replace('mb','', regex=True)
    df_wind['wind.layer']=df_wind['wind.layer'].astype(int)

    df_fact1=df_dataseries.merge(df_rh, left_index=True, right_on='time_interval')
    df_fact=df_fact1.merge(df_wind, left_index=True, right_on='time_interval')
    cols=['cloudcover', 'highcloud', 'midcloud', 'lowcloud', 'temp2m',
        'lifted_index', 'rh2m', 'rh.layer', 'rh', 'msl_pressure', 'prec_type', 'prec_amount',
        'wind10m.direction', 'wind10m.speed',
        'wind.layer', 'wind.direction', 'wind.speed',
        'snow_depth']
    cols_clean=['cloudcover', 'highcloud', 'midcloud', 'lowcloud', 'temp2m',
        'lifted_index', 'rh2m', 'rh_layer', 'rh', 'msl_pressure', 'prec_type', 'prec_amount',
        'wind10m_direction', 'wind10m_speed',
        'wind_layer', 'wind_direction', 'wind_speed',
        'snow_depth']
    df_fact=df_fact.reindex(columns=cols)
    df_fact.columns=cols_clean
    fact_list={'FACT_weather':df_fact}
    # df_fact=[df_dataseries, df_rh, df_wind]
    #return list of object
    return fact_list

#dataframe of references from website
def html_data():
    """
    html data setup for table aggregate
    
    Arg:

        1. html_file= dir of html file
        2. url= .html

    Return:
        dict of dataframes,

    df_dimension={'DIM_cloud':di_cloud, 'DIM_lifted_index':di_index, 
                  'DIM_prec_amount':di_prec, 'DIM_rh':di_rh, 
                  'DIM_snow_depth':di_snow }

        all dataframes are for dimensional table 
    """
    url='http://7timer.info/doc.php?lang=en'
    # html_data=pd.read_html('7Timer! - numerical weather forecast for anywhere over the world.html')[-4]
    html_data=pd.read_html(url)[-4]
    html_data.columns=html_data.iloc[0]
    html_data=html_data.drop(0,axis=0)
    html_data=html_data.reset_index(drop=True)

    # dimesional table
    # cloud
    di_cloud=html_data.iloc[0:9].copy()
    di_cloud[['min','max']]=di_cloud['Meaning'].str.split('-',expand=True)
    di_cloud=di_cloud.drop(columns=['Variable','Meaning'])
    # di_cloud[['Cloud Cover', 'High Cloud', 'Mid Cloud','Low Cloud']]=di_cloud['Value']
    di_cloud.columns=['cloud','min','max']
    di_cloud['min']=di_cloud['min'].str.strip('%')
    di_cloud['max']=di_cloud['max'].str.strip('%')
    di_cloud[['cloud','min','max']]=di_cloud[['cloud','min','max']].astype(int)

    # lifted index
    di_index=html_data[9:17].copy()
    di_index[['min','1','max']]=di_index['Meaning'].str.split(expand=True)
    di_index=di_index.drop(columns=['Variable','Meaning','1'])
    di_index=di_index.fillna(11)
    di_index.iloc[0,1:3]=[0,-7]
    di_index.iloc[-1,1]=11

    di_index.columns=['lifted_index','min','max']
    di_index[['lifted_index','min','max']]=di_index[['lifted_index','min','max']].astype(int)
    di_index=di_index.reset_index(drop=True)
    # di_index.set_index('lifted_index')

    # Relative Humidity
    di_rh=html_data[34:55].copy()
    di_rh[['min','max']]=di_rh['Meaning'].str.split('-',expand=True)
    di_rh=di_rh.drop(columns=['Variable','Meaning'])
    di_rh.columns=['rh','min','max']
    di_rh['min']=di_rh['min'].str.strip('%')
    di_rh['max']=di_rh['max'].str.strip('%')

    di_rh.iloc[-1,1]='99'
    di_rh=di_rh.fillna(100)

    di_rh[['rh','min','max']]=di_rh[['rh','min','max']].astype(int)
    di_rh=di_rh.reset_index(drop=True)

    # wind_speed
    di_wind=html_data[55:68].copy()
    di_wind['description']=di_wind['Meaning'].str.rsplit(expand=True,n=1).iloc[:,-1]
    di_wind[['Speed','description']]=di_wind['Meaning'].str.rsplit(expand=True,n=1)
    di_wind[['min','max']]=di_wind['Speed'].str.rsplit('-',expand=True)
    di_wind=di_wind.drop(columns=['Variable','Meaning','Speed'])

    di_wind['max']=di_wind['max'].str.strip('m/s')

    di_wind=di_wind.fillna(0.3)
    di_wind.iloc[0,2]=0
    di_wind.iloc[-1,2:4]=[55.9,55.9]

    di_wind.columns=['wind_speed','description','min','max']
    di_wind[['min','max']]=di_wind[['min','max']].astype(float)
    di_wind['wind_speed']=di_wind['wind_speed'].astype(int)
    di_wind['description']=di_wind['description'].str.strip('(+)')
    di_wind=di_wind.reset_index(drop=True)

    # Percipitation Amount
    di_prec=html_data[71:81].copy()
    di_prec[['min','max']]=di_prec['Meaning'].str.split('-',expand=True,)
    di_prec=di_prec.drop(columns=['Variable','Meaning'])
    di_prec['max']=di_prec['max'].str.strip('mm/hr')

    di_prec=di_prec.fillna(0)
    di_prec.iloc[0,1] = 0
    di_prec.iloc[-1,1:3]=[75,75]


    di_prec.columns=['prec_amount','min','max']
    di_prec[['min','max']]=di_prec[['min','max']].astype(float)
    di_prec['prec_amount']=di_prec['prec_amount'].astype(int)
    di_prec=di_prec.reset_index(drop=True)
    
    # snow_depth
    di_snow=html_data[81:91].copy()
    di_snow[['min','max']]=di_snow['Meaning'].str.split('-',expand=True,)
    di_snow=di_snow.drop(columns=['Variable','Meaning'])
    di_snow['max']=di_snow['max'].str.strip('cm')

    di_snow=di_snow.fillna(0)
    di_snow.iloc[0,1] = 0
    di_snow.iloc[-1,1:3]=[250,250]

    di_snow.columns=['snow_depth','min','max']
    di_snow[['min','max']]=di_snow[['min','max']].astype(float)
    di_snow['snow_depth']=di_snow['snow_depth'].astype(int)
    di_snow=di_snow.reset_index(drop=True)
    
    df_dimension={'DIM_cloud':di_cloud, 'DIM_lifted_index':di_index, 'DIM_rh':di_rh, 'DIM_wind_speed':di_wind, 'DIM_prec_amount':di_prec, 'DIM_snow_depth':di_snow }
    #return as list of dataframe
    return df_dimension

# df_dim=html_data()

# for name, obj in df_dim.items():
#     obj.to_parquet('setup/data/transform/_7timer/{}.parquet'.format(name), engine='pyarrow', compression='gzip')