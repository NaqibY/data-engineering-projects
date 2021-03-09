import json
import requests
import datetime

# param={'lat': 3.139003,
#     'lon': 101.686852,
#     'unit': 'Metric',
#     'output': 'json',
#     'product': 'meteo'
#         }
def get_dataset(param):
    '''
    return:
            json_data.json
    '''
    date=datetime.datetime.now().date()
    response=requests.get("http://www.7timer.info/bin/api.pl?",params=param)
    json_data=response.json()

    if response.status_code == 200:
        with open('setup/data/raw/7timer_%s.json'%(date),'w') as json_file:
            json.dump(json_data, json_file,indent=2)

    
    else: print('something wrong with the api,{}'.format(response.status_code))
    
    return json_data

# print(get_dataset(param))