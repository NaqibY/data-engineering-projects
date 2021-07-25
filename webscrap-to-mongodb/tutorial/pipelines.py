# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json
import pymongo
import copy
import re

class DataCleaningPipeline(object):

    def process_item(self, data, spider):
        dict_to_delete=[] 
        clean={}
        for lev1,lev2  in data.items():
            if lev1=='Brief':
                pass
            else:
                if type(lev2)==list:
                    clean[lev1]=lev2[0]
                else:
                    clean[lev1]=copy.deepcopy(lev2) # in order to avoid, RuntimeError: dictionary changed size during iteration
                                                    # use deepcopy(), where it makes copy of the dict and reference it with defaultdict 
                                                    # dict key will be out of loop if you alter for example key 'foo' suppose to be in 4th iteration 
                                                    # but when change it to 'bar' python does not know where it should be placed in dict.
                                                    # every changes to dict will be copied and append next to original dict 
                    for key, val in lev2.items():
                        new_key=key.replace('Sec.','Secondary').replace('Aux.', 'Auxilary').replace('Auxilary4', 'Auxilary 4').replace('Cam.','Camera').replace('Min.','Minimum').replace('Equiv.', 'Equivalent')
                        list_val=[]
                        if re.findall('\w..\.', key):
                            dict_to_delete.append(lev1+'/'+key)
                        if len(val) ==1:
                            new_val=val[0].replace('\n', '')
                            clean[lev1].update({new_key: new_val}) 
                        else:
                            for item in val:
                                if item.find('\n, '):
                                    x=item.replace('\n, ','').replace('\n','')
                                elif item.find('\n'):
                                    x=item.replace('\n','').replace('\n','')
                                list_val.append(x)
                            clean[lev1].update({new_key: list_val})
        # remove original dict that will be replace
        for i in dict_to_delete:
            cat=i.split('/')[0]
            key=i.split('/')[-1]
            clean[cat].pop(key)
        # return clean
        return self.mongodb(clean)

#disable this if you dont want to save data to mongodb 
    def mongodb(self, clean):
        client=pymongo.MongoClient('mongodb://localhost:27017/')
        db=client['test']
        db['huawei'].insert(clean)
        client.close()
        return clean


# enable this if you wnat to save data in json
# class JsonWriterPipeline(object): 
#    def __init__(self): 
#       self.file = open('items.json', 'w') 

#    def process_item(self, clean, spider): 
#     #   line = json.dumps(data)
#       json.dump(clean,self.file)
#       return clean
