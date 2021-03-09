from itertools import islice

def extract_mongodb(client, dbs, coll, initial_id=None, extract_by_batch=None):
    """
    export data from mongodb to json.
    
    Arg:
        client = ``MongoClient()``.

        dbs = name of database.
        
        coll = name of collection.
      
        initial_id = document id in ``objectID`` or any unique keys, default ``None``

        extract_by_batch = ``int`` batch of rows , default ``None`` 
    Return:
        list_of_docs
    """ 
    with client:
        db=client[dbs]
        fetch_before=db[coll].find()
        fetch=db[coll].find()
        list_of_docs=[]
        count=0
        if initial_id is not None:                                          # determine which row to start 
            for doc in fetch_before:
                count+=1
                if initial_id == None:
                    count=0
                    break
                if initial_id == doc['_id']:
                    break

        if extract_by_batch is None and initial_id is None:
            for docs in fetch:
                docs['_id']=str(docs['_id'])
                list_of_docs.append(docs)   
            print('extract all')
        elif extract_by_batch is None and initial_id is not None:
            for docs in islice(fetch, count):
                docs['_id']=str(docs['_id'])
                list_of_docs.append(docs)   
            print('extract all start at {}'.format(count))
        elif extract_by_batch is not None and initial_id is None:
            for docs in islice(fetch, 0, count+extract_by_batch):
                docs['_id']=str(docs['_id'])
                list_of_docs.append(docs)   
            print('extract_by_batch {} at {}'.format(extract_by_batch, count))
        elif extract_by_batch is not None and initial_id is not None:
            for docs in islice(fetch, count, count+extract_by_batch):
                docs['_id']=str(docs['_id'])
                list_of_docs.append(docs)   
            print('extract_by_batch {} at {}'.format(extract_by_batch, count))
        print(len(list_of_docs),"'s rows from {} is being extract'".format(coll))
        del fetch_before, fetch
    return list_of_docs
