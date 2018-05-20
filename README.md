# DatabaseManager
A Database manager for Mongodb written in python.

# How to use
  ```python
  import dataBaseManager
  from pymongo import MongoClient

    clients = dict(test = MongoClient('localhost', 27017)) # Support multi client
    db = 'test_db' # You can use list ['test_db', 'test_db2', ...]
    collection = 'test_collection' # You can use list ['test_collection', 'test_collection2', ...]
    data = dict(data='value') # You can use list [dict(data='value'), dict(data='value'), ...]

    dbm = dataBaseManager(clients=clients)
    # dbm = dataBaseManager() is also allowed
    # You can set clients later with
    # bdm.addClient(MongoClient, ClientName) # but it add one client at a time

    dbm.addDB(db) # Add a db on last added client
    # you can specify where to add it addDB(self, db, name=None)
    dbm.addCollection(collection) # Add a collection on last added client
    # you can specify where to add it addCollection(self, collection, db=None, name=None)
    dbm.addData(data) # Add a data on last added client
    # you can specify where to add it addData(self, data, collection=None, db=None, name=None)
    dbm.pushData() # Push last added data
    # you can specify what to push pushData(self, collection=None, db=None, name=None)
    dbm.pullData() # Pull data from server on last added collection
    # You can specify from where to pull pullData(self, collection=None, db=None, name=None)
    data = dbm.getCollectionData() # Get data from last added collections
    # you can specify what to get getCollectionData(self, collection=None, db=None, name=None)
    # it's also possible to change manualy last data with setFull(self, collection, db, name)
