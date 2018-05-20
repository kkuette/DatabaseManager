from pymongo import MongoClient
from copy import deepcopy
import warnings

class dataBaseManager(object):

    def __init__(self, clients=None):
        '''
        args:
            Clients : dict filled with name of client and MongoClient
                      dict(name = client)
        '''

        self.clients = dict()

        # Init default values
        self.is_running = False
        self.last_client = None
        self.last_db = None
        self.last_collection = None

        if clients:
            for name, client in clients.items():
                self.addClient(client, name)

    def setFull(self, collection, db, name):
        self.setClient(name)
        self.setDB(db)
        self.setCollection(collection)

    def setClient(self, name):
        dbmErrorHandler.ClientNameError(name)
        self.last_client = name

    def setDB(self, db):
        dbmErrorHandler.DataBaseError(db)
        self.last_db = db

    def setCollection(self, collection):
        dbmErrorHandler.CollectionError(collection)
        self.last_collection = collection

    def addClient(self, client, name):
        '''
        Add a new client
            args:
                client : MongoClient object
                name : Name of client as str
        '''

        dbmErrorHandler.ClientNameError(name)
        dbmErrorHandler.ClientError(client)

        self.last_client = name
        new_client = dict(
            client = client,
            databases = dict()
        )
        self.clients[name] = new_client

    def addDB(self, db, name=None):
        '''
        Add a database to client
            args:
                db: Database name as str or list of str
                name: Client name as str, default last added client
        '''

        if not name:
            name = self.last_client

        dbmErrorHandler.ClientNameError(name)

        if isinstance(db, str):
            self.clients[name]['databases'][db] = dict()
            self.last_db = db
        elif isinstance(db, list):
            for item in db:
                dbmErrorHandler.DataBaseError(item)
                self.clients[name]['databases'][item] = dict()
                self.last_db = item
        else:
            dbmErrorHandler.DataBaseError(db)


    def addCollection(self, collection, db=None, name=None):
        '''
        Add a collection to the db from client
            args:
                collection: Collection name as str or list of str
                db: Database name as str, default last added database
                name: client name as str, default last added client
        '''

        if not db:
            db = self.last_db
        if not name:
            name = self.last_client

        dbmErrorHandler.DataBaseError(db)
        dbmErrorHandler.ClientNameError(name)

        if isinstance(collection, str):
            self.clients[name]['databases'][db][collection] = dict(
                id = 0,
                current_id = 0,
                data = []
            )
            self.last_collection = collection
        elif isinstance(collection, list):
            for item in collection:
                dbmErrorHandler.CollectionError(item)
                self.clients[name]['databases'][db][item] = dict(
                    id = 0,
                    current_id = 0,
                    data = []
                )
                self.last_collection = item
        else:
            dbmErrorHandler.CollectionError(collection)

    def addData(self, data, collection=None, db=None, name=None):
        '''
        Add data to collection in the db from client
            args:
                data: data as dict or list of dict
                collection: Collection name as str, default last added collection
                db: Database name as str, default last added database
                name: client name as str, default last added client
        '''

        if not name:
            name = self.last_client
        if not db:
            db = self.last_db
        if not collection:
            collection = self.last_collection

        dbmErrorHandler.CollectionError(collection)
        dbmErrorHandler.DataBaseError(db)
        dbmErrorHandler.ClientNameError(name)

        if self.clients[name]['databases'][db][collection]['id'] == 0:
            self.clients[name]['databases'][db][collection]['id'] = \
                self.getGivenCollection().count() - 1

        if isinstance(data, dict):
            self.clients[name]['databases'][db][collection]['id'] += 1
            data['id'] = self.clients[name]['databases'][db][collection]['id']
            self.clients[name]['databases'][db][collection]['data'].append(data)

        elif isinstance(data, list):
            for item in data:
                dbmErrorHandler.DataError(item)
                self.clients[name]['databases'][db][collection]['id'] += 1
                item['id'] = self.clients[name]['databases'][db][collection]['id']
                self.clients[name]['databases'][db][collection]['data'].append(item)
        else:
            dbmErrorHandler.DataError(data)

    def getClients(self):
        ''' Get all clients '''
        return self.clients

    def getCurrentClient(self):
        ''' Get current client dict '''
        return self.clients[self.last_client]

    def getCurrentDataBase(self):
        ''' Get current database dict '''
        return self.clients[self.last_client]['databases'][self.last_db]

    def getCurrentCollection(self):
        ''' Get current collection dict '''
        return self.clients[self.last_client]['databases'][self.last_db][self.last_collection]

    def getGivenClient(self, name=None):
        ''' Get Client() '''

        if not name:
            name = self.last_client
        dbmErrorHandler.ClientNameError(name)
        return self.clients[name]['client']

    def getGivenDB(self, db=None, name=None):
        ''' Get Client()[database] '''

        if not db:
            db = self.last_db
        dbmErrorHandler.DataBaseError(db)
        return self.getGivenClient(name)[db]

    def getGivenCollection(self, collection=None, db=None, name=None):
        ''' Get Client()[database][collection] '''

        if not collection:
            collection = self.last_collection
        dbmErrorHandler.CollectionError(collection)
        return self.getGivenDB(db, name)[collection]

    def getLastClient(self):
        return deepcopy(self.last_client)

    def getLastDB(self):
        return deepcopy(self.last_db)

    def getLastCollection(self):
        return deepcopy(self.last_collection)

    def getClientsNames(self):
        '''
        Get all clients name
            return:
                list of clients
        '''

        _clients = []
        for name, client in self.clients.items():
            _clients.append(name)
        return _clients

    def getDBs(self, name=None):
        '''
        Get all databases if no client name is specified,
        else return all databases from a given client name
            args:
                name: client name as str, default None
            return:
                dict(
                    clientname: list of databases
                )
        '''
        if name:
            name = self.last_name

        dbmErrorHandler.ClientNameError(name)

        _db = dict()
        if name:
            _db[name] = []
            for db, value in self.clients[name]['databases'].items():
                _db[name].append(db)
        else:
            for name, client in self.clients.items():
                _db[name] = []
                for db, value in self.clients[name]['databases'].items():
                    _db[name].append(db)
        return _db

    def getCollections(self, db=None, name=None):
        '''
        Get all collections in database in all clients if no client is specified,
        else if a database is specified, get all collections in a given database from all clients,
        else if a client name is specified get all collections in databases from a given client,
        else database and client name are specified get all collections in a given database from a given client.
            args:
                db: Database name as str, default None
                name: client name as str, default None
            return:
                dict(
                    clientname: dict(
                        database: list of collections
                    )
                )
        '''

        if not name:
            name = self.last_client
        if not db:
            db = self.last_db

        dbmErrorHandler.ClientNameError(name)
        dbmErrorHandler.DataBaseError(db)

        _collections = dict()
        if db and name:
            _collections[name] = dict()
            _collections[name][db] = []
            for collections, values in self.clients[name]['databases'][db].items():
                _collections.append(collections)
        elif db:
            for name, client in self.clients.items():
                _collections[name] = dict()
                for _db, value in self.clients[name]['databases'].items():
                    if _db == db:
                        _collections[name][_db] = []
                        for collections, values in self.clients[name]['databases'][_db].items():
                            _collections[name][_db].append(collections)
        elif name:
            _collections[name] = dict()
            for _db, value in self.clients[name]['databases'].items():
                _collections[name][_db] = []
                for collections, values in self.clients[name]['databases'][_db].items():
                    _collections[name][_db].append(collections)
        else:
            for name, client in self.clients.items():
                _collections[name] = dict()
                for _db, value in self.clients[name]['databases'].items():
                    _collections[name][_db] = []
                    for collections, values in self.clients[name]['databases'][_db].items():
                        _collections[name][_db].append(collections)
        return _collections

    def getCollectionData(self, collection=None, db=None, name=None):
        '''
        Get data from a given collection.
            args:
                collection: Collection name as str, default None
                db: Database name as str, default None
                name: Client name as str, default None
            return:
                list of dict
        '''
        if not name:
            name = self.last_client
        if not db:
            db = self.last_db
        if not collection:
            collection = self.last_collection

        dbmErrorHandler.CollectionError(collection)
        dbmErrorHandler.DataBaseError(db)
        dbmErrorHandler.ClientNameError(name)

        return deepcopy(self.clients[name]['databases'][db][collection]['data'])

    def pushOneData(self, data, collection=None, db=None, name=None):
        if not name:
            name = self.last_client
        if not db:
            db = self.last_db
        if not collection:
            collection = self.last_collection

        dbmErrorHandler.CollectionError(collection)
        dbmErrorHandler.DataBaseError(db)
        dbmErrorHandler.ClientNameError(name)
        dbmErrorHandler.DataError(data)
        dbmErrorHandler.DataPushWarning(data, name+"/"+db+"/"+collection)

        self.getGivenCollection(collection, db, name).insert_one(data)
        self.clients[name]['databases'][db][collection]['current_id'] += 1

    def pushData(self, collection=None, db=None, name=None):
        if not name:
            name = self.last_client
        if not db:
            db = self.last_db
        if not collection:
            collection = self.last_collection

        dbmErrorHandler.CollectionError(collection)
        dbmErrorHandler.DataBaseError(db)
        dbmErrorHandler.ClientNameError(name)

        data = self.getCollectionData(collection, db, name)
        c_id = self.clients[name]['databases'][db][collection]['current_id']
        dbmErrorHandler.DataPushWarning(data[c_id:], name+"/"+db+"/"+collection)
        id = self.clients[name]['databases'][db][collection]['id']
        if data:
            if c_id < id:
                self.getGivenCollection(collection, db, name).insert_many(data[c_id:])
                self.clients[name]['databases'][db][collection]['current_id'] = id

    def pullOneData(self, collection=None, db=None, name=None):
        if not name:
            name = self.last_client
        if not db:
            db = self.last_db
        if not collection:
            collection = self.last_collection

        dbmErrorHandler.CollectionError(collection)
        dbmErrorHandler.DataBaseError(db)
        dbmErrorHandler.ClientNameError(name)

        data_len = self.getGivenCollection().count() - 1
        id = self.clients[name]['databases'][db][collection]['id']

        if data_len < 0 or id == data_len:
            dbmErrorHandler.DataPullWarning(name+"/"+db+"/"+collection)
        else:
            data = self.getGivenCollection(collection, db, name).find({"id":{'$gte':id, '%lte':id+1}})
            self.clients[name]['databases'][db][collection]['id'] = data_len
            for item in data:
                del item['_id']
                self.clients[name]['databases'][db][collection]['data'].append(item)

    def pullData(self, collection=None, db=None, name=None):
        if not name:
            name = self.last_client
        if not db:
            db = self.last_db
        if not collection:
            collection = self.last_collection

        dbmErrorHandler.CollectionError(collection)
        dbmErrorHandler.DataBaseError(db)
        dbmErrorHandler.ClientNameError(name)

        data_len = self.getGivenCollection().count() - 1
        id = self.clients[name]['databases'][db][collection]['id']

        if data_len < 0 or id == data_len:
            dbmErrorHandler.DataPullWarning(name+"/"+db+"/"+collection)
        else:
            data = self.getGivenCollection(collection, db, name).find({"id":{'$gte':id}})
            self.clients[name]['databases'][db][collection]['id'] = data_len
            for item in data:
                del item['_id']
                self.clients[name]['databases'][db][collection]['data'].append(item)

class dbmErrorHandler(object):
    def custom_warning(msg, *args, **kwargs):
        return  '\033[91m' + "Warning : " + str(msg) + '\033[0m' + '\n'

    def CollectionError(value):
        if not isinstance(value, str):
            raise TypeError("%s in not a valid type for collection it should be str" % type(value))

    def DataBaseError(value):
        if not isinstance(value, str):
            raise TypeError("%s in not a valid type for database it should be str" % type(value))

    def ClientError(value):
        if not isinstance(value, MongoClient):
            raise TypeError("%s in not a valid type for client it should be MongoClient" % type(value))

    def ClientNameError(value):
        if not isinstance(value, str):
            raise TypeError("%s in not a valid type for client name it should be str" % type(value))

    def DataError(value):
        if not isinstance(value, dict):
            raise TypeError("%s in not a valid type for data it should dict" % type(value))

    def DataPushWarning(value, loc):
        if len(value) == 0 or value == {}:
            warnings.warn("There is no data to push in {}".format(loc))

    def DataPullWarning(loc):
        warnings.warn("There is no data to pull in {}".format(loc))

warnings.formatwarning = dbmErrorHandler.custom_warning

if __name__ == '__main__':
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
