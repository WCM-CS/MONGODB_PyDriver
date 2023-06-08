from pymongo import MongoClient # to establish connection with MongoDB
from bson.objectid import ObjectId # to return an object value for _id feild in the data dictionary
# since _id can be the primary key we will overwrite or create our own despite what's passed through data dictionary

class AnimalShelter:

    def __init__(self): #Default constructor established connection to the DB using the user account information previously set
        # Local Variables for the method 
        USER = 'aacuser' # user account name
        PASS = 'SNHU1234' # user account password
        HOST = 'nv-desktop-services.apporto.com' # mongo specified host
        PORT = 31237 # mongo specified port
        DB = 'AAC' # database name
        COL = 'animals' # database collection name
        
        # Class Instance Variables to initialize connection to DB
        self.client = MongoClient('mongodb://%s:%s@%s:%d' % (USER, PASS, HOST, PORT))
        self.database = self.client['%s' % (DB)]
        self.collection = self.database['%s' % (COL)] # this allows us to avoid specifying the collections name in the query
        
    def create(self, data): # data is a dictionary with all major fields from AAC animals collection
        if data is not None:
            data['_id'] = ObjectId() # sets the primary key to a unique id using bson.objectid module to ensure proper queries 
            result = self.collection.insert_one(data) # executes the insert function
            return True if result.inserted_id else False
            # checks that the result variable has a inserted_id, bool auto sets with successful insert function
        else:
            raise Exception("USER ERROR: Parameter is empty.")
    
    def read(self, inspect): # inspect should be a dictionary, can include multiple fields for query specificity 
        if inspect is not None:
            docs = self.collection.find(inspect) # performs the query, note self.collection is used, its set in the constructor 
            result = list(docs)
            return result# if successful return query results as a list
        else:
            return [] # else return an empty list
        
    def explain(self, query): # extra function to determine if the index is being used 
        if query is not None:
            res = self.collection.find(query)
            res2 = res.explain() # here it specifies to return the execution plan, verifys index in use
            return res2
        else:
            return []
        
    def update(self, query, update_dict, update_mult=False):# update_mult if not passed anything only one update will occur
        if query is not None and update_dict is not None: # confirms necessary varibales are not null
            update_dict = {"$set": update_dict} #this line adds $set to the update dictionary, so when passed it doesn't need it 
            if update_mult: # branch for multiple updates
                result = self.collection.update_many(query, update_dict) # function for mutiple documents uodated
                return_str = f"The number of documents updated: {result.modified_count}" # returns number of modified documents
                return return_str
            else: # branch for singular update
                result = self.collection.update_one(query, update_dict) # function for one document updated
                return_str = f"The number of documents updated: {result.modified_count}"
                return return_str
        else:
            raise Exception("USER ERROR: One or more parameter is empty.")
            
    def delete(self, query, delete_mult=False): # similar sytax to update function
        if query is not None:
            if delete_mult:
                result = self.collection.delete_many(query)
                return_str = f"The number of documents removed: {result.deleted_count}" # returns number of deleted documents
                return return_str
            else:
                result = self.collection.delete_one(query)
                return_str = f"The number of documents removed: {result.deleted_count}"
                return return_str
        else:
            raise Exception("USER ERROR: Parameter is empty.")

    def createIndex(self, query, order):
        if query is not None:
            key = [(field, order) for field in query.keys()]
            new_index = self.collection.create_index(key)
            return new_index
        else: 
            raise Exception("USER ERROR: Parameter is empty.")
            
            