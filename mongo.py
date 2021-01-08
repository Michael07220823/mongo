# Usage
# cd ..
# python -m Smart_door_system_frontend_2.mongo.mongo

import logging
from random import randint, random
from RandomWordGenerator import RandomWord
from pymongo import MongoClient, ReturnDocument, ASCENDING
from new_timer.timer import get_now_time

if __name__ == "__main__":
    # Set logging config.
    FORMAT = '%(asctime)s [%(levelname)s] %(message)s'
    DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    logging.basicConfig(level=logging.DEBUG, format=FORMAT, datefmt=DATE_FORMAT)


class MongoDB(object):
    """
    MongoDb operation.
    """
    @classmethod
    def insert_document(cls, url=str(), database=str(), collection=str(), documents=dict(), many=False):
        """
        Insert documents to mongodb.

        Args:
        -----
        url: Mongodb host.

        database: Database name.

        collection: Collection name.

        documents: Many documents.

        many: Choose insert single document or many documents.
        """
        logging.info("Connecting to mongodb server...")
        client = MongoClient(url)
        db = client[database]
        coll = db[collection]

        if many == True:
            if type(documents) == dict:
                logging.critical("documents need to be a list or tuple data type!")
                logging.debug("mongo.MongoDB.insert_document.documents value: {}".format(documents))
                logging.debug("mongo.MongoDB.insert_document.documents type: {}".format(type(documents)))
                return

            logging.info("Inserting {} documents...".format(len(documents)))
            for doc in documents:
                logging.debug("Inserting {} document to mongodb server...".format(doc))
                try:
                    insert_results = coll.insert_one(doc)
                except TypeError:
                    logging.error("Inserting document data type error !", exc_info=True)
                    logging.debug("mongo.MongoDB.insert_document.doc value: {}".format(doc))
                    logging.debug("mongo.MongoDB.insert_document.doc type: {}".format(type(doc)))
                    client.close()
                    return
                logging.debug("mongo.MongoDB.insert_document.insert_results.inserted_id: {}".format(insert_results.inserted_id))
                
        else:
            logging.info("Inserting {} document to mongodb server...".format(documents))
            insert_results = coll.insert_one(documents)
            logging.debug("mongo.MongoDB.insert_document.insert_results.inserted_id: {}".format(insert_results.inserted_id))

        logging.info("Inserted documents successfully !")
        logging.info("Closing mongodb connecting...")
        client.close()


    @classmethod
    def query_document(cls, url=str(), database=str(), collection=str(), condition=dict(), sort=list(), many=False):
        """
        Quert document.

        Args:
        -----
        url: Mongodb host.

        database: Database name.

        collection: Collection name.
        
        condition: query condition.

        sort: sort type before query.

        documents: Many documents.

        many: Choose insert single document or many documents.


        returns:
        --------
        document: single document.

        documents: many documents.
        """
        documents = list()

        logging.info("Connecting to mongodb server...")
        client = MongoClient(url)
        db = client[database]
        coll = db[collection]

        logging.info("Querying documents...")
        documents_count = coll.count_documents(condition)
        logging.info("Query matching documents count: {}".format(documents_count))

        try:
            if many == True:
                for doc in coll.find(condition, sort=[sort]):
                    documents.append(doc)
                    logging.info("document: {}".format(doc))
                    logging.debug("document type: {}".format(type(doc)))
                    logging.info("Closing mongodb connecting...")
                    client.close()
                    return documents
            else:
                document = coll.find(condition, sort=[sort])
                logging.debug("mongo.MongoDB.query_document.documents data type: {}".format(type(documents)))

                document = document.next()
                logging.debug("First document: {}".format(document))
                logging.info("Closing mongodb connecting...")
                client.close()

                return document
        except StopIteration:
            logging.info("No document !")



    @classmethod
    def update_documents(cls, url=str(), database=str(), collection=str(), condition=dict(), data=dict(), sort=list()):
        """
        Quert document.

        Args:
        -----
        url: Mongodb host.

        database: Database name.

        collection: Collection name.
        
        condition: query condition.

        data: Updating data value.
        
        sort: sort type before query.
        """

        document = object()

        logging.info("Connecting to mongodb server...")
        client = MongoClient(url)
        db = client[database]
        coll = db[collection]

        logging.info("Updating documents...")
        documents_count = coll.count_documents(condition)
        logging.info("Updating documents count: {}".format(documents_count))

        while document != None:
            document = coll.find_one_and_update(condition,
                                                data, 
                                                upsert=False,
                                                sort=[sort], 
                                                return_document=ReturnDocument.AFTER)
            if document == None:
                logging.info("Not finded any matching document ! ")
            else:
                logging.debug(document)

        logging.info("Updated documents finish !")
        logging.info("Closing mongodb connecting...")
        client.close()


    @classmethod
    def delete_documents(cls, url=str(), database=str(), collection=str(), condition=dict(), sort=list()):
        """
        Delete document.

        Args:
        -----
        url: Mongodb host.

        database: Database name.

        collection: Collection name.
        
        condition: query condition.
        
        sort: sort type before query.
        """
        document = object()

        logging.info("Connecting to mongodb server...")
        client = MongoClient(url)
        db = client[database]
        coll = db[collection]

        logging.info("Deleting documents...")
        document_count = coll.count_documents(condition)
        logging.info("Deleting documents count: {}".format(document_count))

        while document != None:
            document = coll.find_one_and_delete(condition, sort=[sort])
            logging.debug("Delete document: {}".format(document))

        logging.info("Deleted {} documents successfully !".format(document_count))
        logging.info("Closing mongodb connecting...")
        client.close()


    @classmethod
    def generate_random_documents(cls, url=str(), database=str(), collection=str(), random_type=str, size=10):
        """
        Generate random documents to mongodb.

        Args:
        -----
        url: Mongodb host.

        database: Database name.

        collection: Collection name.
        
        condition: query condition.

        random_type: Choose random data type. Has three type: int、float and str.
        
        size: Generate specified amount documents.
        """

        # Mongodb connect.
        logging.info("Connecting to mongodb server...")
        client = MongoClient(url)
        db = client[database]
        coll = db[collection]

        # Define constant.
        logging.info("Defining random data constant variable...")
        if random_type == str:
            rand_account = RandomWord(max_word_size=12, constant_word_size=False,include_digits=True)
            rand_passwd = RandomWord(max_word_size=20, constant_word_size=False, include_digits=True, include_special_chars=True)
        elif random_type == int:
            computer_brands = ["ASUS", "Acer", "MSI", "Lenovo", "Microsoft", "Mac"]
            countrys = ["America", "China", "Taiwan", "Japan", "Korea"]
        elif random_type == float:
            names = ["michael", "peter", "allen", "kevin", "jack"]
        else:
            logging.warning("random_type data type error ! only str、int and float type.", exc_info=True)
            raise TypeError

        # Insert data to mongodb.
        logging.info("Inserting {} type random data...".format(random_type))
        for i in range(size):
            if random_type == str:
                create_time = get_now_time("%Y-%m-%d %H:%M:%S")
                account = rand_account.generate()
                password = rand_passwd.generate()
                account_length = len(account)
                password_length = len(password)

                logging.debug(coll.insert_one({"account": account + '@gmail.com', "password": password, "account_length": account_length, 
                                               "password_length": password_length, "create_time": create_time}))
            elif random_type == int:
                create_time = get_now_time("%Y-%m-%d %H:%M:%S")
                year = randint(1980, 2021)
                country = countrys[randint(0, len(countrys)-1)]
                computer_brand = computer_brands[randint(0, len(computer_brands)-1)]
                notebook_sales = randint(100000, 99999999)
                pc_sales = randint(100000, 99999999)

                logging.debug(coll.insert_one({"year": year, "country": country, "computer_brand": computer_brand, 
                                               "notebook_sales": notebook_sales, "pc_sales": pc_sales, "create_time": create_time}))
            elif random_type == float:
                create_time = get_now_time("%Y-%m-%d %H:%M:%S")
                name = names[randint(0, len(names)-1)]
                height = randint(150, 220) + round(random(), 2)
                weight = randint(30, 100) + round(random(), 2)
                logging.debug(coll.insert_one({"name": name, "height": height, "weight": weight, "create_time": create_time}))

        logging.info("Insertd {} doduments successfully !".format(size))
        logging.info("Closing mongodb connecting...")
        client.close()