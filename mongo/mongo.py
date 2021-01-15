import logging
from random import randint, random
from RandomWordGenerator import RandomWord
from pymongo import MongoClient, ReturnDocument, ASCENDING
from new_timer import get_now_time


class MongoDB(object):
    """
    MongoDb operation.
    """
    def __init__(self, url=str(), database=str(), collection=str()):
        logging.info("Connecting to MongoDB server...")
        self.__client = MongoClient(url)
        self.__db = self.__client[database]
        self.__coll = self.__db[collection]


    def insert_document(self, document=dict()):
        """
        Insert documents to MongoDB.

        Args:
        -----
        document: Many documents.
        """                

        try:
            logging.debug("Inserting {} document to MongoDB server...".format(document))
            insert_result = self.__coll.insert_one(document)
            logging.debug("insert_document.insert_result.inserted_id: {}".format(insert_result.inserted_id))
            logging.debug("Inserted documents successfully !")
        except Exception as err:
            logging.error(err, exc_info=True)
            

    def query_document(self, condition=dict(), sort=list()):
        """
        Quert document.

        Args:
        -----
        condition: query condition.

        sort: sort type before query.


        returns:
        --------
        document: single document.
        """

        logging.debug("Querying documents...")
        documents_count = self.__coll.count_documents(condition)
        logging.debug("Query matching documents count: {}".format(documents_count))

        try:
            documents = self.__coll.find(condition, sort=[sort])
            logging.debug("query_document.documents data type: {}".format(type(documents)))
            logging.debug("query_document.documents: {}".format(documents))

            return documents
        except StopIteration:
            logging.warning("No document !")


    def update_documents(self, condition=dict(), data=dict(), sort=list()):
        """
        Quert document.

        Args:
        -----
        condition: query condition.

        data: Updating data value.
        
        sort: sort type before query.
        """

        document = object()

        logging.info("Updating documents...")
        documents_count = self.__coll.count_documents(condition)
        logging.info("Updating documents count: {}".format(documents_count))

        while document != None:
            document = self.__coll.find_one_and_update(condition,
                                                       data, 
                                                       upsert=False,
                                                       sort=[sort], 
                                                       return_document=ReturnDocument.AFTER)
            if document == None:
                logging.info("Not finded any matching document ! ")
            else:
                logging.debug(document)

        logging.info("Updated documents finish !")


    def delete_documents(self, condition=dict(), sort=list()):
        """
        Delete document.

        Args:
        -----
        condition: query condition.
        
        sort: sort type before query.
        """
        document = object()

        logging.info("Deleting documents...")
        document_count = self.__coll.count_documents(condition)
        logging.info("Deleting documents count: {}".format(document_count))

        while document != None:
            document = self.__coll.find_one_and_delete(condition, sort=[sort])
            logging.debug("Delete document: {}".format(document))

        logging.info("Deleted {} documents successfully !".format(document_count))


    def generate_random_documents(self, random_type=str, size=10):
        """
        Generate random documents to MongoDB.

        Args:
        -----
        condition: query condition.

        random_type: Choose random data type. Has three type: int、float and str.
        
        size: Generate specified amount documents.
        """

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

        # Insert data to MongoDB.
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
                logging.debug(self.__coll.insert_one({"name": name, "height": height, "weight": weight, "create_time": create_time}))

        logging.info("Insertd {} doduments successfully !".format(size))


    def close(self):
        """
        Close MongoDB connect.
        """
        logging.info("Closing MongoDB connected...")
        self.__client.close()