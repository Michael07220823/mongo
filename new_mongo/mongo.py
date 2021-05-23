import logging
from random import randint, random
from RandomWordGenerator import RandomWord
from pymongo import MongoClient
from new_timer import get_now_time


class MongoDB(object):
    """
    MongoDb CRUD operation and feed fake data.
    """
    def __init__(self,
                 host=str(),
                 username=str(),
                 password=str(),
                 auth_database="admin",
                 database=str(),
                 collection=str(),
                 tls=False,
                 tls_insecure=False,
                 tls_ca_file=str(),
                 socket_timeout_ms=10000,
                 connect_timeout_ms=10000):
        
        logging.info("Connecting to {} MongoDB server...".format(host))
        self.__client = MongoClient(host=host,
                                    username=username,
                                    password=password,
                                    authSource=auth_database,
                                    tls=tls,
                                    tlsInsecure=tls_insecure,
                                    tlsCaFile=tls_ca_file,
                                    socketTimeoutMS=socket_timeout_ms,
                                    connectTimeoutMS=connect_timeout_ms)
        self.database = self.__client[database]
        self.collection = self.database[collection]


    def insert_document(self, document=dict()):
        """
        Insert document to MongoDB.

        Args:
        -----
        document: One dict object.
        """                

        logging.debug("Inserting {} document to MongoDB server...".format(document))
        insert_result = self.collection.insert_one(document)
        logging.debug("mongo.MongoDB.insert_document.insert_result.inserted_id: {}".format(insert_result.inserted_id))
            
            
    def search_document(self, condition=dict()):
        """
        Query document.

        Args:
        -----
        condition: Query condition.


        returns:
        --------
        documents: Single or many dict object.
        """

        try:
            documents = self.collection.find(condition)
            logging.debug("mongo.MongoDB.search_document.documents data type: {}".format(type(documents)))
            logging.debug("mongo.MongoDB.search_document.documents: {}".format(documents))

            return documents
        except StopIteration:
            logging.warning("No document !")


    def update_document(self, condition=dict(), data={"$set": {"age": 18}}):
        """
        Update document value.

        Args:
        -----
        condition: query condition.

        data: Updating data value.
        """

        document = self.collection.update_one(condition, data)
        logging.debug("mongo.MongoDB.update_documents.document: {}".format(document))


    def delete_document(self, condition=dict()):
        """
        Delete document.

        Args:
        -----
        condition: query condition.
        """
        document = object()

        document = self.collection.find_one_and_delete(condition)
        logging.debug("Delete document: {}".format(document))


    def generate_random_documents(self, random_type=str, size=10):
        """
        Generate random documents to MongoDB.

        Args:
        -----
        random_type: Choose random data type. Has three type: int, float and str.
        
        size: Generate specified amount documents.
        """

        # Define constant.
        logging.info("Initializing random data constant variable...")
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

                logging.debug(self.collection.insert_one({"account": account + '@gmail.com', "password": password, "account_length": account_length, 
                                                     "password_length": password_length, "create_time": create_time}))
            elif random_type == int:
                create_time = get_now_time("%Y-%m-%d %H:%M:%S")
                year = randint(1980, 2021)
                country = countrys[randint(0, len(countrys)-1)]
                computer_brand = computer_brands[randint(0, len(computer_brands)-1)]
                notebook_sales = randint(100000, 99999999)
                pc_sales = randint(100000, 99999999)

                logging.debug(self.collection.insert_one({"year": year, "country": country, "computer_brand": computer_brand, 
                                                     "notebook_sales": notebook_sales, "pc_sales": pc_sales, "create_time": create_time}))
            elif random_type == float:
                create_time = get_now_time("%Y-%m-%d %H:%M:%S")
                name = names[randint(0, len(names)-1)]
                height = randint(150, 220) + round(random(), 2)
                weight = randint(30, 100) + round(random(), 2)
                logging.debug(self.collection.insert_one({"name": name, "height": height, "weight": weight, "create_time": create_time}))

        logging.info("Insertd {} doduments successfully !".format(size))


    def close(self):
        """
        Close MongoDB connect.
        """
        logging.info("Closing MongoDB connected...")
        self.__client.close()