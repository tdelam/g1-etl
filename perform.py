import petl as etl, MySQLdb, sys

from pymongo import MongoClient
from sqlalchemy import *


# handle characters outside of ascii
reload(sys)
sys.setdefaultencoding('utf8')


DB_CONN = {
    'mmj': 'dbname=mmjmenurails3_db user=root host=127.0.0.1',
    'growone': 'mongodb://meteor:@127.0.0.1'
}

class PerformETL(object):

    def __init__(self):
        pass


    def extract(self):
        """
        Grab all data from source(s).
        """
        pass


    def load_rows(self):
        """
        Manipulate the data extracted by the previous extract method
        """
        pass


    def transform_rows(self):
        """
        Load the transformed data into the destination(s)
        """
        pass


    def source_count(self):
        """
        Count the number of records from source(s)
        """
        pass


    def destination_count(self):
        """
        Same as source_count but with destination(s)
        """
        pass