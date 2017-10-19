import petl, MySQLdb, sys

from pymongo import MongoClient
from sqlalchemy import *


# handle characters outside of ascii
reload(sys)
sys.setdefaultencoding('utf8')

db_conn = {
    'mmj': 'dbname=mmjmenurails3_db user=root host=127.0.0.1',
    'growone': 'mongodb://meteor:@127.0.0.1'
}

# set connections and cursors
source_conn = MySQLdb.connect(db_conn['mmj'])
target_conn = MongoClient(db_conn['growone'])
source_cursor = source_conn.cursor()

# retrieve the names of the source tables to be copied
source_cursor.execute("""
    select table_name from information_schema.columns where table_name in ('menu_items','patients') group by 1""")
source_tables = source_cursor.fetchall()

# iterate through table names to copy over
for table in source_tables:
    target_cursor.execute("drop table if exists %s" % (t[0])) # won't work with mongo
    source_datasource = petl.fromdb(source_conn, 'select * from %s' % (t[0]))
    petl.todb(source_datasource, target_conn, table[0], create=True, sample=10000)
