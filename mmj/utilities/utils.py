import urllib2
import logging
import logging.handlers
import time

from datetime import date, datetime
from calendar import timegm
from pymongo import MongoClient
from bson.objectid import ObjectId


import petl as etl
from petl.io.db import DbView
from petl.io.json import DictsView
from petl.transform.basics import CutView
from datetime import date, datetime

logging.basicConfig(filename="g1-etl-utils.log", level=logging.INFO)
log = logging.getLogger("g1-etl-utils")


def load_db_data(db, dispensary_id, table_name):
    """
    Data extracted from source db
    """
    sql = ("SELECT * from {0} WHERE "
           "dispensary_id={1}").format(table_name, dispensary_id)

    return etl.fromdb(db, sql)


def load_employees(db, dispensary_id):
    sql = ("SELECT u.* FROM dispensary_users as du INNER JOIN users "
           "as u WHERE u.id = du.user_id AND "
           "du.dispensary_id = {0}").format(dispensary_id)

    return etl.fromdb(db, sql)


def load_membership_prices(db, dispensary_id):
    sql = ("SELECT mp.* FROM memberships as m INNER JOIN membership_prices "
           "as mp WHERE mp.membership_id = m.id AND "
           "m.dispensary_id = {0} ORDER BY id DESC").format(dispensary_id)
    
    return etl.fromdb(db, sql)

def view_to_list(data):
    if type(data) is DbView or type(data) is CutView:
        # convert the view to a lists of lists for petl
        # map is quicker than list comp
        return list(map(list, data))

    if type(data) is DictsView:
        return data


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def download_images(env, user_id, pic):
    """
    Downloading MMJ with progress logging
    """
    remote = (r"https://wm-mmjmenu-images-{0}.s3.amazonaws.com/"
              "customers/pictures/{1}/large/{2}").format(env, user_id, pic)
    local = str(user_id) + ".jpg"

    url = urllib2.urlopen(remote)
    header = url.info()
    total_size = int(header["Content-Length"])

    log.info("---"
             "Downloading {0} bytes of image {1}...".format(total_size, pic))
    fp = open(local, 'wb')

    block_size = 8192
    count = 0
    while True:
        chunk = url.read(block_size)
        if not chunk:
            break
        fp.write(chunk)
        count += 1
        if total_size > 0:
            percent = int(count * block_size * 100 / total_size)
            if percent > 100:
                percent = 100
            log.info("%2d%%" % percent)
            if percent < 100:
                log.info("\b\b\b\b\b")  # Erase "NN% "
            else:
                log.info("---"
                         "{0} successfully downloaded.".format(pic))

    fp.flush()
    fp.close()
    if not total_size:
        log.error("--"
                  "Error: File {0} did not successfully download".format(pic))


def chunks(data, size):
    """
    chunks big data so we can send the API data chunks
    """
    chunk = [data[i:i + size] for i in range(0, len(data), size)]
    return chunk


def mongo_connect_and_insert(payload):
    """
    Connects to mongodb.
    Injects pre-generated unique _id.
    Inserts payload into the Imports collection.
    """
    db = MongoClient('mongodb://localhost:3005/')
    imports = db.meteor.etl.imports
    # inject pre-generated and validated ObjectId String
    payload['_id'] = generate_unique_mongo_id(imports)
    imports.insert_one(payload)  # insert paylaod


def generate_unique_mongo_id(imports):
    """
    Generates an object id and extracts the hex string.
    Checks the import collection for _id collisions on the hex string.
    Returns unique hex string.
    """
    mongo_id = None
    while True:
        mongo_id = str(ObjectId())
        id_match = imports.find_one({'_id': mongo_id})
        if id_match is None:
            break
    return mongo_id


def true_or_false(value):
    """
    Returns true or false for transforming
    """
    if value == 1:
        return True
    elif value == 0:
        return False
    return False


def account_status(status):
    """
    Returns ACTIVE or INACTIVE for transforming
    """
    if status == 1:
        return "INACTIVE"
    elif status == 0:
        return "ACTIVE"
    return False


def create_epoch(dt):
    """
    Returns epoch date/time
    """
    return int(dt.strftime('%s'))
