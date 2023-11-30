import os
from dotenv import load_dotenv
load_dotenv()

#mongo vars
MONGODB_USER = os.getenv( "MONGODB_USER" )
MONGODB_PASSWORD = os.getenv( "MONGODB_PASSWORD" )
MONGODB_HOST = os.getenv( "MONGODB_HOST", "localhost" )
MONGODB_PORT = os.getenv( "MONGODB_PORT", "27017" )
MONGODB_USER_ADMIN = os.getenv( "MONGODB_USER_ADMIN" )
MONGODB_PASSWORD_ADMIN = os.getenv( "MONGODB_PASSWORD_ADMIN" )
MONGODB_DBNAME = os.getenv("MONGODB_DBNAME","catalogue")

AWS_BUCKET=os.getenv("AWS_BUCKET",None)
AWS_ACCESS_KEY_ID=os.getenv("AWS_ACCESS_KEY_ID",None)
AWS_SECRET_ACCESS_KEY=os.getenv("AWS_SECRET_ACCESS_KEY",None)
AWS_S3_ENDPOINT=os.getenv("AWS_S3_ENDPOINT",None)
WCSPATH_ENDPOINT=os.getenv("WCSPATH_ENDPOINT",None)


if MONGODB_USER is None or MONGODB_USER is None:
    #authentication wit username and password is not configured
    connection="mongodb://%s:%s/" %( MONGODB_HOST, MONGODB_PORT )
else:
    connection="mongodb://%s:%s@%s:%s/?authSource=%s" %( MONGODB_USER, MONGODB_PASSWORD, MONGODB_HOST, MONGODB_PORT ,MONGODB_DBNAME)

if MONGODB_USER_ADMIN is None or MONGODB_PASSWORD_ADMIN is None:
    connection_admin="mongodb://%s:%s/" %( MONGODB_HOST, MONGODB_PORT )
else:
    connection_admin="mongodb://%s:%s@%s:%s/?authSource=admin"%( MONGODB_USER_ADMIN, MONGODB_PASSWORD_ADMIN, MONGODB_HOST, MONGODB_PORT )

#threding vars
TH_NUMBER=4

def init_environ( ENV_VARS ):
    for key in ENV_VARS:
        AWS_SETTING_VALUE = ENV_VARS[ key ]
        if type( AWS_SETTING_VALUE ) == str and AWS_SETTING_VALUE.startswith("http"):
            AWS_SETTING_VALUE=AWS_SETTING_VALUE.split("/")[2]
        if AWS_SETTING_VALUE  is not None:
            os.environ[ key ] = str( AWS_SETTING_VALUE )

#lock directory and files
LOCK_DIR = "/tmp/mongolock/"
PAUSE_LOCK = "/tmp/mongoloader_pause.lock"

INGESTION_LOG = os.getenv( "INGESTION_LOG", "/opt/meeo-das/log/ingestion.log" )
if not os.path.isdir( os.path.dirname( INGESTION_LOG ) ):
    os.makedirs( os.path.dirname( INGESTION_LOG ) )

TMP_VRT_DIR = os.getenv("TMP_VRT_DIR","/opt/meeo-das/loader/tmp")
if not os.path.isdir( os.path.dirname( TMP_VRT_DIR ) ):
        os.makedirs( os.path.dirname( TMP_VRT_DIR ) )

STATUS_DIR = "/tmp/mongoloaderStatus"
if not os.path.isdir(STATUS_DIR):
    os.mkdir(STATUS_DIR)

STATUSLOADER_DIR = os.path.join( STATUS_DIR, "loaderstatus" )

DEFAULT_LOGGING={
    'version':1,
    'disable_exixting_loggers':False,
    'formatters':{
        'verbose':{
            'format': '[%(asctime)s][%(levelname)s][%(name)s:L%(lineno)s] - %(message)s'
        },
        'stdout':{
            'format': '[%(levelname)s][%(name)s:L%(lineno)s] - %(message)s'
        }
    },
    'handlers':{
        'logfile':{
            'level':'DEBUG',
            'formatter': 'verbose',
            'class': 'logging.handlers.WatchedFileHandler',
            'filename': INGESTION_LOG
        },
        'console':{
            'level': 'DEBUG',
            'formatter': 'stdout',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
        },
        'console_warnings':{
            'level': 'WARNING',
            'formatter': 'stdout',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout'
        },
    },
    'loggers':{
        'debug': {
            'handlers': [ 'logfile', 'console'],
            'propagate': True,
            'level': 'DEBUG',
        },
        'default': {
            'handlers': [ 'logfile', 'console_warnings' ],
            'propagate': True,
            'level': 'DEBUG',
        },
        'virtuaload': {
            'handlers': ['logfile'],
            'propagate': True,
            'level': 'DEBUG'
        },
        'dataset_removal': {
            'handlers': ['logfile'],
            'propagate': True,
            'level': 'DEBUG'
        }
    }
}


DATASETID_COLLECTION_SCHEMA={
        "$jsonSchema" : {
            "bsonType" : "object",
            "required" : ["productId", "productDate"],
            "properties" : {
                "productId" : {
                    "bsonType" : "string",
                    "description" : "It must be a string and is required. It is an unique product identifier (i.e filename)"
                    },
                "productDate" : {
                    "bsonType" : "date",
                    "description" : "must be a date and is required - product date"
                    }
                }
            }
        }

