#!/usr/bin/env python3
import sys
import os
import argparse
import datetime
import time
from pathlib import Path

#module import
import importlib
import pkgutil
import loader_modules
from loader_modules.default_loader import MongoLoader

#settings and log configuration
import settings
import logging, logging.config
logging.config.dictConfig( settings.DEFAULT_LOGGING )
logger = logging.getLogger( 'default' )


#Recursively import all loader submodules of type <MongoLoader>
def import_submodules( package ):
    if isinstance( package, str ):
        package = importlib.import_module( package )
    results = {}
    for loader, name, is_pkg in pkgutil.walk_packages( package.__path__ ):
        full_name = package.__name__ + '.' + name
        if is_pkg:
            results.update( import_submodules( full_name ) )
        else:
            results[full_name] = importlib.import_module( full_name )
    return results


def get_all_subclasses( cls ):
    return set(cls.__subclasses__()).union( [s for c in cls.__subclasses__() for s in get_all_subclasses(c)] )


def pause( pid=None ):
    if pid is None:
        pause_lock_file = settings.PAUSE_LOCK + ".lock"
    else:
        pause_lock_file = settings.PAUSE_LOCK + "_" + pid + ".lock"
    logger.info( 'Set pause lock file %s' %( pause_lock_file ) )
    Path( pause_lock_file ).touch()
    logger.info( 'Done! Before procede with maintenance check on loader log %s that all the loaders have completed the commit' %( settings.INGESTION_LOG ) )


def play( pid=None ):
    if pid is None:
        pause_lock_file = settings.PAUSE_LOCK + ".lock"
    else:
        pause_lock_file = settings.PAUSE_LOCK + "_" + pid + ".lock"
    logger.info( 'Remove lock %s and activate the paused loaders' %( pause_lock_file ) )
    if os.path.isfile( pause_lock_file ):
        os.remove( pause_lock_file )
        logger.info( 'Done. %s removed' %( pause_lock_file ) )


def stop(mid):
    logging.info("Stop MongoLoad instance")
    Path("/tmp/%s_STOP.lock"%(mid)).touch()
    logging.info("MongoLoad Killing in progress...")


if __name__ == '__main__':
    ru = import_submodules( loader_modules )

    parser = argparse.ArgumentParser( description = 'Mongo mwcs/opensearch data loader' )

    parser.add_argument(
        '-mid','--mid',
        required = False,
        help = "MongoLoader instance identifier"
    )
    parser.add_argument(
        '-jid','--jid',
        required = False,
        help = "MongoLoader instance identifier"
    )
    parser.add_argument(
        '-pause', '--pause',
        required = False,
        action = 'store_true',
        help = 'Pause all the loaders in progress (e.g. to perform planned maintenance )'
    )
    parser.add_argument(
        '-play', '--play',
        required = False,
        action = 'store_true',
        help = 'Remove lock and activate all the paused loaders ( e.g. after maintenance is completed )'
    )
    parser.add_argument(
        '-stop','--stop',
        required = False,
        help = "Kill mongoload instance"
    )
    parser.add_argument(
        '-src', '--source',
        required = False,
        help = 'Directory to load or absolute filename'
    )
    parser.add_argument(
        '-s', '--startDate',
        required = False,
        help = 'Start date. Temporal limit for raster to register'
    )
    parser.add_argument(
        '-e', '--endDate',
        required = False,
        help = 'End date. Temporal limit for raster to register'
    )
    parser.add_argument(
        '-p','--pattern',
        required = False,
        default='.+',
        help = 'Regular expression pattern used to filter filelist ( default ".+*" )'
    )
    parser.add_argument(
        '-reload', '--reload',
        required =False,
        action = 'store_true',
        help = 'Reload/replace raster on catalogue. If not set the loader skip registration of raster already managed'
    )
    parser.add_argument(
        '-debug', '--debug',
        required = False,
        action = 'store_true',
        help = 'Print debug log on stdout'
    )
    parser.add_argument(
        '-th', '--th_number',
        required = False,
        type = int,
        default = settings.TH_NUMBER,
        help = 'Number of process for parallel ingestion'
    )
    parser.add_argument(
        '-ds', '--dataset',
        required = False,
        help ='Available modules are: %s' %( [ load_module.getKey() for load_module in get_all_subclasses( MongoLoader ) ] )  #'Dataset key used to identify the loader module'
    )
    parser.add_argument(
        '-pl','--processinglevel',
        required = False,
        help = 'Processing Level LEVEL1B | LEVEL2. Used for example on sentinel-5P loader modules'
    )
    parser.add_argument(
        '-mr','--maxrecords',
        required=False,
        help="Number of results per page in searches on finder.creodias.eu or other"
    )
    parser.add_argument(
        '-pg','--page',
        required=False,
        help="Page number to search on finder.creodias.eu or other"
    )
    parser.add_argument(
        '-i','--instrument',
        required = False,
        help = 'instrument. Used for example on sentinel-5P loader modules'
    )
    parser.add_argument(
        '-pt','--product_type',
        required=False,
        help = 'Pruduct Type. Used for example on sentinel-5P loader modules'
    )
    parser.add_argument(
        '-or','--orbit',
        required=False,
        help = 'Orbit type (ASC/DESC) or orbit number'
    )
    parser.add_argument(
        '-cloud','--cloud',
        required=False,
        help = 'Cloud cover limits'
    )
    parser.add_argument(
        '-report','--report',
        required = False,
        action = 'store_true',
        help = 'Print report summary at the end of data registration'
    )
    parser.add_argument(
        '-updateurl','--updateurl',
        required = False,
        help = 'DPS status update url'
    )
    parser.add_argument(
        '-days_loop', '--days_loop',
        required = False,
        default = 0,
        type = int,
        help = 'Number of days this loader remain active and continue to search for new data'
    )

    parser.add_argument(
        '-onError', '--onError',
        required = False,
        default = "skip",
        help = "Mongoloader behavior in case of error [strict,skip]"
    )

    parser.add_argument(
        '-geom','--geometry',
        required = False,
        help = 'WKT or geoJson polygon used to filter searches on dataprovider catalogue'
    )

    parser.add_argument(
        '-pll','--pipelinelevel',
        required = False,
        default = "processing",
        help = "Only used for sentinel3@json loader. Specifies the pipeline type before the loader[search,download,processing]"
    )

    parser.add_argument(
        '-epsg','--epsg',
        required = False,
        default = None,
        help = "force input raster epsg"
    )

    parser.add_argument(
        '-res','--resolution',
        required = False,
        default = None,
        help = "force input raster resolution"
    )
    parser.add_argument(
        '-dd','--descriptionDocument',
        required=True,
        help="abspath to description document or json "
    )

    args = parser.parse_args()

    if args.debug or args.pause or args.play:
        logger = logging.getLogger( 'debug' )

    if args.pause == True:
        pause( args.mid )
        sys.exit(0)

    if args.play == True:
        play( args.mid )
        sys.exit(0)

    if args.stop:
        stop(args.stop)
        sys.exit(0)

    if args.mid is None:
        args.mid = str( os.getpid() )
        print("MongoLoader Instance Executed with id:%s"%( args.mid ))

    if args.dataset is None:
        logger.warning( 'Dataset key not specified, default loader module will be used' )
        args.dataset = 'default'

    if not os.path.isfile( settings.STATUSLOADER_DIR + "_" + args.mid ):
        Path( settings.STATUSLOADER_DIR + "_" + args.mid ).touch()

    for load_module in get_all_subclasses( MongoLoader ):
        if load_module.getKey() == args.dataset:
            now = datetime.datetime.utcnow()
            end_loop = now + datetime.timedelta( days = args.days_loop )
            while now <= end_loop:
                logger.info( 'Star registration with "%s"' %( load_module  ) )
                try:
                    loader = load_module( args, logger )
                except ValueError as ex:
                    print(ex)
                    parser.print_help()
                    sys.exit( 1 )
                except Exception as ex:
                    logger.exception( str( ex ) )
                    sys.exit( 1 )
                try:
                    loader.start()
                except Exception as e:
                    sys.exit(str(loader.error_list))
                if args.report:
                    print( loader.report() )
                now = datetime.datetime.utcnow()
                if args.days_loop > 0 and now <= end_loop:
                    print( 'Sleep for an hour and then do another round' )
                    time.sleep( 1 * 60 * 60 ) #sleep for one hour
            break
    else:
        logger.error( 'No loader module mathcing key "%s" found' %( args.dataset ) )
        logger.error( 'Available modules are: %s' %( [ load_module.getKey() for load_module in get_all_subclasses( MongoLoader ) ] ) )
        sys.exit("No loader module mathcing key '%s' found"%( args.dataset ))

    if os.path.isfile( "/tmp/%s_STOP.lock" %( args.mid ) ):
        print("MongoLoader Instance killed")
        os.remove("/tmp/%s_STOP.lock"%(args.mid))
        sys.exit(1)

    if os.path.isfile(settings.STATUSLOADER_DIR + "_" + args.mid ):
        os.remove(settings.STATUSLOADER_DIR + "_" + args.mid )


    sys.exit( 0 )
