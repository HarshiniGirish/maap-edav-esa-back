#!/usr/bin/env python3
import sys
import os
dir_path = os.path.dirname( os.path.realpath( __file__ ) )
previous_path = os.path.dirname( dir_path )
for d in [ dir_path, previous_path ]:
    if not d in sys.path:
        sys.path.append( d )
import argparse
import datetime
import time

from loader.settings import init_environ , DEFAULT_LOGGING , TH_NUMBER , LOADER_ENDPOINT

import loader_module
from loader_module.default_loader import CMRLoader
import pkgutil
import importlib

import logging, logging.config
logging.config.dictConfig( DEFAULT_LOGGING )
logger = logging.getLogger( 'default' )
fail_ingestion = logging.getLogger("fail")



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

if __name__ == "__main__":
    ru = import_submodules( loader_module )
    parser = argparse.ArgumentParser( description = 'Pycsw data loader' )

    parser.add_argument(
            '-lk', '--loaderkey',
            required = False,
            help ='Available Module are: %s'%([ load_module.getKey() for load_module in get_all_subclasses( CMRLoader ) ] ),
            default = "default"
            )
    parser.add_argument(
            '-ds', '--dataset',
            required = True,
            help ='DatasetId'
            )
    parser.add_argument(
            '-p', '--pattern',
            required = False,
            help ='Get only the raster that match with this pattern',
            default = ".*$"
            )
    parser.add_argument(
            '-th', '--th_number',
            required = False,
            type = int,
            default = TH_NUMBER,
            help = 'Number of process for parallel ingestion'
            )

    args = parser.parse_args()

    logger.info("Start Ingestion Process")
    fail_ingestion.info("Start Ingestion Process")
    for load_module in get_all_subclasses( CMRLoader ):
        if load_module.getKey() == args.loaderkey:
            try:
                loader=load_module(args,logger,fail_ingestion)
            except ValueError as ex:
                parser.print_help()
                sys.exit( 1 )
            except Exception as e:
                logger.error("Ingestion Process terminated with error: %s"%(e))
                sys.exit(1)

            loader.start()
            break
    else:
        logger.error( 'No loader module mathcing key "%s" found' %( args.loaderkey ) )
        logger.error( 'Available modules are: %s' %( [ load_module.getKey() for load_module in get_all_subclasses( CMRLoader ) ] ) )

    logger.info("Ingestion Process Terminated")
    fail_ingestion.info("Ingestion Process Terminated")
    sys.exit(0)

