from __future__ import unicode_literals

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.core.exceptions import ValidationError

import json
import jsonschema
import os
import time
import uuid
import psycopg2
import datetime
import re
import errno
import fcntl
import boto3

from osgeo import gdal, osr, ogr
from pyproj import Transformer

import logging
from . import settings
logging.config.dictConfig( settings.DEFAULT_LOGGING )
logger = logging.getLogger( 'debug' )
gdal.SetConfigOption( 'GDAL_PAM_ENABLED', 'NO' )
gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN","EMPTY_DIR")
METADATA_SCHEMA = os.path.join( os.path.dirname(__file__), 'metadata_loader_schema.json' )

@csrf_exempt
def get_spec(request):
    if request.method != "GET":
        return JsonResponse(status=405)
    try:
        datasetId=request.GET.get("datasetId")
    except Exception as e:
        print(e)
        return JsonResponse({"status":400,"message":"datasetId required"},status=400)

    try:
        with open(settings.DATASETS_SPEC) as json_file:
            data = json.load(json_file)[datasetId]
        return JsonResponse(data,status=200)
    except Exception as ex:
        return JsonResponse({"status":200,"message":"datasetId specification not available"},status=200)



@csrf_exempt
def upload( request ):
    settings.init_environ()
    """
    Upload raster on the system
    0) check json and data
    1) temporary store raster
    2) register raster to catalogue
    3) move raster to mwcs
    """
    logger.info( "Data upload started" )
    #check method
    files = None
    remote_files = None

    if request.method != 'POST':
        return JsonResponse( { "status":405, "error": "Method %s Allowed" %( request.method ) }, status=405 )

    #check file
    try:
        files = request.FILES
        logger.debug( "Post Raster OK" )
    except Exception as e:
        return JsonResponse( { "status":500, "error": "Raster upload error" }, status=500 )

    if not files:
        try:
            metadata_source = json.loads(request.POST.get("metadata"))
            for feature_id in metadata_source:
                remote_files = metadata_source[feature_id]["source"]
            logger.debug( "Post Raster filename OK" )
        except Exception as e:
            print(e)
            return JsonResponse( { "status":500, "error": "Raster filename upload error" }, status=500 )

    #check metadata
    try:
        if files:
            metadata = json.loads( request.POST.get( 'metadata' ) )
            logger.debug( "Post metadata json OK" )
        else:
            metadata = json.loads(request.POST.get("metadata"))
    except:
        return JsonResponse( { "status":500, "error": "Unable to load posted metadata json" }, status=500 )

    #initialize database connection
    try:
        conn = psycopg2.connect( settings.catalogue_db )
        cur = conn.cursor()
        logger.debug( "Metadata database connection OK" )
    except Exception as e:
        return JsonResponse( { "status":500, "error": "Unable to establish connection with metadata database" }, status=500 )

    response = {}
    describeCoverageTmp = {}

    if files:
        for ff in files:
            dest = None
            original_identifier = None
            logger.info( "Start management of %s" %( files[ff].name ) )
            response[ ff ] = {}
            try:
                start = time.time()
                #::::::::::: 0) verify data :::::::::::
                if not ff in metadata:
                    raise KeyError( "key %s not found in metadata dict" %( ff ) )
                json_schema = json.load( open( METADATA_SCHEMA, 'r' ) )
                validator = jsonschema.Draft7Validator( json_schema )
                for error in validator.iter_errors( metadata[ ff ] ):
                    if error.validator == 'required':
                        raise ValidationError( error.message )
                    else:
                        path = list( error.path )[0]
                        raise ValidationError( "%s - %s" %( path, error.message ) )
                try:
                    identifier = metadata[ ff ][ "identifier" ]
                except:
                    logger.exception("CREATING NEW UUID")
                    identifier = str( uuid.uuid1() )
                landsat_row, landsat_path, wrs, sentinel_mgrs, mgrs, mwcs_grid = _getGrid( metadata[ ff ], cursor=cur )
                logger.debug( "0 - %s validation OK" %( files[ff].name,  ) )


                #::::::::::: 1) store dataset :::::::::::
                dest = os.path.join( settings.TMP_STORAGE_DIR, identifier )
                with open( dest, 'wb+' ) as destination:
                    for chunk in files[ff].chunks():
                        destination.write( chunk )
                response[ ff ][ "tmp_file" ] = dest
                logger.debug( "1 - %s upload OK" %( files[ff].name ) )


                #::::::::::: 2) read dataset ::::::::::::
                raster = ImageUtils( response[ ff ][ "tmp_file" ] )
                if _getKey( metadata[ ff ], "geometry", default="none") is not "none":
                    try:
                        raster.setPolygon( _getKey( metadata[ ff ], "geometry" ) )
                    except Exception as ex :
                        raise ValidationError( "geometry - unable to convetr geojson geometry" )
                logger.debug( "2 - %s raster validity ok" %( files[ff].name ) )

                #::::::::::: 3) check identifier and update ::::::::
                cur.execute( "SELECT identifier, mwcs_path FROM records WHERE identifier='%s'" %( identifier ) )
                row = cur.fetchone()
                if row is not None:
                    response[ ff ][ "status" ] = "UPDATE"
                    if row[1] is not None:
                        absolute_mwcs_path = os.path.join( settings.WCS_DIR, row[1] )
                        if os.path.isfile( absolute_mwcs_path ):
                            logger.debug( "backup existing raster %s for revert" %( absolute_mwcs_path ) )
                            raster_backup = absolute_mwcs_path + ".BCK"
                            os.rename( absolute_mwcs_path, raster_backup )
                    backup_identifier = str( uuid.uuid1() ) + "BCK"
                    cur.execute( "UPDATE records SET identifier='%s' WHERE identifier='%s'" %( backup_identifier, identifier ) )
                    conn.commit()
                    logger.debug( "Metadata backup. New_uid='%s' - Old_uid='%s'" %( backup_identifier, identifier ) )
                    original_identifier = identifier
                else:
                    response[ ff ][ "status" ] = "INSERT"
                logger.debug( "3 - %s identifier OK" %( files[ff].name ) )


                #::::::::::: 4) MWCS store :::::::::::::::
                mwcs_path = os.path.join(
                    metadata[ ff ][ 'datasetId' ],
                    mwcs_grid,
                    "Y%s" %( metadata[ ff ][ 'date' ][ 0:4 ] ),
                    "M%s" %( metadata[ ff ][ 'date' ][ 5:7 ] ),
                    "D%s" %( metadata[ ff ][ 'date' ][ 8:10 ] ),
                    "T%s" %( metadata[ ff ][ 'date' ][ 11:19 ].replace( ":", "") ),
                    "E%s" %( raster.AuthorityCode ),
                    raster.getMwcsGeom(),
                    identifier
                )
                response[ ff ][ "mwcs_path" ] = mwcs_path
                abs_mwcs_path = os.path.join( settings.WCS_DIR, mwcs_path )
                try:
                    os.makedirs( os.path.dirname( abs_mwcs_path ) )
                except OSError as exception:
                    if exception.errno != errno.EEXIST:
                        error_mess = "Unable to create directory %s" %( os.path.dirname( abs_mwcs_path ) )
                        logger.exception( error_mess )
                        raise OSError( error_mess )
                #:::::::::::::::::::::::::::::::: 4.1) upload data on s3 :::::::::::::::::::::::::::::::
                os.rename( response[ ff ][ "tmp_file" ], abs_mwcs_path )

                describeCoverageTmp = _updateDescribeCoverage( describeCoverageTmp, raster, metadata[ ff ] )
                logger.debug( "%s describe coverage update" %( abs_mwcs_path ) )
                del( response[ ff ][ "tmp_file" ] )
                logger.debug( "4 - %s mwcs OK [ %s ]" %( files[ff].name, abs_mwcs_path ) )


                #::::::::::: 5) pycsw catalogue :::::::::::
                typename = "csw:Record"
                schema = "http://www.opengis.net/cat/csw/2.0.2"
                mdsource = "local"
                insert_date = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
                xml = ""
                _type = "http://purl.org/dc/dcmitype/Dataset"
                title = _getKey( metadata[ ff ], "datasetId" )
                title_alternate = _getKey( metadata[ ff ], "title" )
                keywords = _getKey( metadata[ ff ], "keywords" )
                try:
                    time_begin = _getKey( metadata[ ff ], "timeExtent" ).split( "/" )[0]
                    time_end = _getKey( metadata[ ff ], "timeExtent" ).split( "/" )[1]
                except:
                    time_begin = ""
                    time_end = ""
                date = _getKey( metadata[ ff ], "date" )
                date_modified = _getKey( metadata[ ff ], "creationDate" )
                _format = "image/tiff"
                sourcerastergeo= _getSourceRasterGeo( metadata[ ff ], identifier )
                source=metadata[ff]["source"]
                wkt_geometry = raster.getPolygon()
                anytext = ' '.join( [ identifier, title, title_alternate, date, date_modified, time_begin, time_end ] + re.split( "[, \-!?:_]+", title_alternate ) + re.split( "[, \-!?:_]+", title ) )
                abstract_dict = _getGrid( metadata[ ff ],  of="dict" )
                abstract_dict[ "Identifier" ] = identifier
                abstract_dict[ "Data Type" ] = raster.datatype
                abstract_dict[ "Start" ] = time_begin
                abstract_dict[ "End" ] = time_end
                abstract = _getAbstract( abstract_dict )
                metadata[ff]["identifier"]=identifier
                metadata[ff]["minValue"]=raster.min
                metadata[ff]["maxValue"]=raster.max
                metadata[ff]["noDataValue"]=raster.nodata
                metadata[ff]["minDate"]=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(int(describeCoverageTmp["%s_%s"%(metadata[ ff ]["datasetId"],raster.coverageId())]["t_min"])))
                metadata[ff]["maxDate"]=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(int(describeCoverageTmp["%s_%s"%(metadata[ ff ]["datasetId"],raster.coverageId())]["t_max"])))
                metadata[ff]["EPSG"]=raster.AuthorityCode
                metadata[ff]["source"]=metadata[ff]["path"]
                del(metadata[ff]["path"])

                new_record = """
                INSERT INTO records ( identifier, typename, schema, mdsource, insert_date, xml, anytext, type,title, title_alternate, abstract, keywords, time_begin, time_end, date,date_modified, format, sourcerastergeo,source, wkt_geometry,landsat_row, landsat_path, wrs, sentinel_mgrs, mgrs,mwcs_path, json_metadata )
                VALUES ( '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', %s, %s, %s, %s, %s, '%s', '%s' )
                """%(
                    identifier, typename, schema, mdsource, insert_date, xml, anytext, _type, title, title_alternate, abstract, keywords, time_begin, time_end, date, date_modified, _format, sourcerastergeo,source, wkt_geometry,
                    landsat_row, landsat_path, wrs, sentinel_mgrs, mgrs,
                    mwcs_path, json.dumps( metadata[ ff ] )
                )
                cur.execute( new_record )
                conn.commit()
                logger.debug( '5 - Image "%s" registered successfully on catalogue with id "%s"' %( abs_mwcs_path, identifier ) )
                response[ ff ][ "wcs_url" ] = sourcerastergeo
                response[ ff ][ "identifier" ] = identifier
                response[ ff ][ "wcs_server" ] = settings.WCS_BASE_PATH

                #::::::::::::::::: 6) FINISH ::::::::::::
                if response[ ff ][ "status" ] == "UPDATE":
                    try:
                        os.remove( raster_backup )
                        logger.debug( "6 - remove old raster backup %s" %( raster_backup  ) )
                    except:
                        None
                    try:
                        cur.execute( "DELETE FROM records WHERE identifier='%s'" %( backup_identifier ) )
                        conn.commit()
                        logger.debug( "6 - remove old metadata from DB. old identifier=%s" %( backup_identifier ) )
                    except:
                        None
                logger.info( 'Registration of %s completed in %f s'%( files[ff].name, time.time() - start ) )

            except Exception as ex:
                logger.exception(ex)
                try:
                    os.remove( response[ ff ][ "tmp_file" ] )
                    logger.error( "REVERT - remove uploaded file %s" %( response[ ff ][ "tmp_file" ] ) )
                except:
                    None
                try:
                    os.rename( raster_backup, raster_backup.strip( ".BCK" ) )
                    logger.error( "REVERT - restore backuped raster %s" %( raster_backup.strip( ".BCK" ) ) )
                    response[ ff ][ "restored_mwcs_path" ] = raster_backup.strip( ".BCK" )
                except:
                    None
                try:
                    cur.execute( "DELETE FROM records WHERE identifier='%s';" %( original_identifier ) )
                    cur.execute( "UPDATE records SET identifier='%s' WHERE identifier='%s';" %( original_identifier, backup_identifier ) )
                    conn.commit()
                    logger.debug( "Metadata backup restored with identifier '%s'" %( original_identifier ) )
                    response[ ff ][ "restored_identifier" ] = original_identifier
                except:
                    None
                logger.exception( "File upload failed.")
                if 'mwcs_path' in response[ ff ]:
                    del( response[ ff ][ 'mwcs_path' ] )
                if 'identifier' in response[ ff ]:
                    del( response[ ff ][ 'identifier' ] )
                if 'wcs_url' in response[ ff ]:
                    del( response[ ff ][ 'wcs_url' ] )
                response[ ff ][ "error" ] = str( ex )
            response[ ff ][ "time" ] = time.time() - start

        conn.close()
    elif remote_files is not None:
        try:
            start = time.time()
            for feature_id in metadata_source:
                dest = None
                original_identifier = None
                logger.info( "Start management of %s" %( feature_id ) )
                response[ feature_id ] = {}
                json_schema = json.load( open( METADATA_SCHEMA, 'r' ) )
                validator = jsonschema.Draft7Validator( json_schema )
                for error in validator.iter_errors( metadata[ feature_id ] ):
                    if error.validator == 'required':
                        raise ValidationError( error.message )
                    else:
                        path = list( error.path )[0]
                        raise ValidationError( "%s - %s" %( path, error.message ) )

                try:
                    identifier = feature_id
                except:
                    identifier = str( uuid.uuid1() )
                logger.debug( "0 - %s validation OK" %( feature_id  ) )
                if metadata[feature_id]["geolocated"]:
                    landsat_row, landsat_path, wrs, sentinel_mgrs, mgrs, mwcs_grid = _getGrid( metadata[ feature_id ], cursor=cur )


                    raster = ImageUtils( metadata[feature_id]["source"] )
                    raster.setPolygon( metadata[feature_id]["geometry"] )
                    #::::::::::: 1) store dataset :::::::::::
                    dest = os.path.join( settings.TMP_STORAGE_DIR, identifier+".vrt" )
                    translateopt= gdal.TranslateOptions(format="VRT")
                    gdal.Translate(dest,metadata[feature_id]["source"],options=translateopt)
                    response[ feature_id ][ "tmp_file" ] = dest
                    logger.debug( "1 - %s upload OK" %( feature_id ) )

                    #::::::::::: 3) check identifier and update ::::::::
                    cur.execute( "SELECT identifier, mwcs_path FROM records WHERE identifier='%s'" %( identifier ) )
                    row = cur.fetchone()
                    if row is not None:
                        response[ feature_id ][ "status" ] = "UPDATE"
                        if row[1] is not None:
                            absolute_mwcs_path = os.path.join( settings.WCS_DIR, row[1] )
                            if os.path.isfile( absolute_mwcs_path ):
                                logger.debug( "backup existing raster %s for revert" %( absolute_mwcs_path ) )
                                raster_backup = absolute_mwcs_path + ".BCK"
                                os.rename( absolute_mwcs_path, raster_backup )
                        backup_identifier = str( uuid.uuid1() ) + "BCK"
                        cur.execute( "UPDATE records SET identifier='%s' WHERE identifier='%s'" %( backup_identifier, identifier ) )
                        conn.commit()
                        logger.debug( "Metadata backup. New_uid='%s' - Old_uid='%s'" %( backup_identifier, identifier ) )
                        original_identifier = identifier
                    else:
                        response[ feature_id ][ "status" ] = "INSERT"
                    logger.debug( "3 - %s identifier OK" %( feature_id ) )


                    #::::::::::: 4) MWCS store :::::::::::::::
                    mwcs_path = os.path.join(
                        metadata[ feature_id ][ 'datasetId' ],
                        mwcs_grid,
                        "Y%s" %( metadata[ feature_id ][ 'date' ][ 0:4 ] ),
                        "M%s" %( metadata[ feature_id ][ 'date' ][ 5:7 ] ),
                        "D%s" %( metadata[ feature_id ][ 'date' ][ 8:10 ] ),
                        "T%s" %( metadata[ feature_id ][ 'date' ][ 11:19 ].replace( ":", "") ),
                        "E%s" %( raster.AuthorityCode ),
                        raster.getMwcsGeom(),
                        "L_%s"%(metadata[feature_id]["subDatasetId"])
                    )
                    response[ feature_id ][ "mwcs_path" ] = mwcs_path
                    abs_mwcs_path = os.path.join( settings.WCS_DIR, mwcs_path,"" )
                    try:
                        os.makedirs( os.path.dirname( abs_mwcs_path ) )
                    except OSError as exception:
                        if exception.errno != errno.EEXIST:
                            error_mess = "Unable to create directory %s" %( os.path.dirname( abs_mwcs_path ) )
                            logger.exception( error_mess )
                            raise OSError( error_mess )
                    os.rename( response[ feature_id ][ "tmp_file" ], abs_mwcs_path+identifier+".vrt" )
                    describeCoverageTmp = _updateDescribeCoverage( describeCoverageTmp, raster, metadata[ feature_id ] )
                    logger.debug( "%s describe coverage update" %( abs_mwcs_path ) )
                    logger.debug( "4 - %s mwcs OK [ %s ]" %( feature_id, abs_mwcs_path ) )
                else:
                    cur.execute( "SELECT identifier FROM records WHERE identifier='%s'" %( identifier ) )
                    row = cur.fetchone()
                    if row is not None:
                        response[ feature_id ][ "status" ] = "UPDATE"
                        if row[0] is not None:
                            backup_identifier = row[0] + "BCK"
                            cur.execute( "UPDATE records SET identifier='%s' WHERE identifier='%s'" %( backup_identifier, identifier ) )
                            conn.commit()
                            logger.debug( "Metadata backup. New_uid='%s' - Old_uid='%s'" %( backup_identifier, identifier ) )
                            original_identifier = identifier
                    else:
                        response[ feature_id ][ "status" ] = "INSERT"
                    logger.debug( "3 - %s identifier OK" %( feature_id ) )
                    landsat_row="null"
                    landsat_path="null"
                    wrs="null"
                    sentinel_mgrs="null"
                    mgrs="null"
                    mwcs_grid="null"
                    mwcs_path="null"

                #::::::::::: 5) pycsw catalogue :::::::::::
                typename = "csw:Record"
                schema = "http://www.opengis.net/cat/csw/2.0.2"
                mdsource = "local"
                insert_date = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
                xml = ""
                _type = "http://purl.org/dc/dcmitype/Dataset"
                title = _getKey( metadata[ feature_id ], "datasetId" )
                title_alternate = _getKey( metadata[ feature_id ], "title_collection" )
                keywords = _getKey( metadata[ feature_id ], "keywords" )
                try:
                    time_begin = _getKey( metadata[ feature_id ], "timeExtent" ).split( "/" )[0]
                    time_end = _getKey( metadata[ feature_id ], "timeExtent" ).split( "/" )[1]
                except:
                    time_begin = ""
                    time_end = ""
                date = _getKey( metadata[ feature_id ], "date" )
                date_modified = _getKey( metadata[ feature_id ], "creationDate" )
                _format = "image/tiff"
                abstract_dict = _getGrid( metadata[ feature_id ],  of="dict" )
                abstract_dict[ "Identifier" ] = identifier

                abstract_dict[ "Start" ] = time_begin
                abstract_dict[ "End" ] = time_end
                if metadata[feature_id]["geolocated"]:
                    sourcerastergeo = _getSourceRasterGeo( metadata[ feature_id ], identifier ,)
                    source=metadata[feature_id]["source"]
                    wkt_geometry = raster.getPolygon()
                    abstract_dict[ "Data Type" ] = raster.datatype
                    metadata[feature_id]["minValue"]=raster.min
                    metadata[feature_id]["maxValue"]=raster.max
                    metadata[feature_id]["noDataValue"]=raster.nodata
                    metadata[feature_id]["EPSG"]=raster.AuthorityCode
                    metadata[feature_id]["minDate"]=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(int(describeCoverageTmp["%s_%s"%(metadata[ feature_id ]["datasetId"],raster.coverageId())]["t_min"])))
                    metadata[feature_id]["maxDate"]=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(int(describeCoverageTmp["%s_%s"%(metadata[ feature_id ]["datasetId"],raster.coverageId())]["t_max"])))
                else:
                    sourcerastergeo='null'
                    source=metadata[feature_id]["source"]
                    wkt_geometry =(ogr.CreateGeometryFromJson(metadata[feature_id]["geometry"])).ExportToWkt()
                    abstract_dict[ "Data Type" ] = None
                    metadata[feature_id]["minValue"]=None
                    metadata[feature_id]["maxValue"]=None
                    metadata[feature_id]["noDataValue"]=None
                    metadata[feature_id]["EPSG"]=None
                    metadata[feature_id]["minDate"]=metadata[feature_id]["date"]
                    metadata[feature_id]["maxDate"]=metadata[feature_id]["date"]

                anytext = ' '.join( [ identifier, title, title_alternate, date, date_modified, time_begin, time_end ] + re.split( "[, \-!?:_]+", title_alternate ) + re.split( "[, \-!?:_]+", title ) )

                abstract = _getAbstract( abstract_dict )
                del(metadata[feature_id]["meta"])
                del(metadata[feature_id]["umm"])
                if "wcs_val" in metadata[feature_id]:
                    del(metadata[feature_id]["wcs_val"])
                metadata[feature_id]["identifier"]=identifier

                new_record = """
                INSERT INTO records ( identifier, typename, schema, mdsource, insert_date, xml, anytext, type,
                                      title, title_alternate, abstract, keywords, time_begin, time_end, date,
                                      date_modified, format, sourcerastergeo, source, wkt_geometry,
                                      landsat_row, landsat_path, wrs, sentinel_mgrs, mgrs,
                                      mwcs_path, json_metadata )
                VALUES ( '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s', %s, %s, %s, %s, %s, '%s', '%s' )
                """%(
                    identifier, typename, schema, mdsource, insert_date, xml, anytext, _type, title, title_alternate, abstract, keywords, time_begin, time_end, date, date_modified, _format,sourcerastergeo, source, wkt_geometry,
                    landsat_row, landsat_path, wrs, sentinel_mgrs, mgrs,
                    mwcs_path, json.dumps( metadata[ feature_id ] )
                )
                cur.execute( new_record )
                conn.commit()
                logger.debug( '5 - Image registered successfully on catalogue with id "%s"' %( identifier ) )
                response[ feature_id ][ "wcs_url" ] = sourcerastergeo
                response[ feature_id ][ "identifier" ] = identifier
                response[ feature_id ][ "wcs_server" ] = settings.WCS_BASE_PATH

                #::::::::::::::::: 6) FINISH ::::::::::::
                #remove backups
                if response[ feature_id ][ "status" ] == "UPDATE":
                    try:
                        os.remove( raster_backup )
                        logger.debug( "6 - remove old raster backup %s" %( raster_backup  ) )
                    except:
                        None
                    try:
                        cur.execute( "DELETE FROM records WHERE identifier='%s'" %( backup_identifier ) )
                        conn.commit()
                        logger.debug( "6 - remove old metadata from DB. old identifier=%s" %( backup_identifier ) )
                    except:
                        None
                logger.info( 'Registration of %s completed in %f s'%( feature_id, time.time() - start ) )

        except Exception as ex:
            logger.exception(ex)
            response[ feature_id ][ "status" ] = "FAILED"
            try:
                os.remove( response[ feature_id ][ "tmp_file" ] )
                logger.error( "REVERT - remove uploaded file %s" %( response[ feature_id ][ "tmp_file" ] ) )
            except:
                None
            try:
                os.rename( raster_backup, raster_backup.strip( ".BCK" ) )
                logger.error( "REVERT - restore backuped raster %s" %( raster_backup.strip( ".BCK" ) ) )
                response[ feature_id ][ "restored_mwcs_path" ] = raster_backup.strip( ".BCK" )
            except:
                None
            try:
                cur.execute( "DELETE FROM records WHERE identifier='%s';" %( original_identifier ) )
                cur.execute( "UPDATE records SET identifier='%s' WHERE identifier='%s';" %( original_identifier, backup_identifier ) )
                conn.commit()
                logger.debug( "Metadata backup restored with identifier '%s'" %( original_identifier ) )
                response[ feature_id ][ "restored_identifier" ] = original_identifier
            except:
                None
            logger.exception( "File upload failed.")
            if 'mwcs_path' in response[ feature_id ]:
                del( response[ feature_id ][ 'mwcs_path' ] )
            if 'identifier' in response[ feature_id ]:
                del( response[ feature_id ][ 'identifier' ] )
            if 'wcs_url' in response[ feature_id ]:
                del( response[ feature_id ][ 'wcs_url' ] )
            response[ feature_id ][ "error" ] = str( ex )
        response[ feature_id ][ "time" ] = time.time() - start

    conn.close()

    _commitDescribeCoverage( describeCoverageTmp )

    response[ "status" ] = 200
    return JsonResponse( response )

def _getKey( metadata_json, key, default="" ):
    """
    try to extract key from jspn or return default"""
    try:
        return metadata_json[ key ]
    except:
        return default

def _getGrid( metadata_json, of = "string", cursor=None ):
    """
    extract mgrs or patwor grid for mwcs
    """
    landsat_row = "null"
    landsat_path = "null"
    wrs = "null"
    sentinel_mgrs = "null"
    mgrs = "null"
    mwcs_grid = ""

    if "tile" in metadata_json and metadata_json[ "tile" ].strip():
        if re.match( r'^[A-Z]{1}[0-9]{2}[A-Z]{3}$', metadata_json[ "tile" ] ):
            sentinel_mgrs = metadata_json[ "tile" ]
            zone = sentinel_mgrs[ 0:3 ]
            q    = sentinel_mgrs[ 3 ]
            x    = sentinel_mgrs[ 4 ]
            y    = sentinel_mgrs[ 5 ]
            if of == "dict":
                return { 'mgrs_tile': sentinel_mgrs }
            mwcs_grid = ( "%s/%s/%s/%s" %( zone, q, x, y ) ).lower()
            sentinel_mgrs="'%s'" %( sentinel_mgrs )
            cursor.execute( "SELECT id FROM sentinel_mgrs WHERE mgrs=%s" %( sentinel_mgrs ) )
            row = cursor.fetchone()
            if row is None:
                raise ValidationError( "tile - %s not found in DB. Is it a valid Sentinel tile?" %( metadata_json[ "tile" ] ) )
            else:
                mgrs = row[0]

        elif re.match( r'^[0-9]{6}$', metadata_json[ "tile" ] ):
            pathrow = metadata_json[ "tile" ]
            landsat_path = int( pathrow[ 0:3 ] )
            landsat_row = int( pathrow[ 3: ] )
            if of == "dict":
                return{ 'Path': landsat_path, 'Row': landsat_row }
            mwcs_grid = "%d/%d" %( landsat_path, landsat_row )
            cursor.execute( "SELECT id FROM landsat_wrs WHERE path=%d and row=%d" %( landsat_row, landsat_path ) )
            row = cursor.fetchone()
            if row is None:
                raise ValidationError( "tile - %s not found in DB. Is it a valid Landsat tile?" %( metadata_json[ "tile" ] ) )
            else:
                wrs = row[0]
        else:
            raise ValidationError( "tile - %s is not a supported tiling schema" %( metadata_json[ "tile" ] ) )
    else:
        if metadata_json["grid"]:
            if "plan_value" in metadata_json and "image_value" in metadata_json:
                landsat_row = metadata_json["plan_value"]
                landsat_path = metadata_json["image_value"]
                mwcs_grid = "%d/%d" %( int(landsat_path), int(landsat_row ))
                if of == "dict":
                    return {"subset":"gfix(%s,%s)"%(landsat_row,landsat_path)}
            if "wcs_val" in metadata_json and "SubRegion_value" in metadata_json and "Product_value" in metadata_json:
                mwcs_grid = "%s/%s/%s" %( metadata_json["wcs_val"],metadata_json["SubRegion_value"],metadata_json["Product_value"])
                if of == "dict":
                    return {"subset":"gfix(%s,%s,%s)"%(metadata_json["wcs_val"],metadata_json["SubRegion_value"],metadata_json["Product_value"])}
    if of=="dict":
        return {}
    return landsat_row, landsat_path, wrs, sentinel_mgrs, mgrs, mwcs_grid

def _getSourceRasterGeo( metadata_json, identifier ):
    """
    return the wcs url without the host and the base path
    """
    src = 'service=WCS&Request=GetCoverage&version=2.0.0&subset=t(%d)&CoverageId=%s&format=image/tiff&filename=%s&subdataset=%s' %(
        _date_to_unix( metadata_json[ "date" ] ),
        metadata_json[ "datasetId" ],
        identifier,
        metadata_json["subDatasetId"]
    )
    grid_parameters = _getGrid( metadata_json,  of="dict" )
    if grid_parameters:
        for key in grid_parameters:
            new_query_attr = "&%s=%s" %( key.lower(), str( grid_parameters[ key ] ) )
            src += new_query_attr
    return src

def _getAbstract( abstract_dict ):
    table = '<table><tbody>'
    for key in abstract_dict:
        table += '<tr><td><strong>%s</strong></td><td>%s</td></tr>' %( key, str( abstract_dict[ key ] ) )
    table += '</tbody></table>'
    return table

def _date_to_unix( raster_date_string ):
    """
    convert string date to unix time
    """
    raster_date = datetime.datetime.strptime( raster_date_string, '%Y-%m-%dT%H:%M:%SZ')
    origin      = datetime.datetime( 1970, 1, 1, 0, 0, 0, 0 )
    unix_date   = raster_date - origin
    return unix_date.days * 24 * 60 * 60 + unix_date.seconds

def _updateDescribeCoverage( describeCoverageTmp, image_utils, metadata_json ):
    coverageName = "%s_%s" %( metadata_json[ 'datasetId' ], image_utils.coverageId() )
    if not coverageName in describeCoverageTmp:
        describeCoverageTmp[ coverageName ] = {
            "GeoX_lr": image_utils.lower_right_x,
            "GeoY_lr": image_utils.lower_right_y,
            "GeoX_ul": image_utils.up_left_x,
            "GeoY_ul": image_utils.up_left_y,
            "pxAOISizeX": image_utils.size_x,
            "pxAOISizeY": image_utils.size_y,
            "t_min": _date_to_unix( metadata_json[ 'date' ] ),
            "t_max": _date_to_unix( metadata_json[ 'date' ] ),
            "hit_number": 1,
            "coverageName": coverageName,
            "collectionName": metadata_json[ 'datasetId' ],
            "coverageId": image_utils.coverageId(),
            "EPSG": image_utils.AuthorityCode,
            "size_z": image_utils.size_z,
            "resolution": round( image_utils.resolution, 12 ),
            "resolution2": round( image_utils.resolution2, 12 ),
            "datatype_number": image_utils.datatype_number,
            "size": image_utils.getSize()
    }
    else: 
        describeCoverageTmp[ coverageName ][ "GeoX_lr" ] = max( describeCoverageTmp[ coverageName ][ "GeoX_lr" ], image_utils.lower_right_x )
        describeCoverageTmp[ coverageName ][ "GeoY_lr" ] = min( describeCoverageTmp[ coverageName ][ "GeoY_lr" ], image_utils.lower_right_y )
        describeCoverageTmp[ coverageName ][ "GeoX_ul" ] = min( describeCoverageTmp[ coverageName ][ "GeoX_ul" ], image_utils.up_left_x )
        describeCoverageTmp[ coverageName ][ "GeoY_ul" ] = max( describeCoverageTmp[ coverageName ][ "GeoY_ul" ], image_utils.up_left_y )
        describeCoverageTmp[ coverageName ][ "pxAOISizeX" ] = int( ( describeCoverageTmp[ coverageName ][ "GeoX_lr" ] - describeCoverageTmp[ coverageName ][ "GeoX_ul" ] ) / image_utils.resolution )
        describeCoverageTmp[ coverageName ][ "pxAOISizeY" ] = int( ( describeCoverageTmp[ coverageName ][ "GeoY_ul" ] - describeCoverageTmp[ coverageName ][ "GeoY_lr" ] ) / image_utils.resolution )
        describeCoverageTmp[ coverageName ][ "t_min" ] = min( describeCoverageTmp[ coverageName ][ "t_min" ], _date_to_unix( metadata_json[ 'date' ] ) )
        describeCoverageTmp[ coverageName ][ "t_max" ] = max( describeCoverageTmp[ coverageName ][ "t_max" ], _date_to_unix( metadata_json[ 'date' ] ) )
        describeCoverageTmp[ coverageName ][ "hit_number" ] += 1
        try: 
            describeCoverageTmp[ coverageName ][ "size" ] += image_utils.getSize()
        except Exception as ex:
            describeCoverageTmp[ coverageName ][ "size" ] = image_utils.getSize()
    return describeCoverageTmp

def _commitDescribeCoverage( describeCoverageTmp ):
    """
    update DescribeCoverage.json
    """
    for key in describeCoverageTmp:
        commit_start = time.time()
        dc = describeCoverageTmp[ key ]

        json_describe_coverage_file = os.path.join( settings.WCS_DIR, dc[ "collectionName" ], "DescribeCoverage.json" )

        lock_fd = _acquireLock( json_describe_coverage_file )
        try:
            infomess = "Try to update existing DC file %s" %( json_describe_coverage_file )
            logger.debug( infomess )
            dc_json = json.load( lock_fd )
        except:
            #create new dc
            infomess = "Valid DC file %s not found, create new dc file" %( json_describe_coverage_file )
            logger.debug( infomess )
            dc_json = { "Collections": [] }

        updated = False
        for coverage in dc_json[ "Collections" ]:
            if coverage[ "name" ] == dc[ "coverageName" ]:
                infomess = "Coverage %s found in DC file %s" %( dc[ "coverageName" ], json_describe_coverage_file )
                logger.debug( infomess )
                coverage[ "GeoX_lr" ] = max( coverage[ "GeoX_lr" ], dc[ "GeoX_lr" ]  )
                coverage[ "GeoY_lr" ] = min( coverage[ "GeoY_lr" ], dc[ "GeoY_lr" ]  )
                coverage[ "GeoX_ul" ] = min( coverage[ "GeoX_ul" ], dc[ "GeoX_ul" ] )
                coverage[ "GeoY_ul" ] = max( coverage[ "GeoY_ul" ], dc[ "GeoY_ul" ] )
                coverage[ "pxAOISizeX" ] = max( coverage[ "pxAOISizeX" ], dc[ "pxAOISizeX" ] )
                coverage[ "pxAOISizeY" ] = max( coverage[ "pxAOISizeY" ], dc[ "pxAOISizeY" ] )
                coverage[ "t_min" ] = min( coverage[ "t_min" ], dc[ "t_min" ] )
                coverage[ "t_max" ] = max( coverage[ "t_max" ], dc[ "t_max" ] )
                coverage[ "hit_number" ] += dc[ "hit_number" ]
                updated = True
                break
        if not updated:
            coverage = {}
            infomess = "Coverage %s not found in DC file %s" %( dc[ "coverageName" ], json_describe_coverage_file )
            coverage[ "name" ] = dc[ "coverageName" ]
            coverage[ "id" ]   = dc[ "coverageId" ]
            coverage[ "GeoX_lr" ] = dc[ "GeoX_lr" ]
            coverage[ "GeoY_lr" ] = dc[ "GeoY_lr" ]
            coverage[ "GeoX_ul" ] = dc[ "GeoX_ul" ]
            coverage[ "GeoY_ul" ] = dc[ "GeoY_ul" ]
            coverage[ "pxAOISizeX" ] = dc[ "pxAOISizeX" ]
            coverage[ "pxAOISizeY" ] = dc[ "pxAOISizeY" ]
            coverage[ "t_min" ] = dc[ "t_min" ]
            coverage[ "t_max" ] = dc[ "t_max" ]
            coverage[ "hit_number" ] = dc[ "hit_number" ]
            coverage[ "nband" ] = dc[ "size_z" ]
            coverage[ "EPSG" ] = dc[ "EPSG" ]
            coverage[ "x_res" ] = "%.12f".rstrip("0") %( round( dc[ "resolution" ], 12 ) )
            coverage[ "y_res" ] = "%.12f".rstrip("0") %( round( dc[ "resolution2" ], 12 ) )
            coverage[ "type" ] = dc[ "datatype_number" ]
            coverage[ "bands" ] = []
            for i in range( 0, dc[ "size_z" ] ):
                new_band = {}
                new_band[ "color_interpretation" ] = "Undefined"
                new_band[ "data_type" ] = dc[ "datatype_number" ]
                new_band[ "definition" ] = ""
                new_band[ "identifier" ] = ""
                new_band[ "name" ] = ""
                new_band[ "nil_values" ] = { "reason": "http:\/\/www.opengis.net\/def\/nil\/OGC\/0\/unknown", "value": "0" }
                new_band[ "uom" ] = ""
                new_band[ "value_max" ] = ""
                new_band[ "value_min" ] = ""
                coverage[ "bands" ].append( new_band )
            dc_json[ "Collections" ].append( coverage )

        lock_fd.seek(0)
        json.dump( dc_json, lock_fd )
        lock_fd.truncate()
        _releaseLock( lock_fd )

        _updateVirtualDc( dc[ "collectionName" ] )
        commit_stop = time.time()
        logger.info( 'DescribeCoverage "%s" updated in %f seconds' %( json_describe_coverage_file, commit_stop - commit_start))

def _updateVirtualDc( collectionName ):
    """
    remove describe coverage file for virtual collection
    """
    for coll in os.listdir( settings.WCS_DIR ):
        abs_coll = os.path.join( settings.WCS_DIR, coll )
        if os.path.isdir( abs_coll ):
            for el in os.listdir( abs_coll ):
                abs_el = os.path.join( abs_coll, el )
                if os.path.islink( abs_el ) and el == collectionName:
                    try:
                        virtualDescribe = os.path.join( abs_coll, "DescribeCoverage.json" )
                        os.remove( virtualDescribe )
                        logger.info( 'Remove DescribeCoverage for virtual collection "%s" after uptate of "%s"' %( coll, collectionName ) )
                    except:
                        None


def _acquireLock( filename ):
    ''' acquire exclusive lock file access '''
    try:
        locked_file_descriptor = open( filename, 'r+')
    except:
        locked_file_descriptor = open( filename, 'w+')
    fcntl.lockf(locked_file_descriptor, fcntl.LOCK_EX)
    infomess = "Acquire lock for file %s" %( filename )
    logger.debug( infomess )
    return locked_file_descriptor

def _releaseLock( locked_file_descriptor ):
    ''' release exclusive lock file access '''
    locked_file_descriptor.close()


class ImageUtils():
    def __init__( self, filename ):
        """
        utility class used to extract data from raster
        """
        self.filename = filename
        self.polygon = None
        self.open()

    def open( self ):
        
        dataset = gdal.Open( self.filename )
        try:
            if dataset.GetSubDatasets():
                raise Exception( "Multidataset rasters are not supported. Consider to use GDAL VRT for this kind of data." )
        except:
            None

        if dataset != None:
            try:
                band         = dataset.GetRasterBand(1)
                geotransform = dataset.GetGeoTransform()

                self.size_x = dataset.RasterXSize
                self.size_y = dataset.RasterYSize
                self.size_z = dataset.RasterCount
                self.AuthorityCode = self.getAuthorityCode( dataset )
                if self.AuthorityCode == None:
                    raise Exception( "georef is not correct so the dataset can not be considered valid" )
                self.resolution    = geotransform[1]
                self.resolution2   = geotransform[5]
                self.up_left_x     = geotransform[0]
                self.up_left_y     = geotransform[3]
                self.datatype      = gdal.GetDataTypeName( band.DataType )
                self.datatype_number = band.DataType
                self.width  = self.size_x * self.resolution
                self.height = self.size_y * self.resolution2
                self.lower_right_x = self.up_left_x + self.width
                self.lower_right_y = self.up_left_y + self.height
                stats=band.GetStatistics(True,True)
                self.min=stats[0]
                self.max=stats[1]
                self.nodata=str(band.GetNoDataValue())

                self.isValid = True
            except Exception as ex:
                dataset = None

        if dataset is None:
            raise Exception( "GDAL error, unable to read raster %s" %( self.filename ) )
        dataset = None

    def getAuthorityCode( self, dataset ):
        wgssr = osr.SpatialReference()
        wgssr.ImportFromWkt( dataset.GetProjectionRef() )
        wgssr.AutoIdentifyEPSG()
        authority_code = wgssr.GetAuthorityCode(None)
        return authority_code

    def getMwcsGeom( self , of= None):
        if of is None:
            resolution_str  = "%.12f".rstrip("0") %( round( self.resolution, 12 ) )
            resolution2_str = "%.12f".rstrip("0") %( round( self.resolution2, 12 ) )
            return "G%fx%f_%sx%s_%dx%dx%d_%d" %( self.up_left_x, self.up_left_y, resolution_str, resolution2_str, self.size_x, self.size_y, self.size_z, self.datatype_number )
        else:
            resolution_str  = "%.12f".rstrip("0") %( round( self.resolution, 12 ) )
            resolution2_str = "%.12f".rstrip("0") %( round( self.resolution2, 12 ) )
            return "G%fx%f_%sx%s_%dx%dx%d_%d" %( self.up_left_x, self.up_left_y, resolution_str, resolution2_str, self.size_x, self.size_y, self.size_z, self.datatype_number )


    def getPolygon( self ):
        """
        check if polygon is defined
        """
        if self.polygon is not None:
            return self.polygon
        polygon = "POLYGON(( %f %f, %f %f, %f %f, %f %f, %f %f ))" %(
            self.convert( self.up_left_x, self.up_left_y ) +
            self.convert( self.lower_right_x,self.up_left_y ) +
            self.convert( self.lower_right_x,self.lower_right_y ) +
            self.convert( self.up_left_x, self.lower_right_y ) +
            self.convert( self.up_left_x, self.up_left_y )
        )
        return polygon

    def setPolygon( self, geojson_geom=None ):
        if geojson_geom is not None:
            try:
                geom = ogr.CreateGeometryFromJson( str( geojson_geom ) )
                self.polygon = geom.ExportToWkt()
            except Exception as e:
                print(e)

    def convert( self, x1, y1 ):
        inProj = 'EPSG:%s' %( self.AuthorityCode )
        transformer = Transformer.from_crs( inProj, 'EPSG:4326', always_xy=True )
        for pt in transformer.itransform( [ ( x1, y1 ) ] ):
             x2, y2 = pt
        return x2, y2

    def coverageId( self ):
        resolution_str = format( round( self.resolution, 6 ), 'f' ).split(".")[0] + format( round( self.resolution, 6 ), 'f' ).split(".")[1].rstrip("0")
        return "%s_%s" %( self.AuthorityCode, resolution_str )

    def getSize( self ):
        size_byte = self.size_x * self.size_y * 4
        return size_byte
