import time
import sys
import os
import re
import json
from datetime import datetime, timedelta, timezone, date
import pymongo
from collections import OrderedDict
from settings import connection, LOCK_DIR, PAUSE_LOCK , STATUSLOADER_DIR, DATASETID_COLLECTION_SCHEMA,MONGODB_DBNAME,connection_admin,WCSPATH_ENDPOINT
import subprocess
from multiprocessing.pool import ThreadPool
from multiprocessing import Lock
from osgeo import gdal, ogr, osr
import fcntl
from pyproj import Proj, transform
import requests
from pathlib import Path
import traceback
from loader_modules.exceptions import MongoLoaderException
import random
from osgeo import gdal

from virtuaload import Virtualoader




class MongoLoader:
    def __init__(self,cmd_args,logger):
        self.args = cmd_args
        self.logger = logger
        #check if script was run via command line ( or ssh or crontab or ...)
        self.progress_bar = sys.stdin.isatty()
        self.lock=Lock()
        self.outProj=Proj( 'epsg:4326' )
        self.start_timer = 0
        self.start_registration = 0
        self.discovered_counter = 0
        self.loaded_counter = 0
        self.skipped_products_counter = 0
        self.loaded_products_counter = 0
        self.commit_counter=0
        self.error_list=[]
        self.DPSperc=0
        self.pool = None
        self.stop = self.args.stop

        #mongo connection
        self.mongo=pymongo.MongoClient(connection)
        self.db=self.mongo[ MONGODB_DBNAME ]
        self.mongo_admin=pymongo.MongoClient(connection_admin)
        self.db_admin=self.mongo_admin[ MONGODB_DBNAME ]
        self.local_datasets_list={}

        #analytics vars
        self.ingestionAnalytics = {}
        self.ingestionDate=datetime.now().replace(microsecond=0).replace(tzinfo=None)


        self._checkLockDir()

        self._chechArgs()
        self.descriptionDoc = self._checkDescriptionDocument()

        gdal.PushErrorHandler( 'CPLQuietErrorHandler' ) #disable gdal error
        gdal.SetConfigOption( 'GDAL_PAM_ENABLED', 'NO' ) #disable gdal creation .aux.xml file
        gdal.SetConfigOption( 'GDAL_HTTP_TIMEOUT', '60' ) #sety gdal timeout to prevent s3 lock
        gdal.UseExceptions()

    def _chechArgs( self ):
        """
        Check specific module requirements
        """
        if self.args.source is None or ( not os.path.isfile( self.args.source ) and not os.path.isdir( self.args.source ) ):
            raise ValueError( 'Source "%s" not specified or not valid' % ( self.args.source ) )

    def _checkLockDir(self):
        """
        Check if exists directory for lock file
        """
        if not os.path.isdir( LOCK_DIR ):
            os.mkdir( LOCK_DIR )
        else:
            self.logger.info("Lock Directory already exists")

    def _checkDescriptionDocument(self):
        try:
            self.logger.info("Description document loaded from file")
            with open(self.args.descriptionDocument,"r") as jf:
                data=json.load(jf)
            return data
        except Exception as e:
            traceback.print_exc()
            self.logger.info("Description document loaded from args")
            return json.loads(self.args.descriptionDocument)

    def start( self ):
        """
        main method that implement ingestion chain
        """
        self.start_timer = time.time()
        self.logger.info( 'Loader with key "%s" started' %( self.__class__.getKey() ) )
        file_list=self.getList()
        self.start_registration = time.time()
        self.discovered_counter = len( file_list )
        if self.discovered_counter > 0:
            self.pool = ThreadPool( processes = self.args.th_number )
            processes = []
            for filename_complete in file_list:
                processes.append( self.pool.apply_async( self._load, args=( filename_complete, ) ) )
            for (j,p) in enumerate(processes):
                res = p.get()
                if self.stop is not None or os.path.isfile("/tmp/%s_STOP.lock"%(self.args.mid)):
                    self.pool.terminate()
                    self.pool.join()
                    break
                self._print( self.loaded_counter, len( file_list ) )
            #all ThreadPool finished
            self.pool.close()
            self.pool.join()
            self.commit( last = True )
            setattr(self.args,"dataset",None)
            #if self.args.pipelinelevel == "processing":
            #    Virtualoader(self.args,self.logger).start()
            self.mongo.close()
        self.logger.info( 'Loader with key "%s" completed' %( self.__class__.getKey() ) )

    def getList( self ):
        """
        Search raster on self.args.source
        """
        filelist = []
        regular_expression = re.compile( r'%s' %( self.args.pattern ) )
        if os.path.isfile( self.args.source ) and regular_expression.match( self.args.source ):
            filelist.append( self.args.source )
        elif os.path.isdir( self.args.source ):
            for root, dirnames, filenames in os.walk( self.args.source ):
                for filename in filenames:
                    abs_filename = os.path.abspath(os.path.join(root,filename))
                    if regular_expression.match( filename ):
                        filelist.append( abs_filename )
            self.logger.info( '%d raster found on "%s"' %( len( filelist ), self.args.source ) )
        return filelist

    def _load( self, filename ):
        """
        Do not overwrite it modify the pubblic load() method
        this method provide common operation like verify lock and pause/activate ingestion
        """
        while os.path.isfile( PAUSE_LOCK+".lock" ):
            self.logger.info( "...Loader paused, wait for %s to be removed ..." %( PAUSE_LOCK+".lock" ) )
            time.sleep( 2 )
        while os.path.isfile( PAUSE_LOCK+"_"+self.args.mid+".lock" ):
            self.logger.info( "...Loader paused, wait for %s to be removed ..." %( PAUSE_LOCK+"_"+self.args.mid+".lock" ) )
            time.sleep( 2 )

        if os.path.isfile("/tmp/%s_STOP.lock"%(self.stop)):
            sleep_period = random.randint( 10, 20 )
            time.sleep( sleep_period )
        try:
            self.load( filename )
            self.loaded_counter+=1
            self.commit_counter+=1
        except (RuntimeError,Exception) as e:
            date_time=datetime.now().replace(microsecond=0).replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%SZ")
            self.error_list.append({'filename':filename,'exception':date_time+" "+str(type(e))})
            self.logger.exception(e)
            if self.args.onError == "strict":
                self.stop=self.args.mid
                Path("/tmp/%s_STOP.lock"%(self.args.mid)).touch()

    def load(self,filename):
        """
        1- creo,se non presente, collection con collection_name=datasetId
        2- check band/subdataset con return della struttura json i.e.: ["subDatasetId":{subdatsetPointer:"---"},.........,"subDatasetIdn"{}]
        3- inserisco records,temporalbar,ingestion analytics
        4- aggiorno struttura collection datasets per il commit
        """
        load_timer = time.time()
        datasetId = self._getDatasetName( filename )
        self._checkCollection( datasetId )
        item_structure_list = self.checkBandSubDset( filename , datasetId )
        duplicate_test = self.checkDuplicates( filename )
        if duplicate_test [ "action" ] == "skip" :
            self.logger.info( 'Registration of "%s" skipped in %f s' %( filename , time.time() - load_timer ) )
        else:
            time.sleep( 0.01 )
            self.commit() #TODO
            unique_index = duplicate_test[ 'unique_index' ]
            self.logger.info( 'Start registration of "%s"' %( filename ) )
            feature_time = time.time()
            document = self.db[datasetId].find_one(unique_index)
            rec_doc = self._createDictInsertRecord( filename , item_structure_list , datasetId ,document)
            try:
                record_id = self.db[ datasetId ].update_one( unique_index , { '$set' : rec_doc } , upsert = True )
            except pymongo.errors.WriteError as we:
                self.logger.exception( str( we ))
                if "extract geo keys" in str( we ) :
                    wrong_geom = ogr.CreateGeometryFromJson( json.dumps( rec_doc[ 'geometry' ] ) )
                    rec_bbox= self._getEnvelope( wrong_geom )
                    rec_bbox.FlattenTo2D()
                    rec_doc.update( { "geometry" : json.loads( rec_bbox.ExportToJson() ) } )
                    record_id = self.db[datasetId].update_one( unique_index , { '$set' : rec_doc } , upsert = True )
                    self.logger.debug( "Recovery record geometry success" )
                else:
                    rec_doc = {}
            #datasets
            dataset_doc = self.db.datasets.find_one({"datasetId":datasetId})
            rec_dat=self._createDictInsertDataset(item_structure_list,datasetId,filename,dataset_doc)
            self._createDatasetList(rec_dat)
            feature_time_end= time.time()-feature_time
            #temporalbar & ingestionAnalytics
            temporalbar_id=self.insertTemporalBar(datasetId , rec_doc['productDate'])
            for (k,v) in item_structure_list[datasetId].items():
                self.ingestionAnalyticsList( datasetId , filename , feature_time_end , k)

        self.logger.info( 'Registration of "%s" completed in %f s' %( filename, time.time() - load_timer ) )
        self.loaded_products_counter += 1
    
    def _createDatasetList(self,rec_dat):
        with self.lock:
            if rec_dat['datasetId'] in self.local_datasets_list:
                self.local_datasets_list[rec_dat['datasetId']]['updateDate']=rec_dat['creationDate']
                self.local_datasets_list[ rec_dat[ "datasetId" ] ][ "minDate" ] = self._checkMinDate(self.local_datasets_list[rec_dat['datasetId']][ 'minDate' ],rec_dat[ 'minDate' ])
                self.local_datasets_list[ rec_dat[ "datasetId" ] ][ "maxDate" ] = self._checkMaxDate(self.local_datasets_list[rec_dat['datasetId']][ 'maxDate' ],rec_dat[ 'maxDate' ])
                #subdatasets update
                for sdb in rec_dat[ "subDatasets" ]:
                    if sdb in self.local_datasets_list[ rec_dat[ "datasetId" ] ][ "subDatasets" ]:
                        if self.args.pipelinelevel == "processing":
                            self.local_datasets_list[ rec_dat[ "datasetId" ] ][ "subDatasets" ][ sdb ][ "minValue" ] = self._checkMin(rec_dat[ "subDatasets" ][ sdb ][ 'minValue' ],self.local_datasets_list[rec_dat['datasetId']][ "subDatasets" ][ sdb ][ 'minValue' ])
                            self.local_datasets_list[ rec_dat[ "datasetId" ] ][ "subDatasets" ][ sdb ][ "maxValue" ] = self._checkMax(rec_dat[ "subDatasets" ][ sdb ][ 'maxValue' ],self.local_datasets_list[rec_dat['datasetId']][ "subDatasets" ][ sdb ][ 'maxValue' ])
                    else:
                        self.local_datasets_list[ rec_dat[ "datasetId" ] ][ "subDatasets" ].update({sdb:rec_dat[ "subDatasets" ][ sdb ]})
            else:
                self.local_datasets_list[rec_dat['datasetId']]=rec_dat.copy()
    
    def _getEnvelope(self , geom):
        env = geom.GetEnvelope()
        return ogr.CreateGeometryFromJson( json.dumps({"type": "Polygon" , "coordinates": [[[ env[0] , env[2] ],[ env[1] , env[2] ],[ env[1] , env[3] ],[ env[0] , env[3] ],[ env[0] , env[2] ]]]}))

    def checkDuplicates(self,filename):
        unique_index = {
            'productId':self._getProductId( filename ),
            'productDate':self._getDate( filename )
        }
        action = 'load'
        if not self.args.reload and self.db[self._getDatasetName( filename )].count_documents( unique_index ) > 0:
            self.skipped_products_counter += 1
            action = 'skip'
        return {
            'action': action,
            'unique_index':  unique_index
        }


    def _checkCollection(self, datasetId):
        """
        1- get list of  collections
        2- create new one if datasetId name does not exist
        3 create collection indexes
        """
        try:
            collections_list=self.db_admin.list_collection_names()
            if datasetId not in collections_list:
                #create collection
                self.db_admin.create_collection(datasetId)
                #setup collection validator
                validator=DATASETID_COLLECTION_SCHEMA
                cmd=OrderedDict([('collMod', datasetId),('validator', validator),('validationLevel', 'moderate')])
                self.db_admin.command(cmd)
                #create indexex
                index1=pymongo.IndexModel([("productDate",pymongo.ASCENDING)])
                index2=pymongo.IndexModel([("geometry",pymongo.GEOSPHERE)])
                index3=pymongo.IndexModel([("productDate",pymongo.ASCENDING),("productId",pymongo.ASCENDING)])
                self.db_admin[datasetId].create_indexes([index1,index2,index3])
        except Exception as e:
            self.logger.info("Collection created or already created")
    
    def _getProductId(self, filename):
        """
        Return an unique Product ID, for example the filename.[required for records collection]
        """
        return os.path.splitext(os.path.basename( filename ))[0]

    def _getDatasetName( self, filename ):
        """
        Return dataset name as the name of the file before the date witout the abs path
        """
        file_no_date = self.descriptionDoc["datasetId"] if "datasetId" in self.descriptionDoc else re.split(r'(\d{8,14}([\.|_|T]\d{6})?)', os.path.basename( filename ) )[0].rstrip('A').strip('_')
        return file_no_date

    def checkBandSubDset(self,filename,datasetId):
        """
        1- check if the filename has multiband or subdatasets
        2- return a structure like this: {"datasetId":{"subDatasetId":{subDatasetId infos},.....,"subDatasetIdn":{subDatasetIdn infos}}}
        """
        item_structure={}
        item_structure[datasetId]={}
        gdal_opt = gdal.InfoOptions(format="json",stats=True,computeMinMax=True)
        gdal_info= gdal.Info(filename,options=gdal_opt)
        if "bands" in gdal_info and len(gdal_info["bands"])>0 :
            for index,band in enumerate(gdal_info["bands"]):
                subdatasetId = self._getSubDatasetId(filename)
                item_structure[datasetId].update(self.createSubDatasetStructure(filename,gdal_info,subdatasetId,index))
            item_structure["gdalinfo"]=gdal_info

        return item_structure

    def _getSubDatasetId(self,filename):
        return os.path.basename(filename).split("_")[4]

    def createSubDatasetStructure( self , filename , gdalinfo , subDatasetId ,index):
        item = {}
        item[ subDatasetId ] = { }
        item[ subDatasetId ]["datasets"] = {}
        item[ subDatasetId ]["records"] = {}
        item[ subDatasetId ]["datasets"][ "minValue" ] = [gdalinfo["bands"][index]["minimum"]]
        item[ subDatasetId ]["datasets"][ "maxValue" ] = [gdalinfo["bands"][index]["maximum"]]
        item[ subDatasetId ]["datasets"][ 'noDataValue' ] = gdalinfo['bands'][index]['noDataValue'] if "noDataValue" in gdalinfo['bands'][index] else None
        item[ subDatasetId ]["datasets"][ 'numberOfRecords' ] = 1
        item[ subDatasetId ]["datasets"][ 'units' ] = self._getUnit()
        item[ subDatasetId ]["datasets"][ 'unitsDescription' ] = self._getUnitDescription()
        item[ subDatasetId ]["datasets"][ 'Resolution' ] = [self._getResolution(gdalinfo)]
        item[ subDatasetId ]["records"][ "productPath" ] = self._getCachePath( filename )
        item[ subDatasetId ]["records"][ "band" ] = str( index )
        item[ subDatasetId ]["records"][ "mwcs" ] = self._getMWcsData( filename , gdalinfo )
        if "offset" in gdalinfo["bands"][index]:
            item[ subDatasetId ]["records"][ "offsetData"] = self._getOffsetData(gdalinfo)
        if "scale" in gdalinfo["bands"][index]:
            item[ subDatasetId ]["records"][ "scale"] = self._getScaleData(gdalinfo)
        item[ subDatasetId ]["records"][ "minValue" ] = gdalinfo["bands"][index]["minimum"]
        item[ subDatasetId ]["records"][ "maxValue" ] = gdalinfo["bands"][index]["maximum"]
        item[ subDatasetId ]["records"][ 'dataType' ] = gdalinfo["bands"][index]["type"]
        item[ subDatasetId ]["records"][ 'noDataValue' ] = gdalinfo['bands'][index]['noDataValue'] if "noDataValue" in gdalinfo['bands'][index]else None 

        return item

    def _getOffsetData(self,ginfo):
        """
        Return the offset
        """
        offset = float(ginfo["bands"][0]["offset"])
        return offset

    def _getScaleData(self,ginfo):
        """
        Return the scale
        """
        scale = float(ginfo["bands"][0]["scale"])
        return scale


    def _getMWcsData(self,filename,gdal_info):
        """
        Return a string containing element for MWCS
        """
        band=len(gdal_info['bands'])
        geo_transf=gdal_info['geoTransform']
        size=gdal_info['size']
        dt=gdal.Open(filename)
        ty_pe=dt.GetRasterBand(1).DataType
        epsg=gdal_info["coordinateSystem"]["wkt"].rsplit('ID["EPSG",', 1)[-1].strip("]") if not self.args.epsg else self.args.epsg
        if epsg==None:
            epsg=-1
        srs=osr.SpatialReference()
        srs.ImportFromEPSG(int(epsg))
        wkt=srs.ExportToProj4()
        mwcs_string="EPSG:"+str(epsg)+";"+wkt.rstrip()+";"+str(geo_transf[0])+" "+str(geo_transf[1])+" "+str(geo_transf[2])+" "+str(geo_transf[3])+" "+str(geo_transf[4])+" "+str(geo_transf[5])+" "+str(size[0])+" "+str(size[1])+" "+str(band)+" "+str(ty_pe)
        dt=None
        return mwcs_string
    
    def _getCachePath(self,filename):
        """
        Return the file path without the mounting point
        """
        target_command = [ 'df', '--output=target', filename ]
        target = subprocess.Popen( target_command, stdout=subprocess.PIPE ).communicate()[0].decode('utf-8').splitlines()[1]
        file_command = [ 'df', '--output=file', filename ]
        relative_filename = subprocess.Popen( file_command, stdout=subprocess.PIPE ).communicate()[0].decode('utf-8').splitlines()[1]
        return relative_filename.split( target )[1]
    
    def _getUnit(self):
        """
        Return unit of measure
        """
        return self.descriptionDoc["units"] if "units" in self.descriptionDoc else ""

    def _getUnitDescription(self):
        """
        Return Temperature, Volumes, Meters....
        """
        return self.descriptionDoc["unitsDescriptions"] if "unitsDescriptions" in self.descriptionDoc else ""

    def _getDate( self, filename ):
        """
        Return raster reference datetime.[required for records collection]
        """
        fmt_list = [
            '%Y%m%dT%H%M%SZ',
            '%Y-%m-%dT%H:%M:%S.%fZ',
            '%Y%m%d%H%M%S',
            '%Y%m%dh%H',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%d %H:%M:%S',
            '%Y.%m.%d %H:%M:%S',
            '%Y%m%dT%H:%M:%SZ',
            '%Y%m%d %H:%M:%S',
            '%Y%m%d%H:%M:%S',
            '%Y%m%dT%H%M%S',
            '%Y%m%d.%H%M%S',
            '%Y%m%d',
            '%Y.%m.%d',
            '%Y-%m-%d'
        ]
        date_string = re.split(r'(\d{8,14}([\.|_|T]\d{6})?)',os.path.basename(filename))[1]
        image_date = None
        for fmt in fmt_list:
            try:
                image_date = datetime.strptime( date_string, fmt ).replace( tzinfo = None ).replace( microsecond=0 )
                break
            except:
                None
        if image_date is None:
            self.logger.error( "Date format not recognized - date=%s - filename=%s" %( date_string, filename ) )
        return image_date

    def _createDictInsertRecord(self , filename , item_structure_list , datasetId,document):
        record={}
        record[ 'productId' ] = self._getProductId(filename)
        record[ 'productDate' ] = self._getDate(filename)
        record[ 'geometry' ] = self._getGeom(self._getBbox(item_structure_list["gdalinfo"]))
        record[ 'source' ] = self._getSource(filename)
        record[ 'insertDate'] = self._getInsertDate()
        record[ 'status'] = self._getStatus()
        record[ "subDatasets" ] = {}
        for (sbd,val) in item_structure_list[datasetId].items():
            record[ "subDatasets" ].update({sbd:val["records"]})
        if document:
            for (sbd,value) in document["subDatasets"].items():
                record[ "subDatasets" ].update({sbd:value})
        return record

    def _createDictInsertDataset(self,item_list,datasetId,filename,document):
        dataset={}
        bbox_list=[]
        dataset[ 'profile' ] = {}
        dataset[ 'license' ] = {}
        bbox_list.append(self._getGeomDataset( self._getBbox( item_list["gdalinfo"] ), item_list["gdalinfo"] ) )
        dataset[ 'bbox' ] = bbox_list
        dataset[ 'profile' ][ 'profileSchema' ] = self._getProfileSchema()
        dataset[ 'title' ] = self._getTitle()
        dataset[ 'description' ] = self._getDatasetDescription()
        dataset[ 'application' ] = self._getApplication()
        dataset[ 'license' ][ 'dataProviderName' ] = self._getDataProvider()
        dataset[ 'license' ][ 'dataPolicy ' ] = self._getPolicyURL()
        dataset[ 'license' ][ 'dataProviderUrl' ] = self._getDataProviderUrl()
        dataset[ 'license' ][ 'licenceId' ] = self._getPolicyKey()
        dataset[ 'license' ][ 'documentationURL' ] = self._getDocumentationUrl()
        dataset[ 'datasetId' ] = datasetId
        dataset[ 'creationDate' ] = self._getDate(filename)
        dataset[ 'updateDate' ] = self._getDate(filename)
        dataset[ 'numberOfRecords' ] = 1
        dataset[ 'temporalResolution' ] = self._getTemporalResolution()
        dataset[ 'minDate' ] = self._getDate(filename)
        dataset[ 'maxDate' ] = self._getDate(filename)
        self._getServices(datasetId)
        dataset[ 'services' ] = self.descriptionDoc["services"]
        dataset[ 'subDatasets' ] = {}
        for (sbd,val) in item_list[ datasetId ].items():
            dataset[ 'subDatasets' ].update({sbd:val["datasets"]})
        if document:
            for (sbd,value) in document[ 'subDatasets' ].items():
                dataset[ 'subDatasets' ].update({sbd:value})
        return dataset

    def _getServices(self,datasetId):
        ops_service = {
                "href":"%s/opensearch/search/%s"%(WCSPATH_ENDPOINT,datasetId),
                "ref":"discovery-service",
                "title":"search",
                "type":"application/json"
                }
        if ops_service not in self.descriptionDoc["services"]:
            return self.descriptionDoc["services"].append(ops_service)
        else:
            return self.descriptionDoc["services"]

    #dataset documentation
    def _getDocumentationUrl(self):
        return self.descriptionDoc["documentationURL"] if "documentationURL" in self.descriptionDoc else 'put here the data licence Id'

    #policy key
    def _getPolicyKey(self):
        return self.descriptionDoc["licenceId"] if "licenceId" in self.descriptionDoc else 'put here the data licence Id'

    #Datasource URL
    def _getDataProviderUrl(self):
        return self.descriptionDoc["dataProviderURL"] if "dataProviderURL" in self.descriptionDoc else 'put here the data provider url'

    #policy URL
    def _getPolicyURL(self):
        return self.descriptionDoc["PolicyURL"] if "PolicyURL" in self.descriptionDoc else 'put here the policy url'

    def _getApplication(self):
        return self.descriptionDoc["Application"] if "Application" in self.descriptionDoc else 'put here the application'

    def _getDatasetDescription(self):
        return self.descriptionDoc["DatasetDescription"] if "DatasetDescription" in self.descriptionDoc else 'put here the dataset description'

    def _getTitle(self):
        return self.descriptionDoc["title"] if "title" in self.descriptionDoc else 'put here the title'
    
    def _getSource(self,filename):
        """
        Return the mounting point of this product
        """
        source_command = [ 'df', '--output=source', filename ]
        source = subprocess.Popen( source_command, stdout=subprocess.PIPE ).communicate()[0].decode('utf-8').splitlines()[1]
        fstype_command = [ 'df', '--output=fstype', filename ]
        fstype = subprocess.Popen( fstype_command, stdout=subprocess.PIPE ).communicate()[0].decode('utf-8').splitlines()[1]
        if fstype == 'ext4':
            return 'local'
        else:
            return fstype[0:3] + '://' + source
    
    def _getDataProvider(self):
        """
        Return the data provider name
        """
        return self.descriptionDoc["DataProvider"] if "DataProvider" in self.descriptionDoc else 'Internal'
    
    def _getTemporalResolution(self):
        """
        Return Monthly, daily, hourly when is set
        """
        return self.descriptionDoc["TemporalResolution"] if "TemporalResolution" in self.descriptionDoc else "Daily"
    
    def _getProfileSchema(self):
        """
        Return one of eo_profile_schema.json", "forecast_profile.json", "numerical_profile.json", "space_profile.json"
        """
        return "eo_profile_schema.json"
    
    def _getInsertDate(self):
        """
        Return the insert Date
        """
        return datetime.utcnow().replace(microsecond=0)
    
    def _getStatus(self):
        """
        Return Online/Offline/Deleted/....
        """
        return "Online"

    def _getSize(self,filename):
        """
        Returns the file size in byte
        """
        return os.stat(filename).st_size

    def insertTemporalBar( self , datasetId , product_date):
        """
        Manage temporal bar collection for adam
        """
        try:
            presence_date=product_date
            self.db.temporalbar.update_one( { 'datasetId' : datasetId , 'presenceDate' : presence_date }, { '$set' : { 'datasetId' : datasetId , 'presenceDate' : presence_date } }, upsert = True )
        except pymongo.errors.DuplicateKeyError as de:
            self.logger.exception(str(de))

    def ingestionAnalyticsList(self , datasetId , filename, feature_time , sdb):
        with self.lock:
            if datasetId in self.ingestionAnalytics:
                if sdb in self.ingestionAnalytics[ datasetId ]:
                    self.ingestionAnalytics[ datasetId ][ sdb ][ 'size' ] += self._getSize( filename )
                    self.ingestionAnalytics[ datasetId ][ sdb ][ 'count' ] += 1
                    self.ingestionAnalytics[ datasetId ][ sdb ]['time'] += feature_time
                else:
                    self.ingestionAnalytics[ datasetId ][ sdb ] = {}
                    self.ingestionAnalytics[ datasetId ][ sdb ][ 'size' ] = self._getSize( filename )
                    self.ingestionAnalytics[ datasetId ][ sdb ][ 'count' ] = 1
                    self.ingestionAnalytics[ datasetId ][ sdb ]['time'] = feature_time
            else:
                self.ingestionAnalytics[ datasetId ]={}
                self.ingestionAnalytics[ datasetId ][ sdb ] = {}
                self.ingestionAnalytics[ datasetId ][ sdb ][ 'size' ] = self._getSize( filename )
                self.ingestionAnalytics[ datasetId ][ sdb ][ 'count' ] = 1
                self.ingestionAnalytics[ datasetId ][ sdb ][ 'time' ] = feature_time

    def commit(self, last=False):
        with self.lock:
            if (self.commit_counter > 100 or last):
                for ( key, local_dataset) in self.local_datasets_list.items():
                    lock=self.acquireLock( os.path.join( LOCK_DIR, key ) )
                    mongo_dataset = self._getDataset(key)
                    if mongo_dataset is None:
                        self.db.datasets.update_one( {'datasetId':key},{'$set':local_dataset},upsert=True)
                    else:
                        if mongo_dataset[ "services" ] is None:
                            mongo_dataset[ "services" ] = []
                        for service in local_dataset["services"]:
                            if service not in mongo_dataset[ "services" ]:
                                mongo_dataset[ "services" ].append(service)
                        if not self._checkFieldLock(mongo_dataset,"_minDateLock"):
                            mongo_dataset[ "minDate" ] = self._checkMinDate(mongo_dataset['minDate'],local_dataset['minDate'])
                        if not self._checkFieldLock(mongo_dataset,"_maxDateLock"):
                            mongo_dataset[ "maxDate" ] = self._checkMaxDate(mongo_dataset['maxDate'],local_dataset['maxDate'])
                        for sbd in local_dataset[ "subDatasets" ]:
                            try:
                                if self.args.pipelinelevel == "processing":
                                    #update min
                                    if not self._checkFieldLock(mongo_dataset[ "subDatasets" ][ sbd ],"_minValueLock"):
                                        mongo_dataset[ "subDatasets" ][ sbd ]['minValue']=self._checkMin(mongo_dataset[ "subDatasets" ][ sbd ]['minValue'],local_dataset[ "subDatasets" ][ sbd ]['minValue'])
                                    if not self._checkFieldLock(mongo_dataset[ "subDatasets" ][ sbd ],"_maxValueLock"):
                                        mongo_dataset[ "subDatasets" ][ sbd ]['maxValue']=self._checkMax(mongo_dataset[ "subDatasets" ][ sbd ]['maxValue'],local_dataset[ "subDatasets" ][ sbd ]['maxValue'])
                                if not self._checkFieldLock(mongo_dataset[ "subDatasets" ][ sbd ],"_updateDateLock"):
                                    mongo_dataset[ "subDatasets" ][ sbd ]['updateDate']=self._getInsertDate()
                            except Exception as e:
                                self.logger.info("Insert new subDatasetId in the dataset")
                                mongo_dataset[ "subDatasets" ].update({sbd:local_dataset[ "subDatasets" ][ sbd ]})

                        try:
                            self.db.datasets.update_one( {'datasetId':mongo_dataset['datasetId']},{'$set':mongo_dataset},upsert=True)
                        except Exception as e:
                            print(e)

                    if self.args.pipelinelevel == "processing":
                        self.updateDatasetGeometry( key )
                        #update anytext
                        self.updateDatasetAnyText(key)

                    #update num rec
                    self.updateNumberOfRecords_new(key)
                    #update ingestion analitics
                    self.insertIngestionAnalytics()
                    if last:
                        self.updateDatasetDescribe(key)
                        self._printInline( 'Update description for dataset ' + key  )

                    self.releaseLock(lock)
                if last and len( self.local_datasets_list ) > 0:
                    print('')
                self.local_datasets_list={}
                self.commit_counter=0
                self.logger.info( 'Commit DescribeCoverage' )

    def updateDatasetDescribe(self,datasetId):
        describe = {}
        try:
            describe.update(self._getBasicFilters())
            describe.update(self._getAdvancedFilters())
            self.db.describe.update_one({"datasetId":datasetId},{"$set":{"filters":describe}},upsert=True)
        except Exception as e:
            self.logger.exception(str(e))

    def _getBasicFilters(self):
        return {"startDate":{ "title": "Start Date","class":"number","sort_order":False,"type": "string","format": "date","pattern": "^d{4}-[01]d-[0-3]d(T[0-2]d:[0-9]d:[0-9]dZ)?$"},"endDate":{"title": "End Date","class":"number","sort_order":False,"type": "string","format": "date","pattern": "^d{4}-[01]d-[0-3]d(T[0-2]d:[0-9]d:[0-9]dZ)?$"},"geometry":{"title" : "Seaching bounding box.","class":"object","sort_rule":False,"format" : "geometry","type" : "object","properties" : {"geometry" : {"type" : "object","properties" : {"type" : {"title": "Geometry type","minLength" : 1,"type": "string"},"coordinates" : {"title": "Geometry corners","minLength" : 5,"type" : "array","items" : {"type" : "number"}}}}}},"_id":{"title": "Object Id","class":"string","sort_rule":False,"type": "string","format" : "string"},"datasetId":{"title": "Dataset Id","class":"string","sort_rule":True,"type": "string","format" : "string"},"productId":{"title": "Product Identifier","class":"string","sort_rule":False,"type": "string","format" : "string"}}

    def _getAdvancedFilters(self):
        return {}
    
    def _checkMinDate(self, db_mindate , date):
        """
        Return the minimum date between two date
        """
        if db_mindate<date:
            return db_mindate
        else:
            return date

    def _checkMaxDate(self, db_maxdate , date):
        """
        Return the maximum date between two date
        """
        if db_maxdate>date:
            return db_maxdate
        else:
            return date

    def _checkMin(self,d_array,min_val_array):
        """
        Return the minimum array bands between two
        """
        minup=[]
        for i in range(len(d_array)):
            if d_array[i]>min_val_array[i]:
                minup.append(float(min_val_array[i]))
            else:
                minup.append(float(d_array[i]))
        return minup

    def _checkMax(self,d_array,max_val_array):
        """
        Return the maximum array bands between two
        """
        maxup=[]
        for i in range(len(d_array)):
            if d_array[i]<max_val_array[i]:
                maxup.append(float(max_val_array[i]))
            else:
                maxup.append(float(d_array[i]))
        return maxup

    def _checkFieldLock(self,doc,field):
        """
        Return True if lock field is set or False if is unset or undefined
        """
        try:
            flag=doc[field]
        except:
            flag=None


        if flag is not None:
            return flag
        else:
            return False

    def insertIngestionAnalytics(self):
        print(self.ingestionDate,type(self.ingestionDate))
        for datasetId in self.ingestionAnalytics:
            for (sbd,val) in self.ingestionAnalytics[datasetId].items():
                try:
                    self.db.ingestionAnalytics.update_one({
                        "datasetId":datasetId,
                        "subDatasetId":sbd,
                        'ingestionDate':self.ingestionDate
                        },
                        {
                        "$set":{"datasetId":datasetId,'ingestionDate':self.ingestionDate},
                        "$inc":{"size":val['size'],"count":val['count'],"time":val['time']}
                        },upsert=True)
                except pymongo.errors.DuplicateKeyError as de:
                    self.logger.exception(str(de))
        self.ingestionAnalytics={}


    def updateNumberOfRecords_new(self,datasetId):
        """
        use estimated count to get the number of record for each datasetId collection
        """
        amount=self.db[datasetId].estimated_document_count()
        self.db.datasets.update_one({'datasetId':datasetId},{'$set':{'numberOfRecords':amount}},upsert=True)


    def updateResolutions(self,datasetId):
        """
        Return the list of resolutions
        """
        dataset=self._getDataset( datasetId )
        resolutions=[]
        if not self._checkFieldLock(dataset,"_resolutionsLock"):
            for i in range(0,len(dataset['bbox'])):
                for res in dataset['bbox'][i]['properties']['Resolution']:
                    if res not in resolutions:
                        resolutions.append(float(res))
        self.db.datasets.update_one({'datasetId':datasetId},{"$set":{"resolutions":resolutions}},upsert=True)


    def updateDatasetGeometry( self, datasetId ):
        """
        Union of all dataset bbox geometries
        """
        dataset=self._getDataset( datasetId )
        if not self._checkFieldLock(dataset,"_geometryLock"):
            union=ogr.CreateGeometryFromJson(json.dumps(dataset['bbox'][0]['geometry']))
            for i in range(1,len(dataset['bbox'])):
                geom=ogr.CreateGeometryFromJson(json.dumps(dataset['bbox'][i]['geometry']))
                union=union.Union(geom)
            env=union.GetEnvelope()
            geometry = { 'type':'Polygon','coordinates':[[ [env[0],env[2]],[env[1],env[2]],[env[1],env[3]],[env[0],env[3]],[env[0],env[2]]]] }
            self.db.datasets.update_one({'datasetId':dataset['datasetId']},{'$set':{ 'geometry':geometry }},upsert=True)

    def updateDatasetAnyText(self,datasetId):
        """
        Update anytext field if it is possible
        """
        dataset=self._getDataset( datasetId )
        if not self._checkFieldLock(dataset,"_anyTextLock"):
            anytext=self._getDatasetAnyText()
            self.db.datasets.update_one({"datasetId":datasetId},{"$set":{"anyText":anytext}},upsert=True)
    def _getResolution(self,gdal_info):
        """
        Return the Resolution of the product
        """
        if self.args.resolution:
            resolution=self.args.resolution
        else:
            resolution=gdal_info['geoTransform'][1]
        return resolution

    def _getDatasetAnyText(self):
        """
        Return dataset free search field(opensearch q parameter field)
        """
        anyText= ','.join([
            self._getEpsg(),
            self._getDataProvider(),
            self._getTemporalResolution(),
            self._getDataProvider()
            ])
        return anyText
    
    def _getEpsg(self):
        """
        Return EPSG
        """
        return self.descriptionDoc["epsg"] if "epsg" in self.descriptionDoc else "4326"

    def _getDataset( self, datasetid ):
        """
        Return dataset from datasets collection
        """
        dataset = self.db.datasets.find_one( { 'datasetId':datasetid} )
        return dataset

    def _getGeomDataset( self, bbox, gdal_info ,fname=None):
        """
        Return the geometry for dataset collection
        """
        if fname is not None:
            pro=self._getProjection(gdal_info,fname=fname)
        else:
            pro=self._getProjection(gdal_info)
        g4 = {
            'type':'Feature',
            'geometry':{ 'type':'Polygon','coordinates':[ bbox ] },
            'properties':{
                'Projection':pro,
                'Resolution' :[ self._getResolution(gdal_info) ]
            }
        }
        return g4
    
    def _getProjection(self,gdal_info,fname=None):
        """
        Return the projection of the product
        """
        if self.args.epsg:
            epsg=self.args.epsg
        elif fname is not None:
            epsg=fname.split("/")[10].strip("E")
        else:
            epsg=gdal_info["coordinateSystem"]["wkt"].rsplit('ID["EPSG",', 1)[-1].strip("]")
        return epsg

    def _checkBbox(self,cord):
        """
        Check if coordinates respect mongodb rules
        """
        if cord[0]>180:
            cord[0]=180
        elif cord[0]<-180:
            cord[0]=-180

        if cord[1]>90:
            cord[1]=90
        elif cord[1]<-90:
            cord[1]=-90
        return cord


    def _getBbox(self,gdal_info,fname=None):
        """
        Return the product bbox (WKT)
        """
        bbox=[]
        ll=gdal_info["cornerCoordinates"]["lowerLeft"]
        ul=gdal_info["cornerCoordinates"]["upperLeft"]
        lr=gdal_info["cornerCoordinates"]["lowerRight"]
        ur=gdal_info["cornerCoordinates"]["upperRight"]
        if self.args.epsg:
            inepsg=int(self.args.epsg)
        else:
            try:
                if fname is not None:
                    inepsg=int(fname.split("/")[10].strip("E"))
                else:
                    inepsg=int(gdal_info["coordinateSystem"]["wkt"].rsplit('"EPSG","', 1)[-1].split('"')[0])
            except:
                inepsg=int(gdal_info["coordinateSystem"]["wkt"].rsplit('"EPSG",',1)[-1].split("]]")[0])


        if inepsg == 4326:
            ll_t=self._checkBbox(ll)
            ul_t=self._checkBbox(ul)
            lr_t=self._checkBbox(lr)
            ur_t=self._checkBbox(ur)
        else:
             inProj = Proj('epsg:%d' %( inepsg ))
             ll_tx,ll_ty=transform(inProj,self.outProj,ll[0],ll[1], always_xy=True)
             lr_tx,lr_ty=transform(inProj,self.outProj,lr[0],lr[1], always_xy=True)
             ul_tx,ul_ty=transform(inProj,self.outProj,ul[0],ul[1], always_xy=True)
             ur_tx,ur_ty=transform(inProj,self.outProj,ur[0],ur[1], always_xy=True)
             ll_t=[ll_tx,ll_ty]
             lr_t=[lr_tx,lr_ty]
             ul_t=[ul_tx,ul_ty]
             ur_t=[ur_tx,ur_ty]

        bbox.append(ll_t)
        bbox.append(lr_t)
        bbox.append(ur_t)
        bbox.append(ul_t)
        bbox.append(ll_t)
        return bbox

    def _checkSplit(self,bbox):
        """
        Return True if the distance between the longitude of Lower Left point and Upper Right point is greater then 180°, else False
        """
        if self._checkIdl( bbox) == False and abs( bbox[0][0] - bbox[2][0] ) > 180:
            return True
        elif self._checkIdl( bbox) == True and abs( 180 - bbox[0][0] ) + abs( -180 - bbox[2][0] ) > 180:
            return True
        else:
            return False

    def _checkIdl(self, bbox):
        """
        return True when raster bbox is across the idl
        """
        if bbox[0][0] < bbox[2][0]:
            return False
        else:
            return True

    def _getGeom(self, bbox):
        """
        manage geometry
        handle IDL
        split in multipolygon bbox greater than 180 for mongo
        """
        geomcol={'geometries':[],'type':'GeometryCollection'}
        poly=[]
        if self._checkIdl(bbox) == True and self._checkSplit(bbox) == True:
            xmd=((360-(abs(bbox[0][0])+abs(bbox[1][0])))/2)
            poly.append({'type':'Polygon','coordinates':[[[bbox[0][0],bbox[0][1]],[xmd,bbox[0][1]],[xmd,bbox[2][1]],[bbox[3][0],bbox[3][1]],[bbox[0][0],bbox[0][1]]]]})
            poly.append({'type':'Polygon','coordinates':[[[xmd,bbox[0][1]],[bbox[1][0],bbox[1][1]],[bbox[2][0],bbox[2][1]],[xmd,bbox[2][1]],[xmd,bbox[0][1]]]]})
        elif self._checkIdl(bbox) == False and self._checkSplit(bbox) == True:
            xmd=((bbox[1][0]+bbox[0][0])/2)
            poly.append({'type':'Polygon','coordinates':[[[bbox[0][0],bbox[0][1]],[xmd,bbox[0][1]],[xmd,bbox[2][1]],[bbox[3][0],bbox[3][1]],[bbox[0][0],bbox[0][1]]]]})
            poly.append({'type':'Polygon','coordinates':[[[xmd,bbox[0][1]],[bbox[1][0],bbox[1][1]],[bbox[2][0],bbox[2][1]],[xmd,bbox[2][1]],[xmd,bbox[0][1]]]]})
        else:
            poly.append({'type':'Polygon','coordinates':[bbox]})

        for p in poly:
            geomcol['geometries'].append(p)

        return geomcol
    
    def acquireLock( self, filename ):
        """
        When a dataset included in the Describe Coverage is updated, blocks other readings or writes
        """
        locked_file_descriptor = open( filename, 'w+')
        fcntl.lockf(locked_file_descriptor, fcntl.LOCK_EX)
        infomess = "Acquire lock for file %s" %( filename )
        self.logger.debug( infomess )
        return locked_file_descriptor

    def releaseLock( self, locked_file_descriptor ):
        """
        After the Update of the describe coverage, release the block for other update
        """
        locked_file_descriptor.close()
    
    def _print( self, count_part, count_tot ):
        """
        print to stdout the progress bar
        """
        if not self.args.debug and self.progress_bar: #when debug is set or if there is no display attached the progressbar will not be displayed
            if count_tot != 0:
                percent      = int( count_part * 100 / count_tot )
                percent_bar  = "=" * percent + " " * ( 100 - percent )
                elapsed_time = time.time() - self.start_registration
                ETA          = ( elapsed_time / count_part ) * count_tot - elapsed_time
                sys.stdout.write("\r [%s] - %d %% | %d/%d - %ds ETA" %( percent_bar, count_part*100/count_tot, count_part, count_tot, ETA  ))    # or print >> sys.stdout, "\r%d%%" %i,
                sys.stdout.flush()
                with open(STATUSLOADER_DIR+"_"+self.args.mid,"w") as status:
                    json.dump({
                        "elapsed_time" : elapsed_time ,
                        "percent" : ( 100 - percent ) ,
                        "ETA" : ETA
                        },status)

                #PUT progress on DPS
                if self.args.updateurl:
                    old_percent=percent
                    if old_percent == 0 or (old_percent-self.DPSperc) >= 10 :
                        self.DPSperc=percent
                        self.sendDPSStatus("R",self.DPSperc)
            else:
                self.sendDPSStatus("E",self.DPSperc)

            if count_part == count_tot:
                print ( "" )
                if self.args.updateurl:
                    self.sendDPSStatus("S",self.DPSperc)

    def _printInline( self, out_str ):
        if not self.args.debug and self.progress_bar: #when debug is set or if there is no display attached the progressbar will not be displayed
            sys.stdout.write( "\r" + out_str )
            sys.stdout.flush()
    
    def sendDPSStatus( self, code, perc ):
        """
        update DPS status when loader is launched from a pipeline
        available status:
        - R --> running
        - E --> error
        - S --> success
        """
        status={
            "code":code,
            "perc":perc,
            "errors": json.dumps( self.error_list )
        }
        data={
            "type":"load",
            "status":status
            }
        if self.args.onError == "strict" and len( self.error_list )>0:
            data['code']="E"
        r = requests.put( self.args.updateurl, json=data )
        self.logger.info( "DPS status sent" )


class DefaultLoader( MongoLoader ):
    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )

    def getKey():
        return 'default'

