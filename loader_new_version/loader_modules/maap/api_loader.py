from loader_modules.default_loader import DefaultLoader
import json
from osgeo import gdal,ogr
from datetime import datetime
import time
import pymongo
from settings import WCSPATH_ENDPOINT
class ApiLoader(DefaultLoader):
    """
    {
    "src":s3://....,
    "f":{
        "datasetId:"",required
        "productDate:"",required
        "geometry":{"type":"Polygon","coordinates":[[[]]]}||{"type":"GeometryCollection","geometries":[{"type":"Polygon","coordinates":[[[]]]},{"type":"Polygon","coordinates":[[[]]]}]} following the right hand rule,required
        "subDatasetId":"",required
        "minValue":int,required
        "maxValue":int,required
        "noDataValue:int,
        "offsetData":int,
        "dataType":int,required
        "scale":1,
        "size":byte,
        "unit":"K",required
        "unitDescription":"Temperature",required
        "Datasettitle":"",required
        "DatasetTemporalResolution":"Yearly|Monthly|Daily|Hourly",required
        "DatasetDescription":"",required
        "DatasetApplication":"",required
        "DatasetdataProviderName":"",
        "DatasetdataPolicy":"",
        "DatasetdataProviderUrl":"",
        "DatasetlicenceId":"",
        "DatasetdocumentationURL":""
        }
    }
    }

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _chechArgs(self):
        if self.args.feature is None:
            raise ValueError( 'Source "%s"  or feature "%s" not specified or not valid' % ( self.args.source, self.args.feature ) )

    def _getSubDatasetId(self,feature):
        return feature["subDatasetId"]
    def checkBandSubDset(self,filename,datasetId,feature=None):
        """
        1- check if the self.args.source has multiband or subdatasets
        2- return a structure like this: {"datasetId":{"subDatasetId":{subDatasetId infos},.....,"subDatasetIdn":{subDatasetIdn infos}}}
        """
        item_structure={}
        item_structure[datasetId]={}
        gdal_opt = gdal.InfoOptions(format="json",stats=True,computeMinMax=True)
        gdal_info= gdal.Info(filename,options=gdal_opt)
        subdatasetId = self._getSubDatasetId(feature)
        item_structure[datasetId].update(self.createSubDatasetStructure(filename,gdal_info,subdatasetId,0,feature))
        item_structure["gdalinfo"]=gdal_info
        return item_structure

    def createSubDatasetStructure( self , filename , gdalinfo , subDatasetId ,index,feature):
        item = {}
        item[ subDatasetId ] = { }
        item[ subDatasetId ]["datasets"] = {}
        item[ subDatasetId ]["records"] = {}
        item[ subDatasetId ]["datasets"][ "minValue" ] = [feature["minValue"]]
        item[ subDatasetId ]["datasets"][ "maxValue" ] = [feature["maxValue"]]
        item[ subDatasetId ]["datasets"][ 'noDataValue' ] = feature["noDataValue"] if "noDataValue" in feature else None
        item[ subDatasetId ]["datasets"][ 'numberOfRecords' ] = 1
        item[ subDatasetId ]["datasets"][ 'units' ] = feature["unit"]
        item[ subDatasetId ]["datasets"][ 'unitsDescription' ] = feature["unitDescription"]
        item[ subDatasetId ]["datasets"][ 'Resolution' ] = [self._getResolution(gdalinfo)]
        item[ subDatasetId ]["records"][ "productPath" ] = f"/{'/'.join(self.args.source.split('/')[3:])}"
        item[ subDatasetId ]["records"][ "band" ] = str( index )
        item[ subDatasetId ]["records"][ "mwcs" ] = self._getMWcsData( filename , gdalinfo )
        if "offsetData" in feature:
            item[ subDatasetId ]["records"][ "offsetData"] = feature["offsetData"]
        if "scale" in feature:
            item[ subDatasetId ]["records"][ "scale"] = feature["scale"]
        item[ subDatasetId ]["records"][ "minValue" ] = feature["minValue"]
        item[ subDatasetId ]["records"][ "maxValue" ] = feature["maxValue"]
        item[ subDatasetId ]["records"][ 'dataType' ] = feature["dataType"]
        item[ subDatasetId ]["records"][ 'noDataValue' ] = feature["noDataValue"] if "noDataValue" in feature else None

        return item
    def checkDuplicates(self,filename,feature):
        unique_index = {
            'productId':self._getProductId( self.args.source ),
            'productDate':self._getDate( feature["productDate"] )
        }
        action = 'load'
        if not self.args.reload and self.db[self._getDatasetName( self.args.source )].count_documents( unique_index ) > 0:
            self.skipped_products_counter += 1
            action = 'skip'
        return {
            'action': action,
            'unique_index':  unique_index
        }

    def _getDate( self, date ):
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

        image_date = None
        for fmt in fmt_list:
            try:
                image_date = datetime.strptime( date, fmt ).replace( tzinfo = None ).replace( microsecond=0 )
                break
            except:
                None
        if image_date is None:
            self.logger.error( "Date format not recognized - date=%s" %( date) )
        return image_date
    def _createDictInsertRecord(self , filename , item_structure_list , datasetId,document,feature):
        record={}
        record[ 'productId' ] = self._getProductId(filename)
        record[ 'productDate' ] = self._getDate(feature["productDate"])
        record[ 'geometry' ] = feature["geometry"]
        record[ 'source' ] = f"s3://{self.args.source.split('/')[2]}"
        record[ 'insertDate'] = self._getInsertDate()
        record[ 'status'] = self._getStatus()
        record[ "subDatasets" ] = {}
        for (sbd,val) in item_structure_list[datasetId].items():
            record[ "subDatasets" ].update({sbd:val["records"]})
        if document:
            for (sbd,value) in document["subDatasets"].items():
                record[ "subDatasets" ].update({sbd:value})
        return record

    def _createDictInsertDataset(self,item_list,datasetId,filename,document,feature):
        dataset={}
        bbox_list=[]
        dataset[ 'profile' ] = {}
        dataset[ 'license' ] = {}
        bbox_list.append(self._getGeomDataset( feature["geometry"]["coordinates"][0], item_list["gdalinfo"] ) )
        dataset[ 'bbox' ] = bbox_list
        dataset[ 'profile' ][ 'profileSchema' ] = self._getProfileSchema()
        dataset[ 'title' ] = feature["Datasettitle"]
        dataset[ 'description' ] = feature["DatasetDescription"]
        dataset[ 'application' ] = feature["DatasetApplication"]
        if "DatasetdataProviderName" in feature:
            dataset[ 'license' ][ 'dataProviderName' ] = feature["DatasetdataProviderName"]
        if "DatasetdataPolicy" in feature:
            dataset[ 'license' ][ 'dataPolicy ' ] = feature["DatasetdataPolicy"]
        if "DatasetdataProviderUrl" in feature:
            dataset[ 'license' ][ 'dataProviderUrl' ] = feature["DatasetdataProviderUrl"]
        if "DatasetlicenceId" in feature:
            dataset[ 'license' ][ 'licenceId' ] = feature["DatasetlicenceId"]
        if "DatasetdocumentationURL" in feature:
            dataset[ 'license' ][ 'documentationURL' ] = feature["DatasetdocumentationURL"]
        dataset[ 'datasetId' ] = datasetId
        dataset[ 'creationDate' ] = self._getInsertDate()
        dataset[ 'updateDate' ] = self._getInsertDate()
        dataset[ 'numberOfRecords' ] = 1
        dataset[ 'temporalResolution' ] = feature["DatasetTemporalResolution"]
        dataset[ 'minDate' ] = self._getDate(feature["productDate"])
        dataset[ 'maxDate' ] = self._getDate(feature["productDate"])
        dataset[ 'services' ] = self._getServices(datasetId)
        dataset[ 'subDatasets' ] = {}
        for (sbd,val) in item_list[ datasetId ].items():
            dataset[ 'subDatasets' ].update({sbd:val["datasets"]})
        if document:
            for (sbd,value) in document[ 'subDatasets' ].items():
                dataset[ 'subDatasets' ].update({sbd:value})
        return dataset
    def _getServices(self,datasetId):
        ops_service = {
            "href":f"{WCSPATH_ENDPOINT.replace('-loader','-ops')}/search/{datasetId}",
            "ref":"discovery-service",
            "title":"search",
            "type":"application/json"
        }
        wcs_service = {
            "href":f"{WCSPATH_ENDPOINT.replace('-loader','-wcs')}/wcs",
            "ref":"access-service",
            "title":"wcs",
            "type":"application/json|image/png|image/tif"
        }
        return [ops_service,wcs_service]

    def getList( self ):
        """
        Search raster on self.args.source
        """
        try:
            return [self.args.feature]
        except Exception as e:
            self.logger.exception(e)
            return {}
    def load(self,feature):
        load_timer = time.time()
        feature = json.loads(feature)
        self.logger.debug(feature)
        datasetId = feature["datasetId"]
        self._checkCollection( datasetId )
        filename= self.args.source
        item_structure_list = self.checkBandSubDset( "/vsis3/%s"%(filename.replace("s3://","")) , datasetId,feature)
        duplicate_test = self.checkDuplicates( filename,feature )
        if duplicate_test [ "action" ] == "skip" :
            self.logger.info( 'Registration of "%s" skipped in %f s' %( filename , time.time() - load_timer ) )
        else:
            time.sleep( 0.01 )
            self.commit() #TODO
            unique_index = duplicate_test[ 'unique_index' ]
            self.logger.info( 'Start registration of "%s"' %( filename ) )
            feature_time = time.time()
            document = self.db[datasetId].find_one(unique_index)
            rec_doc = self._createDictInsertRecord( filename , item_structure_list , datasetId ,document,feature)
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
            rec_dat=self._createDictInsertDataset(item_structure_list,datasetId,filename,dataset_doc,feature)
            self._createDatasetList(rec_dat)
            feature_time_end= time.time()-feature_time
            #temporalbar & ingestionAnalytics
            self.insertTemporalBar(datasetId , rec_doc['productDate'])
            #for (k,v) in item_structure_list[datasetId].items():
            #    self.ingestionAnalyticsList( datasetId , filename , feature_time_end , k)

        self.logger.info( 'Registration of "%s" completed in %f s' %( filename, time.time() - load_timer ) )
        self.loaded_products_counter += 1
    def getKey():
        return "apiloadervap@maap"
