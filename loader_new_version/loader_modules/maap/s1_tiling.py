from loader_modules.maap.maap_loader import MaapLoader
import os
import time
from settings import WCSPATH_ENDPOINT,AWS_S3_ENDPOINT
from osgeo import gdal,osr,ogr
import boto3
import pymongo
import json
import sys
class S1TilingLoader(MaapLoader):

    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)

    def _getDatasetName(self,filename):
        return os.path.basename(filename).split("_")[0][0:2]+"_tiling"
    
    def createVrt(self,vrt_filename,filename):
       gdtr_opt= gdal.WarpOptions(format="VRT",dstSRS="EPSG:4326")
       gdal.Warp(vrt_filename,filename,options=gdtr_opt)

    def load(self,filename):
        """
        1- creo,se non presente, collection con collection_name=datasetId
        2- check band/subdataset con return della struttura json i.e.: ["subDatasetId":{subdatsetPointer:"---"},.........,"subDatasetIdn"{}]
        3- inserisco records,temporalbar,ingestion analytics
        4- aggiorno struttura collection datasets per il commit
        """
        load_timer = time.time()
        try:
            if not "x" in os.path.basename(filename):
                filename=self.uploadFileinOut( filename )
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
                    rec_doc = self._createDictInsertRecord( filename , item_structure_list , datasetId )
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
                    rec_dat=self._createDictInsertDataset(item_structure_list,datasetId,filename)
                    self._createDatasetList(rec_dat)
                    feature_time_end= time.time()-feature_time
                    #temporalbar & ingestionAnalytics
                    temporalbar_id=self.insertTemporalBar(datasetId , rec_doc['productDate'])
                    for (k,v) in item_structure_list[datasetId].items():
                        self.ingestionAnalyticsList( datasetId , filename , feature_time_end , k)
            else:
                print("File skipped: %s"%(os.path.basename(filename))) 
        except Exception as e:
            print(e)
            print("Unable to sync in direcotry with out directory")
            sys.exit(1)


        self.logger.info( 'Registration of "%s" completed in %f s' %( filename, time.time() - load_timer ) )
        self.loaded_products_counter += 1

    def uploadFileinOut(self,filename):
        try:
            os.system("aws s3 mv %s %s --endpoint-url %s"%(filename.replace("/vsis3/","s3://"),filename.replace("/vsis3/","s3://").replace("/in/","/out/"),"https://"+AWS_S3_ENDPOINT))
        except Exception as e:
            print(e)
        return filename.replace("/in/","/out/")
    
    def _getServices(self,datasetId):
        ops_service = [{
                "href":WCSPATH_ENDPOINT.replace("/wcs","/opensearch/search")+"/%s"%(datasetId),
                "ref":"discovery-service",
                "title":"search",
                "type":"application/json"
                },
                {
                "href":WCSPATH_ENDPOINT.replace("/wcs","/stac/collections")+"/%s"%(datasetId),
                "ref":"discovery-service",
                "title":"search",
                "type":"application/json"
                }
                ]
        for service in ops_service:
            self.descriptionDoc["services"].append(service)
        return self.descriptionDoc["services"]

    def getKey():
        return "s1tiling@maap"
