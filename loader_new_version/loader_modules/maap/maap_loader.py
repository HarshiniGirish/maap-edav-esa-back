from loader_modules.default_loader import DefaultLoader
from settings import AWS_BUCKET,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_S3_ENDPOINT,WCSPATH_ENDPOINT
import boto3
import os
from osgeo import gdal
import json
import re
import shlex
import subprocess
class MaapLoader(DefaultLoader):

   def __init__(self,*args,**kwargs):
      super().__init__(*args,**kwargs)

   def _chechArgs( self ):
        """
        Check specific module requirements
        """
        if self.args.source is None :
            raise ValueError( 'Source "%s" not specified or not valid' % ( self.args.source ) )


   def getList(self):
       file_list=[]

       s3 = boto3.resource( 's3' ,aws_access_key_id=AWS_ACCESS_KEY_ID,aws_secret_access_key=AWS_SECRET_ACCESS_KEY,endpoint_url="https://"+AWS_S3_ENDPOINT)
       bucket = s3.Bucket( name =  AWS_BUCKET)
       regular_expression = re.compile( r'%s' %( self.args.pattern ) )
       for obj in bucket.objects.filter( Prefix = self.args.source ):
           if regular_expression.match( obj.key ):
               filename = os.path.join( '/vsis3', AWS_BUCKET, obj.key )
               file_list.append( filename )
       return file_list

   def _getDatasetName( self,filename ):
       return os.apth.basename(filename).split("_")[0]
   
   def createVrt(self,vrt_filename,filename):
       gdtr_opt= gdal.TranslateOptions(format="VRT",outputSRS="EPSG:4326")
       gdal.Translate(vrt_filename,filename,options=gdtr_opt)

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
               subdatasetId = datasetId+"_B"+str(index)
               vrt_filename=os.path.join("/tmp",os.path.basename(filename).split(".")[0]+".vrt")
               self.createVrt(vrt_filename,filename)
               gdal_opt = gdal.InfoOptions(format="json",stats=True,computeMinMax=True)
               gdal_info_vr= gdal.Info(vrt_filename,options=gdal_opt)
               item_structure[datasetId].update(self.createSubDatasetStructure(filename,gdal_info_vr,subdatasetId,index,datasetId,vrt_filename))
           item_structure["gdalinfo"]=gdal_info
       return item_structure

   def _getVrt(self,vrt_filename):
        command="cat %s"%(vrt_filename)
        args=shlex.split(command)
        p1=subprocess.Popen(args,stdout=subprocess.PIPE,shell=False)
        output,err=p1.communicate()
        vrt=output.decode('utf-8').replace('\n','').replace("&quot;",'"')
        return vrt

   def createSubDatasetStructure( self , filename , gdalinfo , subDatasetId ,index,datasetId,vrt_filename):
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
       item[ subDatasetId ]["records"][ "productPath" ] = "vrt"
       item[ subDatasetId ]["records"][ "band" ] = str( index )
       item[ subDatasetId ]["records"][ "mwcs" ] = self._getMWcsData( filename , gdalinfo )
       item[ subDatasetId ]["records"]["wcsPath"] =WCSPATH_ENDPOINT+"?service=WCS&version=2.0.0&request=GetCoverage&coverageId=%s&subdataset=%s&subset=unix(%s)&scale=0.05&format=image/tiff"%(datasetId,subDatasetId,self._getDate(filename).strftime("%Y-%m-%dT%H:%M:%SZ"))
       if "offset" in gdalinfo["bands"][index]:
           item[ subDatasetId ]["records"][ "offsetData"] = self._getScaleData(gdalinfo)
       if "scale" in gdalinfo["bands"][index]:
           item[ subDatasetId ]["records"][ "scale"] = self._getScaleData(gdalinfo)
       item[ subDatasetId ]["records"][ "vrt" ] = self._getVrt(vrt_filename)
       item[ subDatasetId ]["records"][ "minValue" ] = gdalinfo["bands"][index]["minimum"]
       item[ subDatasetId ]["records"][ "maxValue" ] = gdalinfo["bands"][index]["maximum"]
       item[ subDatasetId ]["records"][ 'dataType' ] = gdalinfo["bands"][index]["type"]
       item[ subDatasetId ]["records"][ 'noDataValue' ] = gdalinfo['bands'][index]['noDataValue'] if "noDataValue" in gdalinfo['bands'][index]else None

       os.remove(vrt_filename)
       return item

   def _createDictInsertRecord(self , filename , item_structure_list , datasetId):
       record={}
       record[ 'productId' ] = self._getProductId(filename)
       record[ 'productDate' ] = self._getDate(filename)
       record[ 'geometry' ] = self._getGeom(self._getBbox(item_structure_list["gdalinfo"]))
       record[ 'source' ] = "vrt"
       record[ 'downloadLink' ] = filename.replace("/vsis3/","s3://")
       record[ 'insertDate'] = self._getInsertDate()
       record[ 'status'] = self._getStatus()
       record[ "subDatasets" ] = {}
       for (sbd,val) in item_structure_list[datasetId].items():
           record[ "subDatasets" ].update({sbd:val["records"]})
       return record

   def _getSize(self,filename):
        s3 = boto3.resource( 's3' ,aws_access_key_id=AWS_ACCESS_KEY_ID,aws_secret_access_key=AWS_SECRET_ACCESS_KEY,endpoint_url="https://"+AWS_S3_ENDPOINT)
        bucket = s3.Bucket( name = AWS_BUCKET )
        for obj in bucket.objects.filter( Prefix = filename.replace("/vsis3/%s/"%(AWS_BUCKET),"")):
            return obj.size

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

       self.logger.info( 'Registration of "%s" completed in %f s' %( filename, time.time() - load_timer ) )
       self.loaded_products_counter += 1
   
   def _createDictInsertDataset(self,item_list,datasetId,filename):
       datasets = super()._createDictInsertDataset(item_list,datasetId,filename)
       datasets["envs"] = "AWS_ACCESS_KEY_ID=%s;AWS_SECRET_ACCESS_KEY=%s;AWS_S3_ENDPOINT=%s;CPL_DEBUG=ON"%(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_S3_ENDPOINT)
       return datasets

   def getKey():
       return "maap@default"

