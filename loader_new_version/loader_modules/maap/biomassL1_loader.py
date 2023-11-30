from loader_modules.default_loader import DefaultLoader
from settings import WCSPATH_ENDPOINT
import os
from osgeo import gdal
import time 
class BiomassL1Loader(DefaultLoader):

    def __init__(self,*args,**kwargs):
        """
        gdal_translate -of GTiff -a_ullr 40.8632243719488 2.76453065994073 41.9753263394112 1.6429090218097 -a_srs EPSG:4326 -b 1 /nfsdata/BIOMASS_Sample_Products/BIOMASS_Sample_Products2/BIO_S2_DGM__1S_20170111T050609_20170111T050630_I_G03_M03_C03_T010_F001_01_CFGG6U/measurement/bio_s2_dgm__1s_20170111t050609_20170111t050630_i_g03_m03_c03_t010_f001_i_abs.tiff /nfsdata/BIOMASS_Sample_Products/warp/bio_s2_dgm__1s_20170111t050609_20170111t050630_i_g03_m03_c03_t010_f001_i_abswarped.tiff
        """
        super().__init__(*args,**kwargs)
        self.translate_dir ="/nfsdata/BIOMASS_Sample_Products/warp"

    def _getProductId(self, filename):
        """
        Return an unique Product ID, for example the filename.[required for records collection]
        """
        return os.path.dirname(filename).split("/")[-2]

    def _getSubDatasetId(self,filename,polarization):
        return os.path.basename(filename).split("_")[-1].rstrip(".tiff")+"_"+polarization

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
       gdal_info_original = gdal.Info(filename,format="json")
       gcps=gdal_info_original["gcps"]["gcpList"]
       polarizations = gdal_info_original["metadata"][""]["PolarisationsSequence"].split(" ")
       for i,band in enumerate(gdal_info_original["bands"]):
           vrt_filename=os.path.join(self.translate_dir,os.path.basename(filename).replace(".tiff",f"_{polarizations[i]}.tiff"))
           opt = gdal.TranslateOptions(format="GTiff",outputBounds=[gcps[3]["x"],gcps[3]["y"],gcps[1]["x"],gcps[1]["y"]],outputSRS="EPSG:4326",bandList=[band["band"]])
           gdal.Translate(vrt_filename,filename,options=opt)
           item_structure_list = self.checkBandSubDset( filename , datasetId ,vrt_filename,polarizations[i])
           duplicate_test = self.checkDuplicates( filename )
           if duplicate_test [ "action" ] == "skip" :
               self.logger.info( 'Registration of "%s" skipped in %f s' %( vrt_filename , time.time() - load_timer ) )
           else:
               time.sleep( 0.01 )
               self.commit() #TODO
               unique_index = duplicate_test[ 'unique_index' ]
               self.logger.info( 'Start registration of "%s"' %( filename ) )
               feature_time = time.time()
               document = self.db[datasetId].find_one(unique_index)
               rec_doc = self._createDictInsertRecord( vrt_filename , item_structure_list , datasetId, document,filename)
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

    def checkBandSubDset(self,filename,datasetId,vrt_filename,polarization):
       """
       1- check if the filename has multiband or subdatasets
       2- return a structure like this: {"datasetId":{"subDatasetId":{subDatasetId infos},.....,"subDatasetIdn":{subDatasetIdn infos}}}
       """
       item_structure={}
       item_structure[datasetId]={}
       gdal_opt = gdal.InfoOptions(format="json",stats=True,computeMinMax=True)
       gdal_info= gdal.Info(vrt_filename,options=gdal_opt)
       if "bands" in gdal_info and len(gdal_info["bands"])>0 :
           for index,band in enumerate(gdal_info["bands"]):
               subdatasetId = self._getSubDatasetId(filename,polarization)
               item_structure[datasetId].update(self.createSubDatasetStructure(vrt_filename,gdal_info,subdatasetId,index,datasetId))
           item_structure["gdalinfo"]=gdal_info
       return item_structure

    def createSubDatasetStructure( self , filename , gdalinfo , subDatasetId ,index,datasetId):
       item = super().createSubDatasetStructure(filename , gdalinfo , subDatasetId ,index)
       item[ subDatasetId ]["records"]["wcsPath"] =WCSPATH_ENDPOINT.replace("v1","wcs")+"?service=WCS&version=2.0.0&request=GetCoverage&coverageId=%s&subdataset=%s&subset=unix(%s)&scale=0.05&format=image/tiff"%(datasetId,subDatasetId,self._getDate(filename).strftime("%Y-%m-%dT%H:%M:%SZ"))
       return item

    def _createDictInsertRecord(self , filename , item_structure_list , datasetId,document,org_filename):
        record={}
        record[ 'productId' ] = self._getProductId(org_filename)
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

    def getKey():
        return "biomassL1@maap"
