import sys
import os
import traceback

import requests
dir_path = os.path.dirname( os.path.realpath( __file__ ) )
previous_path = os.path.dirname( dir_path )
for d in [ dir_path, previous_path ]:
    if not d in sys.path:
        sys.path.append( d )
from loader_module.default_loader import CMRLoader
from loader.settings import init_environ,DATASETS_SPEC
import re
import json
import boto3
from osgeo import gdal
from pyproj import Proj,transform
from datetime import datetime,timedelta
class AfrisarOneraGeoLoader(CMRLoader):

    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        init_environ()
        with open(DATASETS_SPEC) as json_file:
            self.data = json.load(json_file)

    def getList(self):
        file_list,datasetlist=super().getList()
        file_list=[]

        s3 = boto3.resource( 's3' ,aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),endpoint_url="https://"+os.getenv("AWS_S3_ENDPOINT"))
        bucket = s3.Bucket( name = "maap-scientific-data" )
        regular_expression = re.compile( r'%s' %( self.args["pattern"] ) )
        for obj in bucket.objects.filter( Prefix = "edav_prepocessed_data/afrisar_onera" ):
            if regular_expression.match( obj.key ):
                filename = os.path.join( '/vsis3', "maap-scientific-data", obj.key )
                file_list.append( filename )
        return file_list,datasetlist

    def load(self, filename,datasetlist):
        """
        1- get spec from cmr with no geocoded granule_ur
        2- source -> file list element
        """
        try:
            if "kz" in filename:
                granule_ur=os.path.basename(filename).replace("_geo","")
            else:
                granule_ur=re.split(r"\_\w{1}\_geo",os.path.basename(filename))[0]+".tiff"
            params={
                "granule_ur[]":granule_ur
            }
            r=requests.get(self.granules_url,params=params)
            if r.status_code==200 and r.json()["hits"]==1:
                json_feature=r.json()["items"][0]
                data_json = {}
                send_json = {}
                if "kz" in filename:
                    concept_id=json_feature [ "meta" ][ "concept-id" ]+"geo"
                else:
                    concept_id=json_feature [ "meta" ][ "concept-id" ]+"geo"+os.path.basename(filename).split("_")[5]

                data_json [ concept_id ] = json_feature
                data_json[ concept_id ].update( { "source" : filename } )

                start_date=datetime.strptime( json_feature["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"] , "%Y-%m-%dT%H:%M:%S.%fZ" ).replace( microsecond=0 )
                end_date=datetime.strptime( json_feature["umm"]["TemporalExtent"]["RangeDateTime"]["EndingDateTime"]  , "%Y-%m-%dT%H:%M:%S.%fZ" ).replace( microsecond=0 )
                data_json[ concept_id ].update( { "date" :  start_date.strftime("%Y-%m-%dT%H:%M:%SZ")} )
                product=self.extractAttributes(json_feature["umm"]["AdditionalAttributes"],"Product Type")
                if "kz" in filename:
                    polarization=None
                else:
                    polarization=os.path.basename(filename).split("_")[4]
                if polarization:
                    data_json[ concept_id ].update( { "datasetId" : datasetlist [ 0 ][ "short_name" ]+"_geo" } )
                    data_json[ concept_id ].update( { "subDatasetId" : datasetlist [ 0 ][ "short_name" ]+"_geo_"+product+"_"+polarization+"_"+os.path.basename(filename).split("_")[5] } )
                else:
                    data_json[ concept_id ].update( { "datasetId" : datasetlist [ 0 ][ "short_name" ]+"_geo" } )
                    data_json[ concept_id ].update( { "subDatasetId" : datasetlist [ 0 ][ "short_name" ]+"_geo_"+product } )

                data_json[ concept_id ].update( { "grid" : True } )
                data_json[ concept_id ].update( { "gridType" : "Custom" } )

                data_json[ concept_id ].update( { "dataset_specification" : "%s/loader/specs.json?datasetId=%s"%(os.getenv("REFERENCE_HOST").strip("/"),datasetlist [ 0 ][ "short_name" ])})


                data_json[ concept_id ].update( { "dataset_type" : "Raster" } )
                data_json[ concept_id ].update( { "single_multiband" : "1" } )
                data_json[ concept_id ].update( { "dataset_dimension" : "3" } )
                data_json[ concept_id ].update( { "dataset_dimension_description" : "Long Lat Time" } )

                data_json[ concept_id ].update( { "dataset_description" : datasetlist [ 0 ][ "summary" ] } )
                data_json[ concept_id ].update( { "title" : datasetlist [ 0 ][ "title" ] } )
                data_json[ concept_id ].update( { "geometry" : json.dumps(self._getGeoJsonGeom(filename)) } )
                data_json[ concept_id ].update( { "timeExtent" : start_date.strftime("%Y-%m-%dT%H:%M:%SZ")+"/"+end_date.strftime("%Y-%m-%dT%H:%M:%SZ")} )



                scenetype,scene_value,subregion, subregion_value ,wcs_val= self.getSceneparams(self.extractAttributes(json_feature["umm"]["AdditionalAttributes"],"Scene Name"),datasetlist [ 0 ][ "title" ],["LA LOPE","MABOUNIE","MONDAH","RABI"])
                data_json[ concept_id ].update( { "SubRegion" : subregion } )
                data_json[ concept_id ].update( { "SubRegion_value" : subregion_value } )
                data_json[ concept_id ].update( { "SceneType" :  scenetype} )
                data_json[ concept_id ].update( { "SceneType_value" :  scene_value} )


                data_json[ concept_id ].update( { "Product" :  product} )
                data_json[ concept_id ].update( { "Product_value" :  self.data["AFRISAR_DLR"]["Product"][product]} )
                data_json[ concept_id ].update( {"wcs_val": wcs_val })
                data_json[ concept_id ].update( { "defaultViewMode":["band1"] } )
                data_json[ concept_id ].update( { "geolocated" : True } )
                send_json[ "metadata" ] = json.dumps(data_json)
                req=self.execute_request(send_json)
                if req.status_code == 200:
                    self.logger.info("Product %s inserted on pycsw"%(data_json[concept_id]["datasetId"]))
                else:
                    self.logger.error("Product %s not inserted on pycsw for error: %s"%(data_json[concept_id]["datasetId"],req.text))
                    req.raise_for_status()
        except Exception as e:
            self.fail_ing.info("CMR(GEO) feature with id %s not ingested"%(concept_id))
            self.notingested+=1

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

    def _getGeoJsonGeom(self,filename):
        gdal_info=gdal.Info(filename, format='json')
        epsg = int(gdal_info['coordinateSystem']['wkt'].rsplit('"EPSG",', 1)[-1].split(']]')[0])
        bbox=[]
        ll=gdal_info["cornerCoordinates"]["lowerLeft"]
        ul=gdal_info["cornerCoordinates"]["upperLeft"]
        lr=gdal_info["cornerCoordinates"]["lowerRight"]
        ur=gdal_info["cornerCoordinates"]["upperRight"]
        if epsg == 4326:
            ll_t=self._checkBbox(ll)
            ul_t=self._checkBbox(ul)
            lr_t=self._checkBbox(lr)
            ur_t=self._checkBbox(ur)
        else:
             inProj = Proj('epsg:%d' %( epsg ))
             ll_tx,ll_ty=transform(inProj,4326,ll[0],ll[1], always_xy=True)
             lr_tx,lr_ty=transform(inProj,4326,lr[0],lr[1], always_xy=True)
             ul_tx,ul_ty=transform(inProj,4326,ul[0],ul[1], always_xy=True)
             ur_tx,ur_ty=transform(inProj,4326,ur[0],ur[1], always_xy=True)
             ll_t=[ll_tx,ll_ty]
             lr_t=[lr_tx,lr_ty]
             ul_t=[ul_tx,ul_ty]
             ur_t=[ur_tx,ur_ty]

        bbox.append(ll_t)
        bbox.append(lr_t)
        bbox.append(ur_t)
        bbox.append(ul_t)
        bbox.append(ll_t)
        return {"type":"Polygon","coordinates":[bbox]}


    def getKey():
        return "afrisaronerageo"
