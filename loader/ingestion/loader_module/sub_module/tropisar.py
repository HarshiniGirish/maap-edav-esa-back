import sys
import os
dir_path = os.path.dirname( os.path.realpath( __file__ ) )
previous_path = os.path.dirname( dir_path )
for d in [ dir_path, previous_path ]:
    if not d in sys.path:
        sys.path.append( d )
from loader_module.default_loader import CMRLoader
from loader.settings import init_environ,DATASETS_SPEC
import re
import json
from datetime import datetime,timedelta
class TropisarLoader(CMRLoader):

    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        init_environ()
        with open(DATASETS_SPEC) as json_file:
            self.data = json.load(json_file)

    def load(self, json_feature,datasetlist):
        #this function create a json object in compliance with the metadata pwcsw schema
        try:
            data_json = {}
            send_json = {}
            concept_id=json_feature [ "meta" ][ "concept-id" ]
            data_json [ concept_id ] = json_feature
            if self.extractAttributes(json_feature["umm"]["AdditionalAttributes"],"Data Format") == "shapefile":
                data_json[ concept_id ].update({ "source" : self._getShpSource ( json_feature ["umm"][ "RelatedUrls" ] ) } )
            else:
                for link in json_feature ["umm"][ "RelatedUrls" ]:
                    if link["Type"]=="GET DATA":
                        data_json[ concept_id ].update( { "source" : self._getSource ( link [ "URL" ] ) } )

            start_date=datetime.strptime( json_feature["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"] , "%Y-%m-%dT%H:%M:%S.%fZ" ).replace( microsecond=0 )
            end_date=datetime.strptime( json_feature["umm"]["TemporalExtent"]["RangeDateTime"]["EndingDateTime"]  , "%Y-%m-%dT%H:%M:%S.%fZ" ).replace( microsecond=0 )
            data_json[ concept_id ].update( { "date" :  start_date.strftime("%Y-%m-%dT%H:%M:%SZ")} )
            data_json[ concept_id ].update( { "grid" : False } )
            data_json[ concept_id ].update( { "gridType" : "Custom" } )
            product=self.extractAttributes(json_feature["umm"]["AdditionalAttributes"],"Product Type")
            try:
                polarization=self.extractAttributes(json_feature["umm"]["AdditionalAttributes"],"Polarization")
            except:
                polarization=None
            if polarization:
                data_json[ concept_id ].update( { "datasetId" : datasetlist [ 0 ][ "short_name" ] } )
                data_json[ concept_id ].update( { "subDatasetId" : datasetlist [ 0 ][ "short_name" ]+"_"+product+"_"+polarization } )
            else:
                data_json[ concept_id ].update( { "datasetId" : datasetlist [ 0 ][ "short_name" ] } )
                data_json[ concept_id ].update( { "subDatasetId" : datasetlist [ 0 ][ "short_name" ]+"_"+product } )

            data_json[ concept_id ].update( { "dataset_specification" : "%s/loader/specs.json?datasetId=%s"%(os.getenv("REFERENCE_HOST").strip("/"),datasetlist [ 0 ][ "short_name" ])})
            data_json[ concept_id ].update( { "dataset_type" : "Raster" } )
            data_json[ concept_id ].update( { "single_multiband" : "1" } )
            data_json[ concept_id ].update( { "dataset_dimension" : "3" } )
            data_json[ concept_id ].update( { "dataset_dimension_description" : "Long Lat Time" } )

            data_json[ concept_id ].update( { "dataset_description" : datasetlist [ 0 ][ "summary" ].replace("'","") } )
            data_json[ concept_id ].update( { "title" : datasetlist [ 0 ][ "title" ] } )
            data_json[ concept_id ].update( { "geometry" : self._getGeoJsonGeom(json_feature["umm"]["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["GPolygons"]) } )
            data_json[ concept_id ].update( { "timeExtent" : start_date.strftime("%Y-%m-%dT%H:%M:%SZ")+"/"+end_date.strftime("%Y-%m-%dT%H:%M:%SZ")} )
            
            if product == "ROI":
                scene_name=json_feature ["umm"]["GranuleUR"].split("_")[-1]
                scenetype,scene_value,subregion,subregion_value,wcs_val = self.getSceneparams(scene_name,datasetlist [ 0 ][ "title" ],["ARBOCEL","KAW","NOURAGUES","PARACOU"])
            else:
                scenetype,scene_value,subregion,subregion_value,wcs_val = self.getSceneparams(self.extractAttributes(json_feature["umm"]["AdditionalAttributes"],"Scene Name"),datasetlist [ 0 ][ "title" ],["ARBOCEL","KAW","NOURAGUES","PARACOU"])
            data_json[ concept_id ].update( { "SceneType" :  scenetype} )
            data_json[ concept_id ].update( { "SceneType_value" :  scene_value} )
            data_json[ concept_id ].update( { "SubRegion" : subregion } )
            data_json[ concept_id ].update( { "SubRegion_value" : subregion_value } )

            product=self.extractAttributes(json_feature["umm"]["AdditionalAttributes"],"Product Type")
            self.fail_ing.info(product)
            data_json[ concept_id ].update( { "Product" :  product} )
            data_json[ concept_id ].update( { "Product_value" :  self.data[datasetlist [ 0 ][ "title" ]]["Product"][product]} )
            data_json[ concept_id ].update( { "defaultViewMode":["band1"] } )
            data_json[ concept_id ].update( { "geolocated" : False } )
            send_json[ "metadata" ] = json.dumps(data_json)
            req=self.execute_request(send_json)
            if req.status_code == 200:
                self.logger.info("Product %s inserted on pycsw"%(data_json[concept_id]["datasetId"]))
            else:
                self.logger.error("Product %s not inserted on pycsw for error: %s"%(data_json[concept_id]["datasetId"],req.text))
                req.raise_for_status()
        except Exception as e:
            self.fail_ing.info("CMR feature with id %s not ingested"%(concept_id))
            self.notingested+=1
    
    def _getSource(self,links,dformat):
        outSource=[]
        if dformat in ["shapefile","tiff"]:
            for link in links:
                if link["Type"] == "GET DATA":
                    outSource.append(link["URL"])
            return ",".join(outSource)


    def getKey():
        return "tropisar"


