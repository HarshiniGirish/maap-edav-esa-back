import sys
import os
dir_path = os.path.dirname( os.path.realpath( __file__ ) )
previous_path = os.path.dirname( dir_path )
for d in [ dir_path, previous_path ]:
    if not d in sys.path:
        sys.path.append( d )
import argparse
from datetime import datetime, timedelta
import time
import re
import requests
import json
import xml.etree.ElementTree as ET
from multiprocessing.pool import ThreadPool
from osgeo import ogr,gdal
from pyproj import Proj,transform

from loader.settings import init_environ
from loader_module.default_loader import CMRLoader
import logging, logging.config


image_table = {
        "14043":"lopenp",
        "14044":"twenty",
        "14045":"fortym",
        "14046":"sixtym",
        "14047":"eighty",
        "14048":"hundre",
        "14049":"htwent",
        "14050":"hsixty",
        "14051":"height"
        }

date_table = {
        "16008":"2016-02-25",
        "16015":"2016-03-08"
        }

class AfrisarCoregSLcLoader(CMRLoader):

    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)

        init_environ()


    def load(self, json_feature,datasetlist):
        data_json = {}
        send_json = {}
        concept_id=json_feature [ "meta" ][ "concept-id" ]
        data_json [ concept_id ] = json_feature
        for link in json_feature ["umm"][ "RelatedUrls" ]:
            if re.match(self.regex_pattern,link["URL"]):
                data_json[ concept_id ].update( { "source" : self._getSource ( link [ "URL" ] ) } )

        try:
            start_date=datetime.strptime( json_feature["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"] , "%Y-%m-%dT%H:%M:%S.%fZ" ).replace( microsecond=0 )
            end_date=datetime.strptime( json_feature["umm"]["TemporalExtent"]["RangeDateTime"]["EndingDateTime"]  , "%Y-%m-%dT%H:%M:%S.%fZ" ).replace( microsecond=0 )
            
            image,image_value, date,date_value= self._getImageDate( json_feature["meta"][ "native-id" ])
            
            data_json[ concept_id ].update( { "dataset" : datasetlist [ 0 ][ "short_name" ] } )
            data_json[ concept_id ].update( { "date" : date } )
            data_json[ concept_id ].update( { "image" : image } )
            data_json[ concept_id ].update( { "image_value" : image_value } )
            data_json[ concept_id ].update( { "plan" : date } )
            data_json[ concept_id ].update( { "plan_value" : date_value } )
            data_json[ concept_id ].update( { "subDatasetId" : datasetlist [ 0 ][ "short_name" ]+"_Amplitude" } )
            data_json[ concept_id ].update( { "grid" : True } )
            data_json[ concept_id ].update( { "gridType" : "Custom" } )
            data_json[ concept_id ].update( { "datasetId" : datasetlist [ 0 ][ "short_name" ] })
    
    
            data_json[ concept_id ].update( { "dataset_type" : "Raster" } )
            data_json[ concept_id ].update( { "single_multiband" : "1" } )
            data_json[ concept_id ].update( { "dataset_dimension" : "3" } )
            data_json[ concept_id ].update( { "dataset_dimension_description" : "Long Lat Time" } )
    
            data_json[ concept_id ].update( { "dataset_description" : datasetlist [ 0 ][ "summary" ] } )
            data_json[ concept_id ].update( { "title" : datasetlist [ 0 ][ "title" ] } )
            data_json[ concept_id ].update( { "geometry" : self._getGeoJsonGeom(json_feature["umm"]["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["BoundingRectangles"])} )
            data_json[ concept_id ].update( { "timeExtent" : start_date.strftime("%Y-%m-%dT%H:%M:%SZ")+"/"+end_date.strftime("%Y-%m-%dT%H:%M:%SZ")} )
            data_json[ concept_id ].update( { "geolocated" : True } )
            data_json[ concept_id ].update( { "defaultViewMode":["band1"] } )
    
            send_json[ "metadata" ] = json.dumps(data_json)
            req=self.execute_request(send_json)
            if req.status_code == 200:
                self.logger.info("Product %s inserted on pycsw"%(data_json[concept_id]["dataset"]))
            else:
                self.logger.error("Product %s not inserted on pycsw for error: %s"%(data_json[concept_id]["dataset"],req.text))
                req.raise_for_status()
        except Exception as e:
            print(e)
            pass

    def _getSource(self,source):
        if source.startswith("s3"):
            return source.replace("s3://","/vsis3/")
        elif source.startswith("https"):
            return source
        else:
            return source

    def _getGeoJsonGeom(self,boxes):
        ring = ogr.Geometry(ogr.wkbLinearRing)
        ring.AddPoint( float(boxes[0]["WestBoundingCoordinate"]) ,float(boxes[0]["SouthBoundingCoordinate"]) )
        ring.AddPoint( float(boxes[0]["EastBoundingCoordinate"]) ,float(boxes[0]["SouthBoundingCoordinate"]) )
        ring.AddPoint( float(boxes[0]["EastBoundingCoordinate"]) ,float(boxes[0]["NorthBoundingCoordinate"]) )
        ring.AddPoint( float(boxes[0]["WestBoundingCoordinate"]) ,float(boxes[0]["NorthBoundingCoordinate"]) )
        ring.AddPoint( float(boxes[0]["WestBoundingCoordinate"]) ,float(boxes[0]["SouthBoundingCoordinate"]) )
        poly = ogr.Geometry(ogr.wkbPolygon)
        poly.AddGeometry(ring)
        poly.FlattenTo2D()
        return poly.ExportToJson()


    def _getImageDate(self,title):
        num_image = title.split("_")[4]
        num_date = title.split("_")[5]
        image = self._compare(num_image,"image")
        date = self._compare(num_date,"date")+"T00:00:00Z"
        return image,num_image,date,num_date

    def _compare(self,obj,obj_type):
        if obj_type == "image":
            try:
                return image_table[obj]
            except Exception as e:
                self.logger.exception("Image table obj not Found")
                raise ValueError("Image table obj not Found")
        elif obj_type == "date":
            try:
                return date_table[obj]
            except Exception as e:
                self.logger.exception("Date table obj not Found")
                raise ValueError("Date table obj not Found")
        else:
            raise ValueError("object type empty or wrong")



    def getKey():
        return "afrisarcoregscl"
