import sys
import os
dir_path = os.path.dirname( os.path.realpath( __file__ ) )
previous_path = os.path.dirname( dir_path )
for d in [ dir_path, previous_path ]:
    if not d in sys.path:
        sys.path.append( d )
from loader_module.default_loader import CMRLoader
from loader.settings import init_environ
import re
import json
from osgeo import ogr,gdal
from datetime import datetime,timedelta
class AfrisarAGBLoader(CMRLoader):

    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)

        init_environ()

    def load(self, json_feature,datasetlist):
        data_json = {}
        send_json = {}
        concept_id=json_feature [ "meta" ][ "concept-id" ]
        data_json [ concept_id ] = json_feature
        for link in json_feature["umm"][ "RelatedUrls" ]:
            if re.match(self.regex_pattern,link["URL"]):
                data_json[ concept_id ].update( { "source" : self._getSource ( link [ "URL" ] ) } )
        try:
            start_date=datetime.strptime( json_feature["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"], "%Y-%m-%dT%H:%M:%S.%fZ" ).replace( microsecond=0 )
            end_date=datetime.strptime( json_feature["umm"]["TemporalExtent"]["RangeDateTime"]["EndingDateTime"], "%Y-%m-%dT%H:%M:%S.%fZ" ).replace( microsecond=0 )
            data_json[ concept_id ].update( { "dataset" : datasetlist [ 0 ][ "short_name" ] } )
            data_json[ concept_id ].update( { "date" :  start_date.strftime("%Y-%m-%dT%H:%M:%SZ")} )
            data_json[ concept_id ].update( { "subDatasetId" : datasetlist [ 0 ][ "short_name" ]+"_AGB" } )
            data_json[ concept_id ].update( { "grid" : False } )
            data_json[ concept_id ].update( { "gridType" : "Custom" } )
            data_json[ concept_id ].update( { "datasetId" : datasetlist [ 0 ][ "short_name" ] } )
    
    
            data_json[ concept_id ].update( { "dataset_type" : "Raster" } )
            data_json[ concept_id ].update( { "single_multiband" : "1" } )
            data_json[ concept_id ].update( { "dataset_dimension" : "3" } )
            data_json[ concept_id ].update( { "dataset_dimension_description" : "Long Lat Time" } )
    
            data_json[ concept_id ].update( { "dataset_description" : datasetlist [ 0 ][ "summary" ] } )
            data_json[ concept_id ].update( { "title" : datasetlist [ 0 ][ "title" ] } )
            data_json[ concept_id ].update( { "geometry" : self._getGeoJsonGeom(json_feature["umm"]["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["BoundingRectangles"]) } )
            data_json[ concept_id ].update( { "timeExtent" : start_date.strftime("%Y-%m-%dT%H:%M:%SZ")+"/"+end_date.strftime("%Y-%m-%dT%H:%M:%SZ")} )
            data_json[ concept_id ].update( { "geolocated" : True } )
    
            send_json[ "metadata" ] = json.dumps(data_json)
            req=self.execute_request(send_json)
            if req.status_code == 200:
                self.logger.info("Product %s inserted on pycsw"%(data_json[concept_id]["dataset"]))
            else:
                self.logger.error("Product %s not inserted on pycsw for error: %s"%(data_json[concept_id]["dataset"],req.text))
                req.raise_for_status()
        except Exception as e:
            print(e)
    
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

    def getKey():
        return "afrisarAGB"
