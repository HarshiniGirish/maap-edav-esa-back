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
from osgeo import ogr
from loader.settings import init_environ , LOADER_ENDPOINT
import logging, logging.config




class CMRLoader:

    def __init__(self,args,logger,fail_ingestion):
        self.collections_url = "https://cmr.maap-project.org/search/collections.json"
        self.granules_url = "https://cmr.maap-project.org/search/granules.umm_json"
        self.args=vars(args)
        self.regex_pattern = re.compile(self.args["pattern"])
        self.start_registration = 0
        self.discovered_counter = 0
        self.loaded_counter = 0
        self.logger=logger
        self.fail_ing=fail_ingestion
        self.notingested=0

        self.checkArg()

    def checkArg(self):
        if self.args is None or self.args["dataset"] is None:
            raise ValueError("DatasetId not specified")

    def getList(self):
        filelist = []
        datasetlist = []
        param={
                "dataset_id" : self.args["dataset"]
                }
        r=requests.get( self.collections_url, params= param )
        self.logger.info("%s/%s"%(self.collections_url,param))
        if r.status_code == 200 :
            if len(r.json()["feed"][ "entry" ])>0:
                datasetlist.extend( r.json()["feed"][ "entry" ] )
                page_num = 1
                short_name = r.json()["feed"][ "entry" ][0]["short_name"]
                granule_param = {
                        "short_name": short_name,
                        "page_size":200
                        }
                while True:
                    granule_param["page_num"]=page_num
                    self.logger.info("%s/%s"%(self.granules_url,granule_param))
                    r_granule = requests.get( self.granules_url, params=granule_param)
                    if r_granule.status_code == 200:
                        if len(r_granule.json()["items"])>0:
                            json_granules = r_granule.json()["items"]
                            for feature in json_granules:
                                if self.extractAttributes(feature["umm"]["AdditionalAttributes"],"Data Format") in ["tiff", "shapefile","vrt","GeoTIFF"]: 
                                    filelist.append(feature)
                            page_num+=1
                        else:
                            break
                    else:
                        r_granule.raise_for_status()
            else:
                raise ValueError("Wrong DatasetId")
        else:
            r.raise_for_status()
        return filelist , datasetlist

    def start(self):
        self.start_time = time.time()
        file_list , datasetlist = self.getList()
        self.start_registration = time.time()
        self.discovered_counter = len( file_list )
        if self.discovered_counter > 0:
            self.pool = ThreadPool( processes = self.args["th_number"] )
            processes=[]
            for json_feature in file_list:
                processes.append(self.pool.apply_async(self._load,args=(json_feature,datasetlist,)))
            for ( j , p ) in enumerate(processes):
                res=p.get()
                self._printStatus(self.loaded_counter,len(file_list))
            self.pool.close()
            self.pool.join()
        self.fail_ing.info("Feature Not ingested from CMR %d.Have a look the fail_ingestion.log file to get all the not ingested feature ids"%(self.notingested))
        self.logger.info("CMR Loader completed")

    def _load(self,json_feature,datasetlist):
        try:
            self.load(json_feature,datasetlist)
            self.loaded_counter+=1
        except Exception as e:
            self.notingested+=1
            self.logger.exception(e)

    def _getGeoJsonGeom(self,boxes):
        #minX: %d, minY: %d, maxX: %d, maxY
        ring = ogr.Geometry(ogr.wkbLinearRing)
        try:
            points=boxes[0].split(" ")
            ring.AddPoint( float(points[1]) ,float(points[0]) )
            ring.AddPoint( float(points[3]) ,float(points[0]) )
            ring.AddPoint( float(points[3]) ,float(points[2]) )
            ring.AddPoint( float(points[1]) ,float(points[2]) )
            ring.AddPoint( float(points[1]) ,float(points[0]) )

            poly = ogr.Geometry(ogr.wkbPolygon)
            poly.AddGeometry(ring)
            poly.FlattenTo2D()
        except:
            for points in boxes[0]["Boundary"]["Points"]:
                ring.AddPoint( points["Longitude"],points["Latitude"] )
            poly = ogr.Geometry(ogr.wkbPolygon)
            poly.AddGeometry(ring)
            poly.FlattenTo2D()


        return poly.ExportToJson()
    
    def extractAttributes(self,attributes,value):
        for obj in attributes:
            if obj["Name"] == value:
                return obj["Values"][0]
    
    def _getShpSource(self,links):
        source=[]
        for link in links:
            if link["Type"] == "GET DATA":
                source.append(str(link["URL"]))
        return ",".join(source)
        
    def _getSource(self,source):
        if source.startswith("s3"):
            return source.replace("s3://","/vsis3/")
        elif source.startswith("https"):
            return source
        else:
            return source


    def extractAttributes(self,attributes,value):
        for obj in attributes:
            if obj["Name"] == value:
                return obj["Values"][0]
                
    def getSceneparams(self,value,dataset,regions):
        for subregion in regions:
            for obj in self.data[dataset]["SubRegion"][subregion]["scenes"]:
                for item in obj["scene_type_values"]:
                    for (k,v) in item.items():
                        if value == k:
                            return obj["scene_type"],value,subregion,self.data[dataset]["SubRegion"][subregion]["value"],v

    def execute_request(self,send_json):
        url = os.path.join(LOADER_ENDPOINT,"upload.json")
        req = requests.post(url,data=send_json,verify=False)
        return req

    def _printStatus(self,count_part,count_tot):
        if count_tot != 0:
            percent = int(count_part*100/count_tot)
            percent_bar = "=" * percent + " " * (100-percent)
            elapsed_time = time.time() -self.start_registration
            ETA = ( elapsed_time / count_part ) * count_tot - elapsed_time
            sys.stdout.write("\r [%s] - %d %% | %d/%d - %ds ETA" %( percent_bar, count_part*100/count_tot, count_part, count_tot, ETA  ))
            sys.stdout.flush()

class DefaultLoader(CMRLoader):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)

    def getKey():
        return "default"
