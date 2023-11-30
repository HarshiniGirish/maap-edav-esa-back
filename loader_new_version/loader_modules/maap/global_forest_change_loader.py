from loader_modules.maap.stac_loader import STACLoader
from settings import TMP_VRT_DIR,WCSPATH_ENDPOINT,AWS_BUCKET
from osgeo import gdal
import os
import re
import shlex
import subprocess
class BIOSARLoader(STACLoader):

    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.tmp = TMP_VRT_DIR

    def _createDictInsertRecord(self, feature, item_structure_list, datasetId,document):
        record = {}
        record["productId"] = feature["id"]
        record["productDate"] = self._getDate(feature)
        record["geometry"] =self._getGeom(self._getBbox(item_structure_list["gdalinfo"]))
        record["insertDate"] = self._getInsertDate()
        record["status"] = self._getStatus()
        record["source"] = f"s3://{AWS_BUCKET}"
        record["subDatasets"] = {}
        for sbd, val in item_structure_list[datasetId].items():
            record["subDatasets"].update({sbd: val["records"]})
        if document:
            for (sbd,value) in document["subDatasets"].items():
                record[ "subDatasets" ].update({sbd:value})
        return record


    def getKey():
        return "globalforestchange@stacmaap"

