from loader_modules.maap.stac_loader import STACLoader
from settings import TMP_VRT_DIR,WCSPATH_ENDPOINT
from osgeo import gdal
import os
import re
import shlex
import subprocess
class BIOSARLoader(STACLoader):

    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.tmp = TMP_VRT_DIR

    def createVrt(self, vrt_filename, filename,bbox):
        gdtr_opt = gdal.TranslateOptions(format="VRT", outputSRS="EPSG:4326",outputBounds=[bbox[0],bbox[3],bbox[2],bbox[1]])
        gdal.Translate(vrt_filename, filename.replace("https","/vsicurl/https"), options=gdtr_opt)

    def _getVrt(self, vrt_filename):
        command = "cat %s" % (vrt_filename)
        args = shlex.split(command)
        p1 = subprocess.Popen(args, stdout=subprocess.PIPE, shell=False)
        output, err = p1.communicate()
        vrt = output.decode("utf-8").replace("\n", "").replace("&quot;", '"')
        return vrt

    def checkBandSubDset(self, feature, datasetId):
        """
        1- check if the filename has multiband or subdatasets
        2- return a structure like this: {"datasetId":{"subDatasetId":{subDatasetId infos},.....,"subDatasetIdn":{subDatasetIdn infos}}}
        """
        item_structure = {}
        item_structure[datasetId] = {}
        print(feature)
        if not self.dataset_info:
            self._getDatasetInfo()
        for (subdataset,v) in feature["assets"].items():
            if v["type"] == "image/tiff":
                print(v["href"])
                subdatasetId = f"{datasetId}_{subdataset.replace('.','')}"
                vrt_filename = os.path.join(self.tmp,v["href"].split("/")[-1].replace(".tiff",".vrt"))
                self.createVrt(vrt_filename,v["href"],feature["bbox"])
                gdal_opt = gdal.InfoOptions(format="json", stats=True, computeMinMax=True)
                gdal_info_vr = gdal.Info(vrt_filename, options=gdal_opt)
                item_structure[datasetId].update(
                        self.createSubDatasetStructure(
                            feature,
                            v,
                            gdal_info_vr,
                            subdatasetId,
                            0,
                            datasetId,
                            vrt_filename
                        )
                    )
                item_structure["gdalinfo"] = gdal_info_vr
        return item_structure

    def createSubDatasetStructure(self, feature,v, gdalinfo, subDatasetId, index, datasetId,vrt_filename):
        item = {}
        item[subDatasetId] = {}
        item[subDatasetId]["datasets"] = {}
        item[subDatasetId]["records"] = {}
        item[subDatasetId]["datasets"]["minValue"] = []
        item[subDatasetId]["datasets"]["maxValue"] = []
        try:
            for band in v["raster:bands"]:
                item[subDatasetId]["datasets"]["minValue"].append(band["statistics"]["minimum"])
                item[subDatasetId]["datasets"]["maxValue"].append(band["statistics"]["maximum"])
        except:
            for band in gdalinfo["bands"]:
                item[subDatasetId]["datasets"]["minValue"].append(band["minimum"])
                item[subDatasetId]["datasets"]["maxValue"].append(band["maximum"])
        for band in gdalinfo["bands"]:
            item[subDatasetId]["datasets"]["noDataValue"] = gdalinfo["bands"][index]["noDataValue"] if "noDataValue" in gdalinfo["bands"][index] else None
            item[subDatasetId]["records"]["noDataValue"] = (
                gdalinfo["bands"][0]["noDataValue"]
                if "noDataValue" in gdalinfo["bands"][0]
                else None
            )
        item[subDatasetId]["records"]["minValue"] = min(item[subDatasetId]["datasets"]["minValue"])
        item[subDatasetId]["records"]["maxValue"] = max(item[subDatasetId]["datasets"]["maxValue"])
        try:
            item[subDatasetId]["records"]["dataType"] = v["raster:bands"][0]["data_type"]
        except:
            gdalinfo["bands"][index]["type"]
        item[subDatasetId]["datasets"]["numberOfRecords"] = 1
        item[subDatasetId]["datasets"]["description"] = self.dataset_info["description"]
        item[subDatasetId]["datasets"]["minDate"] = self.dataset_info["extent"]["temporal"]["interval"][0][0]
        item[subDatasetId]["datasets"]["minDate"] = self.dataset_info["extent"]["temporal"]["interval"][0][1]
        item[subDatasetId]["datasets"]["Resolution"] = [self._getResolution(gdalinfo)]
        item[subDatasetId]["records"]["productPath"] = "vrt"
        item[subDatasetId]["records"]["vrt"] = self._getVrt(vrt_filename)
        item[subDatasetId]["records"]["band"] = str(index)
        item[subDatasetId]["records"]["mwcs"] = self._getMWcsData(vrt_filename, gdalinfo)
        item[subDatasetId]["records"]["wcsPath"]=f"{WCSPATH_ENDPOINT}?service=WCS&version=2.0.0&request=GetCoverage&coverageId={datasetId}&subdataset={subDatasetId}&subset=unix({feature['properties']['datetime'].split('+')[0]})&scale=0.05&format=image/tiff"
        item[subDatasetId]["records"]["downloadLink"] = v["href"]
        try:
            if "offset" in v["raster:bands"][0]:
                item[subDatasetId]["records"]["offsetData"] = v["raster:bands"][0]["offset"]
        except:
            item[subDatasetId]["records"]["offsetData"] =0
        try:
            if "scale" in v["raster:bands"][0]:
                item[subDatasetId]["records"]["scale"] = v["raster:bands"][0]["scale"]
        except:
             item[subDatasetId]["records"]["scale"] =1

        os.remove(vrt_filename)
        return item

    def _getServices(self,datasetId):
        ops_service = {
                "href":"%s/search/%s"%(WCSPATH_ENDPOINT.replace("das-esa-wcs","das-esa-ops"),datasetId),
                "ref":"discovery-service",
                "title":"search",
                "type":"application/json"
                }
        if ops_service not in self.descriptionDoc["services"]:
            return self.descriptionDoc["services"].append(ops_service)
        else:
            return self.descriptionDoc["services"]

    def _createDictInsertRecord(self, feature, item_structure_list, datasetId,document):
        record = {}
        record["productId"] = feature["id"]
        record["productDate"] = self._getDate(feature)
        record["geometry"] = feature["geometry"]
        record["insertDate"] = self._getInsertDate()
        record["status"] = self._getStatus()
        record["source"] = "vrt"
        record["subDatasets"] = {}
        for sbd, val in item_structure_list[datasetId].items():
            record["subDatasets"].update({sbd: val["records"]})
        if document:
            for (sbd,value) in document["subDatasets"].items():
                record[ "subDatasets" ].update({sbd:value})
        return record

    def _createDictInsertDataset(self, item_list, datasetId, filename,dataset_doc):
        datasets = super()._createDictInsertDataset(item_list, datasetId, filename,dataset_doc)
        return datasets

    def getKey():
        return "biosar1@stacmaap"

