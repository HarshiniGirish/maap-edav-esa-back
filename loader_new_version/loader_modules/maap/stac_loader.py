from loader_modules.default_loader import DefaultLoader
from settings import (
    AWS_BUCKET,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_S3_ENDPOINT,
    WCSPATH_ENDPOINT,
    init_environ
)
import boto3
import os
from osgeo import gdal,ogr
import json
from datetime import datetime
import shlex
import subprocess
import time
import requests
import pymongo
import re

class STACLoader(DefaultLoader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataset_info = None
        self.stac_url = "https://stac.maap-project.org"
        init_environ({"AWS_BUCKET":AWS_BUCKET,"AWS_ACCESS_KEY_ID":AWS_ACCESS_KEY_ID,"AWS_SECRET_ACCESS_KEY":AWS_SECRET_ACCESS_KEY,"AWS_S3_ENDPOINT":AWS_S3_ENDPOINT})

    def _chechArgs(self):
        """
        Check specific module requirements
        """
        if self.args.source is None:
            raise ValueError(
                'Source "%s" not specified or not valid' % (self.args.source)
            )

    def getList(self):
        feature_list = []
        params={}
        noNext=False
        items_url = os.path.join(self.stac_url,"collections",self.args.source,"items")
        if self.args.maxrecords:
            params.update({"limit":self.args.maxrecords})
        while True:
            items = requests.get(items_url)
            items.raise_for_status()
            if items.status_code == 200:
                if len(items.json()["features"]) > 0:
                    feature_list.extend(items.json()["features"])
                for link in items.json()["links"]:
                    if link["rel"] == "next":
                        items_url = link["href"]
                        noNext=False
                        break
                    else:
                        noNext=True
                if noNext:
                    break

            else:
                items.raise_for_status()
        return feature_list

    def createVrt(self, vrt_filename, filename):
        gdtr_opt = gdal.TranslateOptions(format="VRT", outputSRS="EPSG:4326")
        gdal.Translate(vrt_filename, filename.replace("s3://","/vsis3/"), options=gdtr_opt)

    def _getDatasetInfo(self):
        dataset = requests.get(os.path.join(self.stac_url,"collections",self.args.source))
        dataset.raise_for_status()
        self.dataset_info = dataset.json()

    def checkBandSubDset(self, feature, datasetId):
        """
        1- check if the filename has multiband or subdatasets
        2- return a structure like this: {"datasetId":{"subDatasetId":{subDatasetId infos},.....,"subDatasetIdn":{subDatasetIdn infos}}}
        """
        item_structure = {}
        item_structure[datasetId] = {}
        if not self.dataset_info:
            self._getDatasetInfo()
        for (subdataset,v) in feature["assets"].items():
            if re.match(r'%s'%(self.args.pattern),v["href"]):
                subdatasetId = f"{datasetId}_{subdataset}"
                gdal_opt = gdal.InfoOptions(format="json", stats=True, computeMinMax=True)
                gdal_info_vr = gdal.Info(v["href"].replace("s3://","/vsis3/"), options=gdal_opt)
                item_structure[datasetId].update(
                        self.createSubDatasetStructure(
                            feature,
                            v,
                            gdal_info_vr,
                            subdatasetId,
                            0,
                            datasetId
                        )
                    )
                item_structure["gdalinfo"] = gdal_info_vr
        return item_structure

    def _getVrt(self, vrt_filename):
        command = "cat %s" % (vrt_filename)
        args = shlex.split(command)
        p1 = subprocess.Popen(args, stdout=subprocess.PIPE, shell=False)
        output, err = p1.communicate()
        vrt = output.decode("utf-8").replace("\n", "").replace("&quot;", '"')
        return vrt

    def createSubDatasetStructure(
        self, feature,v, gdalinfo, subDatasetId, index, datasetId):
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
        item[subDatasetId]["records"]["productPath"] = v["href"].replace(f"s3://{AWS_BUCKET}","")
        item[subDatasetId]["records"]["band"] = str(index)
        item[subDatasetId]["records"]["mwcs"] = self._getMWcsData(v["href"].replace("s3://","/vsis3/"), gdalinfo)
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
        return item

    def _getDate(self,feature):
        return  datetime.strptime(feature["properties"]["datetime"].split("+")[0],"%Y-%m-%dT%H:%M:%S")

    def _createDictInsertRecord(self, feature, item_structure_list, datasetId,document):
        record = {}
        record["productId"] = feature["id"]
        record["productDate"] = self._getDate(feature)
        record["geometry"] = feature["geometry"]
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

    def _getSize(self, feature):
        s3 = boto3.resource(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url="https://" + AWS_S3_ENDPOINT,
        )
        bucket = s3.Bucket(name=AWS_BUCKET)
        for obj in bucket.objects.filter(
            Prefix=features.replace("/vsis3/%s/" % (AWS_BUCKET), "")
        ):
            return obj.size


    def checkDuplicates(self,feature):
        unique_index = {
            'productId':feature["id"],
            'productDate':datetime.strptime(feature["properties"]["datetime"].split("+")[0],"%Y-%m-%dT%H:%M:%S")
        }
        action = 'load'
        if not self.args.reload and self.db[self._getDatasetName( feature )].count_documents( unique_index ) > 0:
            self.skipped_products_counter += 1
            action = 'skip'
        return {
            'action': action,
            'unique_index':  unique_index
        }
    def load(self, feature):
        """
        1- creo,se non presente, collection con collection_name=datasetId
        2- check band/subdataset con return della struttura json i.e.: ["subDatasetId":{subdatsetPointer:"---"},.........,"subDatasetIdn"{}]
        3- inserisco records,temporalbar,ingestion analytics
        4- aggiorno struttura collection datasets per il commit
        """
        load_timer = time.time()
        datasetId = self._getDatasetName(feature)
        self._checkCollection(datasetId)
        item_structure_list = self.checkBandSubDset(feature, datasetId)
        duplicate_test = self.checkDuplicates(feature)
        if duplicate_test["action"] == "skip":
            self.logger.info(
                'Registration of "%s" skipped in %f s'
                % (feature["id"], time.time() - load_timer)
            )
        else:
            time.sleep(0.01)
            self.commit()  # TODO
            unique_index = duplicate_test["unique_index"]
            self.logger.info('Start registration of "%s"' % (feature["id"]))
            feature_time = time.time()
            document = self.db[datasetId].find_one(unique_index)
            rec_doc = self._createDictInsertRecord(
                feature, item_structure_list, datasetId,document
            )
            try:
                record_id = self.db[datasetId].update_one(
                    unique_index, {"$set": rec_doc}, upsert=True
                )
            except pymongo.errors.WriteError as we:
                self.logger.exception(str(we))
                if "extract geo keys" in str(we):
                    wrong_geom = ogr.CreateGeometryFromJson(
                        json.dumps(rec_doc["geometry"])
                    )
                    rec_bbox = self._getEnvelope(wrong_geom)
                    rec_bbox.FlattenTo2D()
                    rec_doc.update({"geometry": json.loads(rec_bbox.ExportToJson())})
                    record_id = self.db[datasetId].update_one(
                        unique_index, {"$set": rec_doc}, upsert=True
                    )
                    self.logger.debug("Recovery record geometry success")
                else:
                    rec_doc = {}
            # datasets
            dataset_doc = self.db.datasets.find_one({"datasetId":datasetId})
            rec_dat = self._createDictInsertDataset(
                item_structure_list, datasetId, feature,dataset_doc
            )
            self._createDatasetList(rec_dat)
            feature_time_end = time.time() - feature_time
            # temporalbar & ingestionAnalytics
            temporalbar_id = self.insertTemporalBar(datasetId, rec_doc["productDate"])
            #for k, v in item_structure_list[datasetId].items():
            #    self.ingestionAnalyticsList(datasetId, feature, feature_time_end, k)

        self.logger.info(
            'Registration of "%s" completed in %f s'
            % (feature["id"], time.time() - load_timer)
        )
        self.loaded_products_counter += 1

    def _createDictInsertDataset(self, item_list, datasetId, filename,dataset_doc):
        datasets = super()._createDictInsertDataset(item_list, datasetId, filename,dataset_doc)
        datasets["envs"] = (
            "AWS_ACCESS_KEY_ID=%s;AWS_SECRET_ACCESS_KEY=%s;AWS_S3_ENDPOINT=%s;CPL_DEBUG=ON"
            % (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_S3_ENDPOINT)
        )
        return datasets

    def _getServices(self,datasetId):
        ops_service = {
                "href":"%s/search/%s"%(WCSPATH_ENDPOINT.replace("wcs","ops"),datasetId),
                "ref":"discovery-service",
                "title":"search",
                "type":"application/json"
                }
        if ops_service not in self.descriptionDoc["services"]:
            return self.descriptionDoc["services"].append(ops_service)
        else:
            return self.descriptionDoc["services"]


    def getKey():
        return "maap@stac"

