from loader_modules.default_loader import DefaultLoader
from settings import WCSPATH_ENDPOINT
import os
from osgeo import gdal
class BiomassL2Loader(DefaultLoader):

    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)

    def _getProductId(self, filename):
        """
        Return an unique Product ID, for example the filename.[required for records collection]
        """
        return os.path.dirname(filename).split("/")[-2]

    def _getSubDatasetId(self,filename):
        return os.path.basename(filename).split("_")[-1].rstrip(".tiff")


    def checkBandSubDset(self,filename,datasetId):
        """
        1- check if the filename has multiband or subdatasets
        2- return a structure like this: {"datasetId":{"subDatasetId":{subDatasetId infos},.....,"subDatasetIdn":{subDatasetIdn infos}}}
        """
        item_structure={}
        item_structure[datasetId]={}
        try:
            gdal_opt = gdal.InfoOptions(format="json",stats=True,computeMinMax=True)
            gdal_info= gdal.Info(filename,options=gdal_opt)
        except:
            gdal_opt = gdal.InfoOptions(format="json")
            gdal_info= gdal.Info(filename,options=gdal_opt)

        if "bands" in gdal_info and len(gdal_info["bands"])>0 :
            for index,band in enumerate(gdal_info["bands"]):
                subdatasetId = self._getSubDatasetId(filename)
                item_structure[datasetId].update(self.createSubDatasetStructure(filename,gdal_info,subdatasetId,index,datasetId))
            item_structure["gdalinfo"]=gdal_info

        return item_structure


    def createSubDatasetStructure( self , filename , gdalinfo , subDatasetId ,index,datasetId):
        item = {}
        item[ subDatasetId ] = { }
        item[ subDatasetId ]["datasets"] = {}
        item[ subDatasetId ]["records"] = {}
        item[ subDatasetId ]["datasets"][ "minValue" ] = [gdalinfo["bands"][index]["minimum"]] if "minimum" in gdalinfo["bands"][index] else [0]
        item[ subDatasetId ]["datasets"][ "maxValue" ] = [gdalinfo["bands"][index]["maximum"]] if "maximum" in gdalinfo["bands"][index] else [0]
        item[ subDatasetId ]["datasets"][ 'noDataValue' ] = gdalinfo['bands'][index]['noDataValue'] if "noDataValue" in gdalinfo['bands'][index] else None
        item[ subDatasetId ]["datasets"][ 'numberOfRecords' ] = 1
        item[ subDatasetId ]["datasets"][ 'units' ] = self._getUnit()
        item[ subDatasetId ]["datasets"][ 'unitsDescription' ] = self._getUnitDescription()
        item[ subDatasetId ]["datasets"][ 'Resolution' ] = [self._getResolution(gdalinfo)]
        item[ subDatasetId ]["records"][ "productPath" ] = self._getCachePath( filename )
        item[ subDatasetId ]["records"][ "band" ] = str( index )
        item[ subDatasetId ]["records"][ "mwcs" ] = self._getMWcsData( filename , gdalinfo )
        item[ subDatasetId ]["records"]["wcsPath"] =WCSPATH_ENDPOINT.replace("/v1","")+"/wcs?service=WCS&version=2.0.0&request=GetCoverage&coverageId=%s&subdataset=%s&subset=unix(%s)&scale=0.05&format=image/tiff"%(datasetId,subDatasetId,self._getDate(filename).strftime("%Y-%m-%dT%H:%M:%SZ"))
        if "offset" in gdalinfo["bands"][index]:
            item[ subDatasetId ]["records"][ "offsetData"] = self._getOffsetData(gdalinfo)
        if "scale" in gdalinfo["bands"][index]:
            item[ subDatasetId ]["records"][ "scale"] = self._getScaleData(gdalinfo)
        item[ subDatasetId ]["records"][ "minValue" ] = gdalinfo["bands"][index]["minimum"] if "minimum" in gdalinfo["bands"][index] else 0
        item[ subDatasetId ]["records"][ "maxValue" ] = gdalinfo["bands"][index]["maximum"] if "maximum" in gdalinfo["bands"][index] else 0
        item[ subDatasetId ]["records"][ 'dataType' ] = gdalinfo["bands"][index]["type"]
        item[ subDatasetId ]["records"][ 'noDataValue' ] = gdalinfo['bands'][index]['noDataValue'] if "noDataValue" in gdalinfo['bands'][index]else None

        return item

    def getKey():
        return "biomassL2@maap"
