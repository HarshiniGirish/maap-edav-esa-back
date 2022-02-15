#include "includes.h"
#include "datatypes.h"

struct TileMatrixSet {
	int 	id;
	double 	scale_denominator;
	double	pixel_size;
	int	width;
	int	height;
	int	TileWidth;
	int	TileHeight;

};





// Tiles utilis OSM
double tilex2lon_EPSG_3857(int x, int z) { return x / (double)(1 << z) * 360.0 - 180; }	
double tiley2lat_EPSG_3857(int y, int z) { double n = M_PI - 2.0 * M_PI * y / (double)(1 << z);	return 180.0 / M_PI * atan(0.5 * (exp(n) - exp(-n))); }


// Tiles utils EPSG:4326
double tilex2lon_EPSG_4326(int x, int z) { return (  360.0     / ( (double)(1 << z) * 2 ) * (double)x - 180.0); }
double tiley2lat_EPSG_4326(int y, int z) { return ( 90 - 180.0 /   (double)(1 << z)       * (double)y  ); }


int fexists(const char *fname);
int unencode(char *src, char *dest);
OGRErr ImportFromEPSG(  OGRSpatialReferenceH *hSRS, int nCode );
const char *datatimeStringFromUNIX(struct tm *ts);
int GoodbyeMessage(struct info *, const char *, ... );
int addStatToPush( struct info *info, char *key, char *value, int type);

//------------------------------------------------------------------------------------------
int WMTSErrorMessage(request_rec *r, int mode){
	char string[MAX_STR_LEN]; bzero(string, MAX_STR_LEN);
	strcat(string, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        strcat(string, "<ows:ExceptionReport xmlns:ows=\"http://www.opengis.net/ows/1.1\"\n");
        strcat(string, "       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n");
        strcat(string, "       xsi:schemaLocation=\"http://www.opengis.net/ows/1.1 http://schemas.opengis.net/ows/1.1.0/owsExceptionReport.xsd\"\n");
        strcat(string, "       version=\"1.0.0\" xml:lang=\"en\">\n");

	if ( mode == OperationNotSupported  ){
        strcat(string, "       <ows:Exception exceptionCode=\"OperationNotSupported\">\n");
        strcat(string, "               <ows:ExceptionText>Request is for an operation that is not supported by this server</ows:ExceptionText>\n");
	} 
	if ( mode == MissingParameterValue ){
        strcat(string, "       <ows:Exception exceptionCode=\"MissingParameterValue\">\n");
        strcat(string, "               <ows:ExceptionText>Operation request does not include a parameter value</ows:ExceptionText>\n");
	} 
	if ( mode == InvalidParameterValue ){
        strcat(string, "       <ows:Exception exceptionCode=\"InvalidParameterValue\">\n");
        strcat(string, "               <ows:ExceptionText>Operation request contains an invalid parameter value</ows:ExceptionText>\n");
	} 


        strcat(string, "       </ows:Exception>\n");
        strcat(string, "</ows:ExceptionReport>\n");


	ap_custom_response( r, HTTP_BAD_REQUEST, string);
	 
	return TRUE;
}

//------------------------------------------------------------------------------------------

int md5_tile(struct info *info, char *md5){
	request_rec  *r = info->r;

        MD5_CTX mdContext; MD5_Init (&mdContext);
	extern long int GeomPOSTSize;
        unsigned char *c = (unsigned char *)malloc(MD5_DIGEST_LENGTH);

				  	 MD5_Update (&mdContext, info->uri, 	     strlen(info->uri) 		);
	if (info->query_string != NULL ) MD5_Update (&mdContext, info->query_string, strlen(info->query_string) );

	if ( ! strcmp(r->method, "POST") ) {
        	apr_array_header_t *pairs = NULL;

		if ( info->pairs == NULL ) {
		        if ( GeomPOSTSize > 0 ) ap_parse_form_data(r, NULL, &info->pairs, -1, GeomPOSTSize );
		        else                    ap_parse_form_data(r, NULL, &info->pairs, -1, 1024 * 1024 * 3 );
		}

		pairs = apr_array_copy(r->pool, info->pairs);

		while (pairs && !apr_is_empty_array(pairs)) {
	                apr_off_t       len      = 0;
	                apr_size_t      size     = 0;
	                char            *buffer  = NULL;
	                ap_form_pair_t  *pair   = (ap_form_pair_t *) apr_array_pop(pairs);
	
	                apr_brigade_length(pair->value, 1, &len);       size = (apr_size_t) len;
	                buffer = apr_palloc(r->pool, size + 1);         apr_brigade_flatten(pair->value, buffer, &size); buffer[len] = '\0';
			MD5_Update (&mdContext, pair->name, 	strlen(pair->name));
	                MD5_Update (&mdContext, buffer, 	size);
	        } 
	}


        MD5_Final (c,&mdContext);

        for(int i = 0; i < MD5_DIGEST_LENGTH; i++) { sprintf(md5 + ( i * 2 ), "%02x", c[i]); }

        return TRUE;
}


//------------------------------------------------------------------------------------------


int WMSErrorMessage(request_rec *r, int mode, char *version ){
	char string[MAX_STR_LEN]; bzero(string, MAX_STR_LEN);

	strcat(string, "<?xml version='1.0' encoding=\"ISO-8859-1\" standalone=\"no\" ?>\n");

	if		( ! strcmp(version, "1.1.1") ) {
	strcat(string, "<!DOCTYPE ServiceExceptionReport SYSTEM \"http://schemas.opengis.net/wms/1.1.1/exception_1_1_1.dtd\">\n");
	strcat(string, "<ServiceExceptionReport version=\"1.1.1\">\n");
	strcat(string, "	<ServiceException code=\"LayerNotQueryable\">\n");
	strcat(string, "		msWMSFeatureInfo(): WMS server error. Requested layer(s) are not queryable.\n");
	strcat(string, "	</ServiceException>\n");
	strcat(string, "</ServiceExceptionReport>\n");
	} else if 	( ! strcmp(version, "1.3.0") ) {
        strcat(string, "<ServiceExceptionReport xmlns=\"http://www.opengis.net/ogc\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" version=\"1.3.0\" xsi:schemaLocation=\"http://www.opengis.net/ogchttp://schemas.opengis.net/wms/1.3.0/exceptions_1_3_0.xsd\">\n");
        strcat(string, "	<ServiceException code=\"LayerNotQueryable\">\n");
        strcat(string, "	msWMSFeatureInfo(): WMS server error. Requested WMS layer(s) are not queryable: type or connection differ\n");
        strcat(string, "	</ServiceException>\n");
        strcat(string, "</ServiceExceptionReport>\n");
	} else return FALSE;

	ap_custom_response( r, HTTP_BAD_REQUEST, string);

	return TRUE;
}
//------------------------------------------------------------------------------------------
int WMTSGetCapabilities(struct info *info){
	request_rec  *r = info->r; int i, j;
	struct TileMatrixSet CRS84[19];
	struct TileMatrixSet CRS3857[19];
	
	CRS84[0].id  = -1;      CRS84[0].scale_denominator  = 559082264.0287178;      CRS84[0].pixel_size  = 1.40625000000000;       CRS84[0].width  = 1; 	CRS84[0].height  = 1;	   CRS84[0].TileWidth  = CRS84[0].TileHeight  = 256;      	
	CRS84[1].id  = 0;       CRS84[1].scale_denominator  = 279541132.0143589;      CRS84[1].pixel_size  = 0.703125000000000;      CRS84[1].width  = 2;       CRS84[1].height  = 1;      CRS84[1].TileWidth  = CRS84[1].TileHeight  = 256;
	CRS84[2].id  = 1;       CRS84[2].scale_denominator  = 139770566.0071794;      CRS84[2].pixel_size  = 0.351562500000000;      CRS84[2].width  = 4;       CRS84[2].height  = 2;      CRS84[2].TileWidth  = CRS84[2].TileHeight  = 256;
	CRS84[3].id  = 2;       CRS84[3].scale_denominator  = 69885283.00358972;      CRS84[3].pixel_size  = 0.175781250000000;      CRS84[3].width  = 8;       CRS84[3].height  = 4;      CRS84[3].TileWidth  = CRS84[3].TileHeight  = 256;
	CRS84[4].id  = 3;       CRS84[4].scale_denominator  = 34942641.50179486;      CRS84[4].pixel_size  = 8.78906250000000e-2;    CRS84[4].width  = 16;      CRS84[4].height  = 8;      CRS84[4].TileWidth  = CRS84[4].TileHeight  = 256;
	CRS84[5].id  = 4;       CRS84[5].scale_denominator  = 17471320.75089743;      CRS84[5].pixel_size  = 4.39453125000000e-2;    CRS84[5].width  = 32;      CRS84[5].height  = 16;     CRS84[5].TileWidth  = CRS84[5].TileHeight  = 256;
	CRS84[6].id  = 5;       CRS84[6].scale_denominator  = 8735660.375448715;      CRS84[6].pixel_size  = 2.19726562500000e-2;    CRS84[6].width  = 64;      CRS84[6].height  = 32;     CRS84[6].TileWidth  = CRS84[6].TileHeight  = 256;
	CRS84[7].id  = 6;       CRS84[7].scale_denominator  = 4367830.187724357;      CRS84[7].pixel_size  = 1.09863281250000e-2;    CRS84[7].width  = 128;     CRS84[7].height  = 64;     CRS84[7].TileWidth  = CRS84[7].TileHeight  = 256;
	CRS84[8].id  = 7;       CRS84[8].scale_denominator  = 2183915.093862179;      CRS84[8].pixel_size  = 5.49316406250000e-3;    CRS84[8].width  = 256;     CRS84[8].height  = 128;    CRS84[8].TileWidth  = CRS84[8].TileHeight  = 256;
	CRS84[9].id  = 8;       CRS84[9].scale_denominator  = 1091957.546931089;      CRS84[9].pixel_size  = 2.74658203125000e-3;    CRS84[9].width  = 512;     CRS84[9].height  = 256;    CRS84[9].TileWidth  = CRS84[9].TileHeight  = 256;
	CRS84[10].id = 9;       CRS84[10].scale_denominator = 545978.7734655447;      CRS84[10].pixel_size = 1.37329101562500e-3;    CRS84[10].width = 1024;    CRS84[10].height = 512;    CRS84[10].TileWidth = CRS84[10].TileHeight = 256;
	CRS84[11].id = 10;      CRS84[11].scale_denominator = 272989.3867327723;      CRS84[11].pixel_size = 6.86645507812500e-4;    CRS84[11].width = 2048;    CRS84[11].height = 1024;   CRS84[11].TileWidth = CRS84[11].TileHeight = 256;
	CRS84[12].id = 11;      CRS84[12].scale_denominator = 136494.6933663862;      CRS84[12].pixel_size = 3.43322753906250e-4;    CRS84[12].width = 4096;    CRS84[12].height = 2048;   CRS84[12].TileWidth = CRS84[12].TileHeight = 256;
	CRS84[13].id = 12;      CRS84[13].scale_denominator = 68247.34668319309;      CRS84[13].pixel_size = 1.71661376953125e-4;    CRS84[13].width = 8192;    CRS84[13].height = 4096;   CRS84[13].TileWidth = CRS84[13].TileHeight = 256;
	CRS84[14].id = 13;      CRS84[14].scale_denominator = 34123.67334159654;      CRS84[14].pixel_size = 8.58306884765625e-5;    CRS84[14].width = 16384;   CRS84[14].height = 8192;   CRS84[14].TileWidth = CRS84[14].TileHeight = 256;
	CRS84[15].id = 14;      CRS84[15].scale_denominator = 17061.83667079827;      CRS84[15].pixel_size = 4.29153442382812e-5;    CRS84[15].width = 32768;   CRS84[15].height = 16384;  CRS84[15].TileWidth = CRS84[15].TileHeight = 256;
	CRS84[16].id = 15;      CRS84[16].scale_denominator = 8530.918335399136;      CRS84[16].pixel_size = 2.14576721191406e-5;    CRS84[16].width = 65536;   CRS84[16].height = 32768;  CRS84[16].TileWidth = CRS84[16].TileHeight = 256;
	CRS84[17].id = 16;      CRS84[17].scale_denominator = 4265.459167699568;      CRS84[17].pixel_size = 1.07288360595703e-5;    CRS84[17].width = 131072;  CRS84[17].height = 65536;  CRS84[17].TileWidth = CRS84[17].TileHeight = 256;
	CRS84[18].id = 17;      CRS84[18].scale_denominator = 2132.729583849784;      CRS84[18].pixel_size = 5.36441802978516e-6;    CRS84[18].width = 262144;  CRS84[18].height = 131072; CRS84[18].TileWidth = CRS84[18].TileHeight = 256;
                                                                                                                                                                                                
	CRS3857[0].id  = 0;     CRS3857[0].scale_denominator  = 559082264.0287178;    CRS3857[0].pixel_size  = 156543.0339280410;    CRS3857[0].width  = 1;	 CRS3857[0].height  = 1;      CRS3857[0].TileWidth  = CRS3857[0].TileHeight  = 256;      	
	CRS3857[1].id  = 1;     CRS3857[1].scale_denominator  = 279541132.0143589;    CRS3857[1].pixel_size  = 78271.51696402048;    CRS3857[1].width  = 2;	 CRS3857[1].height  = 2;      CRS3857[1].TileWidth  = CRS3857[1].TileHeight  = 256;
	CRS3857[2].id  = 2;     CRS3857[2].scale_denominator  = 139770566.0071794;    CRS3857[2].pixel_size  = 39135.75848201023;    CRS3857[2].width  = 4;	 CRS3857[2].height  = 4;      CRS3857[2].TileWidth  = CRS3857[2].TileHeight  = 256;
	CRS3857[3].id  = 3;     CRS3857[3].scale_denominator  = 69885283.00358972;    CRS3857[3].pixel_size  = 19567.87924100512;    CRS3857[3].width  = 8;	 CRS3857[3].height  = 8;      CRS3857[3].TileWidth  = CRS3857[3].TileHeight  = 256;
	CRS3857[4].id  = 4;     CRS3857[4].scale_denominator  = 34942641.50179486;    CRS3857[4].pixel_size  = 9783.939620502561;    CRS3857[4].width  = 16;     CRS3857[4].height  = 16;     CRS3857[4].TileWidth  = CRS3857[4].TileHeight  = 256;
	CRS3857[5].id  = 5;     CRS3857[5].scale_denominator  = 17471320.75089743;    CRS3857[5].pixel_size  = 4891.969810251280;    CRS3857[5].width  = 32;     CRS3857[5].height  = 32;     CRS3857[5].TileWidth  = CRS3857[5].TileHeight  = 256;
	CRS3857[6].id  = 6;     CRS3857[6].scale_denominator  = 8735660.375448715;    CRS3857[6].pixel_size  = 2445.984905125640;    CRS3857[6].width  = 64;     CRS3857[6].height  = 64;     CRS3857[6].TileWidth  = CRS3857[6].TileHeight  = 256;
	CRS3857[7].id  = 7;     CRS3857[7].scale_denominator  = 4367830.187724357;    CRS3857[7].pixel_size  = 1222.992452562820;    CRS3857[7].width  = 128;    CRS3857[7].height  = 128;    CRS3857[7].TileWidth  = CRS3857[7].TileHeight  = 256;
	CRS3857[8].id  = 8;     CRS3857[8].scale_denominator  = 2183915.093862179;    CRS3857[8].pixel_size  = 611.4962262814100;    CRS3857[8].width  = 256;    CRS3857[8].height  = 256;    CRS3857[8].TileWidth  = CRS3857[8].TileHeight  = 256;
	CRS3857[9].id  = 9;     CRS3857[9].scale_denominator  = 1091957.546931089;    CRS3857[9].pixel_size  = 305.7481131407048;    CRS3857[9].width  = 512;    CRS3857[9].height  = 512;    CRS3857[9].TileWidth  = CRS3857[9].TileHeight  = 256;
	CRS3857[10].id = 10;    CRS3857[10].scale_denominator = 545978.7734655447;    CRS3857[10].pixel_size = 152.8740565703525;    CRS3857[10].width = 1024;   CRS3857[10].height = 1024;   CRS3857[10].TileWidth = CRS3857[10].TileHeight = 256;
	CRS3857[11].id = 11;    CRS3857[11].scale_denominator = 272989.3867327723;    CRS3857[11].pixel_size = 76.43702828517624;    CRS3857[11].width = 2048;   CRS3857[11].height = 2048;   CRS3857[11].TileWidth = CRS3857[11].TileHeight = 256;
	CRS3857[12].id = 12;    CRS3857[12].scale_denominator = 136494.6933663862;    CRS3857[12].pixel_size = 38.21851414258813;    CRS3857[12].width = 4096;   CRS3857[12].height = 4096;   CRS3857[12].TileWidth = CRS3857[12].TileHeight = 256;
	CRS3857[13].id = 13;    CRS3857[13].scale_denominator = 68247.34668319309;    CRS3857[13].pixel_size = 19.10925707129406;    CRS3857[13].width = 8192;   CRS3857[13].height = 8192;   CRS3857[13].TileWidth = CRS3857[13].TileHeight = 256;
	CRS3857[14].id = 14;    CRS3857[14].scale_denominator = 34123.67334159654;    CRS3857[14].pixel_size = 9.554628535647032;    CRS3857[14].width = 16384;  CRS3857[14].height = 16384;  CRS3857[14].TileWidth = CRS3857[14].TileHeight = 256;
	CRS3857[15].id = 15;    CRS3857[15].scale_denominator = 17061.83667079827;    CRS3857[15].pixel_size = 4.777314267823516;    CRS3857[15].width = 32768;  CRS3857[15].height = 32768;  CRS3857[15].TileWidth = CRS3857[15].TileHeight = 256;
	CRS3857[16].id = 16;    CRS3857[16].scale_denominator = 8530.918335399136;    CRS3857[16].pixel_size = 2.388657133911758;    CRS3857[16].width = 65536;  CRS3857[16].height = 65536;  CRS3857[16].TileWidth = CRS3857[16].TileHeight = 256;
	CRS3857[17].id = 17;    CRS3857[17].scale_denominator = 4265.459167699568;    CRS3857[17].pixel_size = 1.194328566955879;    CRS3857[17].width = 131072; CRS3857[17].height = 131072; CRS3857[17].TileWidth = CRS3857[17].TileHeight = 256;
	CRS3857[18].id = 18;    CRS3857[18].scale_denominator = 2132.729583849784;    CRS3857[18].pixel_size = 0.5971642834779395;   CRS3857[18].width = 262144; CRS3857[18].height = 262144; CRS3857[18].TileWidth = CRS3857[18].TileHeight = 256;

	extern char     *ROOT;
	extern char     *WMTSHostname;

	char 		datasetId[256];
	int		proj;
        time_t 		t_min;
        time_t 		t_max;
	time_t 		t_min_tmp;
        time_t 		t_max_tmp;

	time_t		t_dayago;
        DIR             *dp 	= NULL;
	struct dirent   *ep 	= NULL;
	FILE            *DC 	= NULL;

	char 		prod_path[MAX_STR_LEN];
	char 		imgs_path[MAX_PATH_LEN];
	char		*token  = NULL;
	char		*module = NULL;

        DIR             *(*openSource)(const char *)                    = NULL;
        struct dirent   *(*listSource)(DIR *)                           = NULL;
        int              (*closeSource)(DIR *)                          = NULL;

        FILE            *(*openSourceFile)(const char *, const char *)  = NULL;
        int              (*readSourceFile)(FILE *, const char *, ...)   = NULL;
        int              (*closeSourceFile)(FILE *)                     = NULL;


	double x_input_ul, y_input_ul, x_input_lr, y_input_lr, GeoX_ul, GeoY_ul, GeoX_lr, GeoY_lr;
        time_t  chronometer;
        char    chronometer_str[256];
        struct  timespec time_before,time_after;
        double   time_total = 0;

	char	proc_options[5][50] 	= { "overlap", "mostrecent", "leastrecent", "minvalue", "maxvalue" };
	int	proc_cnt		= 5;

	info->killable  = TRUE;

        listSource      = readdir;
        openSource      = opendir;
        closeSource     = closedir;
        openSourceFile  = fopen;
        readSourceFile  = fscanf;
        closeSourceFile = fclose;
	clock_gettime(CLOCK_REALTIME, &time_before); ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: [WMTS] Enter in GetCapabilities");

        if ( ROOT != NULL ) dp = (*openSource)(ROOT);
        if (  dp == NULL  ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] Product ROOT not exists %s", ROOT); return 500; }
	if ( WMTSHostname == NULL ) { WMTSHostname = (char *)malloc(MAX_STR_LEN); bzero(WMTSHostname, MAX_STR_LEN - 1 ); sprintf(WMTSHostname, "http://%s/%s", r->hostname, "wmts" ); }

	if ( info->token  != NULL ) { token  = malloc(MAX_STR_LEN); bzero(token,  MAX_STR_LEN - 1); snprintf(token,  MAX_STR_LEN, "token=%s&amp;",  info->token);  }
	if ( info->module != NULL ) { module = malloc(MAX_STR_LEN); bzero(module, MAX_STR_LEN - 1); snprintf(module, MAX_STR_LEN, "module=%s&amp;", info->module); }



	addStatToPush(info, "action", "wmts", GFT_String ); 

	ap_set_content_type(r, "application/xml");
	ap_rprintf(r, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");

	ap_rprintf(r, "<Capabilities xmlns=\"http://www.opengis.net/wmts/1.0\"\n");
	ap_rprintf(r, "           xmlns:ows=\"http://www.opengis.net/ows/1.1\"\n");
	ap_rprintf(r, "           xmlns:xlink=\"http://www.w3.org/1999/xlink\"\n");
	ap_rprintf(r, "           xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n");
	ap_rprintf(r, "           xsi:schemaLocation=\"http://www.opengis.net/wmts/1.0 http://schemas.opengis.net/wmts/1.0/wmtsGetCapabilities_response.xsd\" version=\"1.0.0\">\n");

	ap_rprintf(r, "   <ows:ServiceIdentification>\n");
	#ifdef MWCS_VERSION
	ap_rprintf(r, "      <ows:Title>MEEO Web Map Tile Service - (Ver. %s)</ows:Title>\n", MWCS_VERSION);
        #else
	ap_rprintf(r, "      <ows:Title>MEEO Web Map Tile Service</ows:Title>\n");
        #endif
	ap_rprintf(r, "      <ows:Keywords>\n");
	ap_rprintf(r, "         <ows:Keyword>World</ows:Keyword>\n");
	ap_rprintf(r, "         <ows:Keyword>Global</ows:Keyword>\n");
	ap_rprintf(r, "         <ows:Keyword>Stuff</ows:Keyword>\n");
	ap_rprintf(r, "      </ows:Keywords>\n");
	ap_rprintf(r, "      <ows:ServiceType>OGC WMTS</ows:ServiceType>\n");
	ap_rprintf(r, "      <ows:ServiceTypeVersion>1.0.0</ows:ServiceTypeVersion>\n");
	ap_rprintf(r, "      <ows:Profile>http://www.opengis.net/spec/wmts-simple/1.0/conf/simple-profile</ows:Profile>\n");
	ap_rprintf(r, "      <ows:Fees>none</ows:Fees>\n");
	ap_rprintf(r, "      <ows:AccessConstraints>none</ows:AccessConstraints>\n");
	ap_rprintf(r, "   </ows:ServiceIdentification>\n");
	ap_rprintf(r, "   <ows:ServiceProvider>\n");
	ap_rprintf(r, "      <ows:ProviderName>MEEO</ows:ProviderName>\n");
	ap_rprintf(r, "      <ows:ProviderSite xlink:href=\"https://www.meeo.it\"/>\n");
	ap_rprintf(r, "      <ows:ServiceContact/>\n");
	ap_rprintf(r, "   </ows:ServiceProvider>\n");

        ap_rprintf(r, "   <ows:OperationsMetadata>\n");
        ap_rprintf(r, "    <ows:Operation name=\"GetCapabilities\">\n");
        ap_rprintf(r, "      <ows:DCP>\n");
        ap_rprintf(r, "        <ows:HTTP>\n");
	if ( info->token == NULL ) ap_rprintf(r, "          <ows:Get xlink:href=\"%s\">\n", 		WMTSHostname);
	else 		    	   ap_rprintf(r, "          <ows:Get xlink:href=\"%s?token=%s\">\n", 	WMTSHostname, info->token );
        ap_rprintf(r, "            <ows:Constraint name=\"GetEncoding\">\n");
        ap_rprintf(r, "              <ows:AllowedValues>\n");
        ap_rprintf(r, "                <ows:Value>KVP</ows:Value>\n");
        ap_rprintf(r, "              </ows:AllowedValues>\n");
        ap_rprintf(r, "            </ows:Constraint>\n");
        ap_rprintf(r, "          </ows:Get>\n");
        ap_rprintf(r, "        </ows:HTTP>\n");
        ap_rprintf(r, "      </ows:DCP>\n");
        ap_rprintf(r, "    </ows:Operation>\n");
        ap_rprintf(r, "    <ows:Operation name=\"GetTile\">\n");
        ap_rprintf(r, "      <ows:DCP>\n");
        ap_rprintf(r, "        <ows:HTTP>\n");
	if ( info->token == NULL ) ap_rprintf(r, "          <ows:Get xlink:href=\"%s\">\n", 		WMTSHostname);
	else 		    	   ap_rprintf(r, "          <ows:Get xlink:href=\"%s?token=%s\">\n", 	WMTSHostname, info->token );
        ap_rprintf(r, "            <ows:Constraint name=\"GetEncoding\">\n");
        ap_rprintf(r, "              <ows:AllowedValues>\n");
        ap_rprintf(r, "                <ows:Value>RESTful</ows:Value>\n");
        ap_rprintf(r, "              </ows:AllowedValues>\n");
        ap_rprintf(r, "            </ows:Constraint>\n");
        ap_rprintf(r, "          </ows:Get>\n");
	if ( info->token == NULL ) ap_rprintf(r, "          <ows:Get xlink:href=\"%s\">\n", 		WMTSHostname);
	else 		    	   ap_rprintf(r, "          <ows:Get xlink:href=\"%s?token=%s\">\n", 	WMTSHostname, info->token );
        ap_rprintf(r, "            <ows:Constraint name=\"GetEncoding\">\n");
        ap_rprintf(r, "              <ows:AllowedValues>\n");
        ap_rprintf(r, "                <ows:Value>KVP</ows:Value>\n");
        ap_rprintf(r, "              </ows:AllowedValues>\n");
        ap_rprintf(r, "            </ows:Constraint>\n");
        ap_rprintf(r, "          </ows:Get>\n");




        ap_rprintf(r, "        </ows:HTTP>\n");
        ap_rprintf(r, "      </ows:DCP>\n");
        ap_rprintf(r, "    </ows:Operation>\n");
        ap_rprintf(r, "  </ows:OperationsMetadata>\n");




	ap_rprintf(r, "   <Contents>\n");
	if ( ROOT != NULL ) while ( (ep = (*listSource)(dp)) ){
        	if ( ep->d_name[0] == '.' ) continue; 

		if ( ( info->coverage != NULL ) && ( ! strstr(ep->d_name,  info->coverage ) ) ) continue; // GetCoverage filter
		bzero(prod_path, MAX_STR_LEN - 1);
		sprintf(prod_path, "%s/%s", ROOT, ep->d_name);


		//-------------------------------------------------------------------------------
		

		// JSON NEW PART

		sprintf(imgs_path, "%s/DescribeCoverage.json", prod_path );


		DC = (*openSourceFile)( imgs_path, "r" );
		if ( DC == NULL ) continue;
		
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: [WMTS] GetCapabilities Using JSON DescribeCoverage for %s ...", ep->d_name );  

		char 		*json_buff	= ( char *)malloc(sizeof(char)*4096);
		char 		*json_dc	= NULL;
		size_t 		json_len 	= 0;
		ssize_t 	json_n		= 0;
		json_object 	*jobj 		= NULL;
		json_object 	*jobj_dc	= NULL;


		while ( (*readSourceFile)(DC,"%s", json_buff) != EOF ){
			json_n = strlen(json_buff);
			json_dc = realloc(json_dc, json_len + json_n + 1);

			memcpy(json_dc + json_len, json_buff, json_n);
			json_len += json_n;
			json_dc[json_len] = '\0';
		}
		(*closeSourceFile)(DC); DC= NULL;

		jobj = json_tokener_parse(json_dc);
		if ( jobj != NULL){
			json_object_object_foreach(jobj, key, val){
				if (	(json_object_is_type(val, json_type_array)) 	&&
					( ! strcmp( key, "Collections" ))		){
					json_object_object_get_ex(jobj, key, &jobj_dc);
					break;
				}
			}
		}
		if ( jobj_dc != NULL ) {
			for( i = 0; i < json_object_array_length(jobj_dc); i++){
				json_object_object_foreach( json_object_array_get_idx(jobj_dc, i) , key, val){
					if 	( ! strcmp(key, "t_min" 	) ) t_min_tmp	 	= json_object_get_int(val);
					else if ( ! strcmp(key, "t_max" 	) ) t_max_tmp	 	= json_object_get_int(val);
                                        else if ( ! strcmp(key, "EPSG"          ) ) proj                = json_object_get_int(val);
                                        else if ( ! strcmp(key, "GeoX_ul"       ) ) x_input_ul      	= json_object_get_double(val);
                                        else if ( ! strcmp(key, "GeoY_ul"       ) ) y_input_ul      	= json_object_get_double(val);
                                        else if ( ! strcmp(key, "GeoY_lr"       ) ) y_input_lr      	= json_object_get_double(val);
                                        else if ( ! strcmp(key, "GeoX_lr"       ) ) x_input_lr      	= json_object_get_double(val);
					else continue; 
				}

				if ( proj != 4326 ){
			                OGRSpatialReferenceH geoSRSsrc = OSRNewSpatialReference(NULL);
			                OGRSpatialReferenceH geoSRSdst = OSRNewSpatialReference(NULL);

                			if ( ImportFromEPSG(&geoSRSsrc, proj ) != OGRERR_NONE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] EPSG for geometry `%d'", proj ); continue; }
                			if ( ImportFromEPSG(&geoSRSdst, 4326 ) != OGRERR_NONE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] EPSG for geometry `%d'", proj ); continue; }

                			#if GDAL_VERSION >= 304
                			OSRSetAxisMappingStrategy(geoSRSsrc, OAMS_TRADITIONAL_GIS_ORDER); // GDAL 3.0 Fix
                			OSRSetAxisMappingStrategy(geoSRSdst, OAMS_TRADITIONAL_GIS_ORDER); // GDAL 3.0 Fix
                			#endif

					OGRCoordinateTransformationH hCT = OCTNewCoordinateTransformation( geoSRSsrc, geoSRSdst );
	                                if ( OCTTransform(hCT, 1, &x_input_ul, &y_input_ul, NULL) == FALSE ) continue; 
                                        if ( OCTTransform(hCT, 1, &x_input_lr, &y_input_lr, NULL) == FALSE ) continue;
					OCTDestroyCoordinateTransformation(hCT);

					OSRDestroySpatialReference(geoSRSsrc);
					OSRDestroySpatialReference(geoSRSdst);
				}					

				if ( i > 0 ) { 
					if ( t_min_tmp < t_min ) t_min = t_min_tmp; 
					if ( t_max_tmp > t_max ) t_max = t_max_tmp;  
					if ( GeoX_ul < x_input_ul ) GeoX_ul = x_input_ul;
					if ( GeoY_ul > y_input_ul ) GeoY_ul = y_input_ul;
					if ( GeoX_lr > x_input_lr ) GeoX_lr = x_input_lr;
					if ( GeoY_lr < y_input_lr ) GeoY_lr = y_input_lr;	
				} else {
					t_min = t_min_tmp; t_max = t_max_tmp;  
					GeoX_ul = x_input_ul;
					GeoY_ul = y_input_ul;
					GeoX_lr = x_input_lr;
					GeoY_lr = y_input_lr;
				}

			}

			t_dayago = t_max - ( 3600 * 24 );
	ap_rprintf(r, "     <Layer>\n");
	ap_rprintf(r, "         <ows:Title>%s</ows:Title>\n", ep->d_name);
	ap_rprintf(r, "         <ows:Identifier>%s</ows:Identifier>\n", ep->d_name);
	ap_rprintf(r, "         <ows:WGS84BoundingBox>\n");
	ap_rprintf(r, "           <ows:LowerCorner>%f %f</ows:LowerCorner>\n", GeoX_ul, GeoY_ul);
	ap_rprintf(r, "           <ows:UpperCorner>%f %f</ows:UpperCorner>\n", GeoX_lr, GeoY_lr);
	ap_rprintf(r, "         </ows:WGS84BoundingBox>\n");

	ap_rprintf(r, "         <Style isDefault=\"true\">\n");
	ap_rprintf(r, "           <ows:Title>default</ows:Title>\n");
	ap_rprintf(r, "           <ows:Identifier>default</ows:Identifier>\n");
	ap_rprintf(r, "         </Style>\n");
	for ( i = 0; i < proc_cnt; i++){
	ap_rprintf(r, "         <Style isDefault=\"false\">\n");
	ap_rprintf(r, "           <ows:Title>default-%s</ows:Title>\n", 		proc_options[i]);
	ap_rprintf(r, "           <ows:Identifier>default;proc=%s</ows:Identifier>\n", 	proc_options[i]);
	ap_rprintf(r, "         </Style>\n");
	}

	ap_rprintf(r, "         <Format>image/png</Format>\n");
	ap_rprintf(r, "         <Dimension>\n");
        ap_rprintf(r, "            <ows:Identifier>TimeStart</ows:Identifier>\n");
        ap_rprintf(r, "            <Default>%s</Default>\n", datatimeStringFromUNIX( gmtime(&t_dayago) ) );
        ap_rprintf(r, "            <Value>%s/%s/PT1S</Value>\n", datatimeStringFromUNIX( gmtime(&t_min) ), datatimeStringFromUNIX( gmtime(&t_max) ) );
        ap_rprintf(r, "         </Dimension>\n");
	ap_rprintf(r, "         <Dimension>\n");
        ap_rprintf(r, "            <ows:Identifier>TimeEnd</ows:Identifier>\n");
        ap_rprintf(r, "            <Default>%s</Default>\n", datatimeStringFromUNIX( gmtime(&t_max) ) );
        ap_rprintf(r, "            <Value>%s/%s/PT1S</Value>\n", datatimeStringFromUNIX( gmtime(&t_min) ), datatimeStringFromUNIX( gmtime(&t_max) ) );
        ap_rprintf(r, "         </Dimension>\n");
	ap_rprintf(r, "	        <TileMatrixSetLink>\n");
	ap_rprintf(r, "       	  <TileMatrixSet>EPSG:4326</TileMatrixSet>\n");
	ap_rprintf(r, "        	</TileMatrixSetLink>\n");
	ap_rprintf(r, "         <ResourceURL format=\"image/png\" resourceType=\"tile\" template=\"%s/{layer}/{style}/{TimeStart}/{TimeEnd}/{TileMatrixSet}/{TileMatrix}/{TileCol}/{TileRow}.png", WMTSHostname );
	if ( ( token  != NULL ) || ( module != NULL )) 	ap_rprintf(r,"?");
	if ( token   != NULL )				ap_rprintf(r,"%s", token);
	if ( module  != NULL )				ap_rprintf(r,"%s", module); ap_rprintf(r, "\"/>\n");
	ap_rprintf(r, "      </Layer>\n");
		}
	}

	//--------------------------------------------------------------------------------------------------------------------------------
	ap_rprintf(r, "      <TileMatrixSet>\n");
	ap_rprintf(r, "         <ows:Title>CRS84 for the World</ows:Title>\n");
	ap_rprintf(r, "         <ows:Identifier>EPSG:4326</ows:Identifier>\n");
	ap_rprintf(r, "         <ows:BoundingBox crs=\"urn:ogc:def:crs:OGC:1.3:CRS84\">\n");
	ap_rprintf(r, "            <ows:LowerCorner>-180 -90</ows:LowerCorner>\n");
	ap_rprintf(r, "            <ows:UpperCorner>180 90</ows:UpperCorner>\n");
	ap_rprintf(r, "         </ows:BoundingBox>\n");
	ap_rprintf(r, "         <ows:SupportedCRS>\n");
	ap_rprintf(r, "            urn:ogc:def:crs:OGC:1.3:CRS84\n");
	ap_rprintf(r, "         </ows:SupportedCRS>\n");
	ap_rprintf(r, "         <WellKnownScaleSet>\n");
	ap_rprintf(r, "            urn:ogc:def:wkss:OGC:1.0:GoogleCRS84Quad\n");
	ap_rprintf(r, "         </WellKnownScaleSet>\n");

	for ( i = 0; i < 19 ; i++ ) {
	ap_rprintf(r, "         <TileMatrix>\n");
	ap_rprintf(r, "            <ows:Identifier>%d</ows:Identifier>\n",	CRS84[i].id);
	ap_rprintf(r, "            <ScaleDenominator>%f</ScaleDenominator>\n",	CRS84[i].scale_denominator);
	ap_rprintf(r, "            <TopLeftCorner>-180 90</TopLeftCorner>\n");
	ap_rprintf(r, "            <TileWidth>%d</TileWidth>\n",		CRS84[i].TileWidth);
	ap_rprintf(r, "            <TileHeight>%d</TileHeight>\n",		CRS84[i].TileHeight);
	ap_rprintf(r, "            <MatrixWidth>%d</MatrixWidth>\n",		CRS84[i].width);
	ap_rprintf(r, "            <MatrixHeight>%d</MatrixHeight>\n",		CRS84[i].height);
	ap_rprintf(r, "         </TileMatrix>\n");
	}
	ap_rprintf(r, "      </TileMatrixSet>\n");
	ap_rprintf(r, "      <TileMatrixSet>\n");
	ap_rprintf(r, "         <ows:Title>Google Maps Compatible for the World</ows:Title>\n");
	ap_rprintf(r, "         <ows:Identifier>EPSG:3857</ows:Identifier>\n");
	ap_rprintf(r, "         <ows:BoundingBox crs=\"urn:ogc:def:crs:EPSG::3857\">\n");
	ap_rprintf(r, "            <ows:LowerCorner>-20037508.3427892 -20037508.3427892</ows:LowerCorner>\n");
	ap_rprintf(r, "            <ows:UpperCorner>20037508.3427892 20037508.3427892</ows:UpperCorner>\n");
	ap_rprintf(r, "         </ows:BoundingBox>\n");
	ap_rprintf(r, "         <ows:SupportedCRS>\n");
	ap_rprintf(r, "            urn:ogc:def:crs:EPSG::3857\n");
	ap_rprintf(r, "         </ows:SupportedCRS>\n");
	ap_rprintf(r, "         <WellKnownScaleSet>\n");
	ap_rprintf(r, "            urn:ogc:def:wkss:OGC:1.0:GoogleMapsCompatible\n");
	ap_rprintf(r, "         </WellKnownScaleSet>\n");

	for ( i = 0; i < 19 ; i++ ) {
	ap_rprintf(r, "         <TileMatrix>\n");
	ap_rprintf(r, "            <ows:Identifier>%d</ows:Identifier>\n",	CRS3857[i].id);
	ap_rprintf(r, "            <ScaleDenominator>%f</ScaleDenominator>\n",	CRS3857[i].scale_denominator);
	ap_rprintf(r, "            <TopLeftCorner>-20037508.3427892 20037508.3427892</TopLeftCorner>\n");
	ap_rprintf(r, "            <TileWidth>%d</TileWidth>\n",		CRS3857[i].TileWidth);
	ap_rprintf(r, "            <TileHeight>%d</TileHeight>\n",		CRS3857[i].TileHeight);
	ap_rprintf(r, "            <MatrixWidth>%d</MatrixWidth>\n",		CRS3857[i].width);
	ap_rprintf(r, "            <MatrixHeight>%d</MatrixHeight>\n",		CRS3857[i].height);
	ap_rprintf(r, "         </TileMatrix>\n");
	}
	ap_rprintf(r, "      </TileMatrixSet>\n");


	ap_rprintf(r, "   </Contents>\n");
	ap_rprintf(r, "</Capabilities>\n");

	clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;
        ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: [WMTS] Finish in GetCapabilities in %.3f sec, Total time %.3f sec", 
			(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
			
	return 0;
}

//------------------------------------------------------------------------------------------

int WMTSGetTileURL(struct info *info){
	request_rec  *r = info->r; int i;
	extern	char	*WMTSCachePath;

	extern 	long int GeomPOSTSize;
        ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: [WMTS] Tile request using URL ...");

        char 		*wmts_uri 		= NULL; wmts_uri 	= malloc(strlen(info->uri) + 1 ); strcpy(wmts_uri, 	info->uri);

	if ( info->cache == TRUE ) {
		char *md5 = NULL; md5 = malloc(33); bzero(md5, 32); md5_tile(info, md5); info->tile_path = malloc(MAX_PATH_LEN); bzero( info->tile_path, MAX_PATH_LEN - 1 );
		snprintf(info->tile_path, MAX_PATH_LEN, "%s/wmts/%s", ( WMTSCachePath != NULL ) ? WMTSCachePath : "", md5 );
	}

        char 		*wmts_tok 		= NULL; int wmts_chek = TRUE;
	char 		*style_tok 	= NULL;
	char 		*CoverageId 	= NULL;
	char 		*time_s	 	= NULL;
	char 		*time_e	 	= NULL;
	char 		*style	 	= NULL;
        int 		TileMatrix  	= -1;
        int 		TileCol     	= -1;
        int 		TileRow     	= -1;
	int 		epsg 		= 4326;
	char    	*pszKey         = NULL;
	char		*style_ptr	= NULL;
	char		*p		= NULL;
	const char      *pszValue;
	char            colortable[128], colorrange[128], nodata[128], token[128], merge[128];
	int		size_x		= 256;
	int		size_y		= 256;

	long int        len             = 0;
	char            *query          = NULL;
	char 		*tok            = NULL;
	char 		*query_ptr	= NULL;
	int		Request		= GetCoverage;
	char		*mwcs_extra	= NULL;

	double x_input_ul, y_input_ul, x_input_lr, y_input_lr, GeoX_ul, GeoY_ul, GeoX_lr, GeoY_lr;
	double (*tilex2lon)(int, int);
	double (*tiley2lat)(int, int);
        wmts_tok = strtok(wmts_uri, "/");
        i = 0; while ( wmts_tok != NULL ){
                switch(i){
                        case 0: if ( strcmp(wmts_tok, "wmts") ) 	wmts_chek = FALSE;      	break;
			case 1: CoverageId = malloc(strlen(wmts_tok)+1); strcpy(CoverageId, wmts_tok); 	break;  
			case 2: style 	   = malloc(strlen(wmts_tok)+1); strcpy(style,  wmts_tok); 	break;  
			case 3: time_s     = malloc(strlen(wmts_tok)+1); strcpy(time_s, wmts_tok); 	break;  
			case 4: time_e     = malloc(strlen(wmts_tok)+1); strcpy(time_e, wmts_tok); 	break;  
                        case 5: if 	( ! strcmp(wmts_tok, "EPSG:4326") ) { tiley2lat = &tiley2lat_EPSG_4326; tilex2lon = &tilex2lon_EPSG_4326; epsg = 4326;     }
				else if ( ! strcmp(wmts_tok, "EPSG:3857") ) { tiley2lat = &tiley2lat_EPSG_3857; tilex2lon = &tilex2lon_EPSG_3857; epsg = 3857;     }
				else if ( ! strcmp(wmts_tok, "TMS"	) ) { tiley2lat = &tiley2lat_EPSG_3857; tilex2lon = &tilex2lon_EPSG_3857; epsg = 900913;   }
				else	wmts_chek = FALSE;      					break;
                        case 6: TileMatrix 	= atoi(wmts_tok);      	                   		break;
                        case 7: TileCol 	= atoi(wmts_tok);                              		break;
			case 8: TileRow 	= atoi(wmts_tok); if (strstr(wmts_tok, ".json")) Request = GetInfo;
				break;
                        default:
				ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] Paramer %s at %d not exists ...", wmts_tok, i );
				return FALSE;


                }
                wmts_tok = strtok(NULL, "/"); i++;
        }

	if ( epsg == 900913 ) { TileRow = ( 1 << TileMatrix ) - TileRow - 1; epsg = 3857; }

        if ( wmts_chek != TRUE ) return FALSE;
	if ( i <= 8 )		 return FALSE;

	if ( CoverageId != NULL ) addStatToPush(info, "datasetId",  CoverageId, GFT_String ); // STATS
				  addStatToPush(info, "action",     "wmts",     GFT_String ); 

	if ( info->query_string != NULL ) {
		len = strlen(info->query_string); 	if( len 		<= 0    ){ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] Error in invocation - wrong FORM probably (Size zero)"); 	return FALSE; }
	        query = malloc(len + 1); 		if ( query 		== NULL ){ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] malloc query %s", info->query_string); 			return FALSE; }
	        bzero(query, len);  unencode(info->query_string, query);

		tok = strtok_r( query, DELIMS, &query_ptr );
		while( tok != NULL ) {
			pszValue = (const char *)CPLParseNameValue( tok, &pszKey );
			if ( pszValue == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] Unable to parsing '%s'", tok); return FALSE; }
			for(p = pszKey;*p;++p) *p=*p>0x40&&*p<0x5b?*p|0x60:*p;

			if 	( ! strcmp(pszKey, "token" ) ) 	{ info->token  = malloc( strlen(pszValue) + 1) ; bzero(info->token,  strlen(pszValue) ); strcpy(info->token,  pszValue); }
			else if ( ! strcmp(pszKey, "module") ) 	{ info->module = malloc( strlen(pszValue) + 1) ; bzero(info->module, strlen(pszValue) ); strcpy(info->module, pszValue); }
			else 				 	{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] Unsupported request %s", pszValue); return FALSE; }
			
			query = query_ptr; tok = strtok_r( query, DELIMS, &query_ptr );	
		}
	       	info->query_string = NULL;
	}



	// Add extra ultra secret parameters from POST to WCS query 
	if ( ! strcmp(r->method, "POST") ) {
		if ( info->pairs == NULL ) {
		        if ( GeomPOSTSize > 0 ) ap_parse_form_data(r, NULL, &info->pairs, -1, GeomPOSTSize );
		        else                    ap_parse_form_data(r, NULL, &info->pairs, -1, 1024 * 1024 * 3 );
		}
		apr_array_header_t *pairs = apr_array_copy(r->pool, info->pairs);
		while (pairs && !apr_is_empty_array(pairs)) {
	                apr_off_t       len      = 0;
	                apr_size_t      size     = 0;
	                ap_form_pair_t  *pair   = (ap_form_pair_t *) apr_array_pop(pairs);
			if ( strcmp(pair->name, "mwcs") ) continue;

	                apr_brigade_length(pair->value, 1, &len);       size = (apr_size_t) len;
	                mwcs_extra = apr_palloc(r->pool, size + 2);    	apr_brigade_flatten(pair->value, mwcs_extra + 1, &size); mwcs_extra[len+1] = '\0'; mwcs_extra[0] = ';';
			for(i = 0; i < ( len + 1 ) ; i++ ) if ( mwcs_extra[i] ==';' ) mwcs_extra[i] = '&';


			break;
       		} 
	}



        info->query_string 	= malloc( MAX_STR_LEN + 1 ); bzero(info->query_string, MAX_STR_LEN);
	info->passthrough 	= TRUE; 
	info->x_input_ul 	= (*tilex2lon)(TileCol, 	TileMatrix );
	info->x_input_lr 	= (*tilex2lon)(TileCol + 1, 	TileMatrix );
	info->y_input_lr 	= (*tiley2lat)(TileRow, 	TileMatrix );
	info->y_input_ul 	= (*tiley2lat)(TileRow + 1, 	TileMatrix );


	if ( ! strcmp(style, "default") )
	        sprintf(info->query_string,
		          "/wcs?service=WCS&Request=%s&version=2.0.0&subset=unix(%s,%s)&format=image/png&epsg_out=EPSG:%d&crop=false&subset=Lat(%.10f,%.10f)&subset=Long(%.10f,%.10f)&size=(%d,%d)&CoverageId=%s&wmts=true&filter=false&math=single",
	        	  (Request == GetInfo) ? "GetInfo" : "GetCoverage", time_s, time_e , epsg, info->y_input_lr, info->y_input_ul, info->x_input_ul, info->x_input_lr, size_x, size_y, CoverageId  ); 
	else {
	        sprintf(info->query_string,
		          "/wcs?service=WCS&Request=%s&version=2.0.0&subset=unix(%s,%s)&format=image/png&epsg_out=EPSG:%d&crop=false&subset=Lat(%.10f,%.10f)&subset=Long(%.10f,%.10f)&size=(%d,%d)&CoverageId=%s&wmts=true&filter=false&math=single",
	        	  (Request == GetInfo) ? "GetInfo" : "GetCoverage", time_s, time_e , epsg, info->y_input_lr, info->y_input_ul, info->x_input_ul, info->x_input_lr, size_x, size_y, CoverageId  ); 

		style_tok = strtok_r( style, ";", &style_ptr ); 
		if ( style_tok != NULL ) {
			snprintf(colortable, 128, "&colortable=%s", style_tok ); strcat(info->query_string, colortable); 
			style = style_ptr; style_tok = strtok_r( style, ";", &style_ptr ); 
		}
		while( style_tok != NULL ) {
			pszValue = (const char *)CPLParseNameValue( style_tok, &pszKey );
			if ( pszValue == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] Unable to parsing style option '%s'", style_tok); return FALSE; }
			for(p = pszKey;*p;++p) { *p=*p>0x40&&*p<0x5b?*p|0x60:*p; if ( *p == '&' ) return FALSE; }

			if 		( ! strcmp(pszKey, "colorrange") ) 	{ snprintf(colorrange, 	128, "&colorrange=%s", 	pszValue ); strcat(info->query_string, colorrange); 	}
			else if		( ! strcmp(pszKey, "nodata") ) 		{ snprintf(nodata, 	128, "&nodata=%s", 	pszValue ); strcat(info->query_string, nodata); 	}
			else if 	( ! strcmp(pszKey, "proc") ) 		{ snprintf(merge,       128, "&proc=%s",        pszValue ); strcat(info->query_string, merge);          } 
				

			style = style_ptr; style_tok = strtok_r( style, ";", &style_ptr );
		}

	}

	if ( mwcs_extra  != NULL ) strcat(info->query_string, mwcs_extra);

//     	fprintf(stderr, "%s\n", info->query_string); fflush(stderr);

        return TRUE;

}



//------------------------------------------------------------------------------------------

int WMSGetCapabilities(struct info *info){

	int i,j;
	extern char     *WMTSHostname;
	extern char     *ROOT;

	request_rec     *r              	= info->r;
	char		*colorTable		= NULL;
	char 		datasetId[256];
	int		NODATA_VALUE		= FALSE;
	int		MINMAX_VALUE		= FALSE;
	int 		PROD_FOUND 		= FALSE;
	double		nodataValue;
	double		maxValue, minValue;
	int		proj;
        time_t 		t_min;
        time_t 		t_max;
	time_t 		t_min_tmp;
        time_t 		t_max_tmp;

	time_t		t_dayago;
        DIR             *dp 	= NULL;
	struct dirent   *ep 	= NULL;
	FILE            *DC 	= NULL;

	char 		style[MAX_STR_LEN];
	char 		prod_path[MAX_STR_LEN];
	char 		imgs_path[MAX_PATH_LEN];

        DIR             *(*openSource)(const char *)                    = NULL;
        struct dirent   *(*listSource)(DIR *)                           = NULL;
        int              (*closeSource)(DIR *)                          = NULL;

        FILE            *(*openSourceFile)(const char *, const char *)  = NULL;
        int              (*readSourceFile)(FILE *, const char *, ...)   = NULL;
        int              (*closeSourceFile)(FILE *)                     = NULL;

	time_t  	chronometer;
        char    	chronometer_str[256];
	struct timespec time_before,time_after;
        double   	time_total = 0;

	double x_input_ul, y_input_ul, x_input_lr, y_input_lr, GeoX_ul, GeoY_ul, GeoX_lr, GeoY_lr;
	info->killable  = TRUE;


        listSource      = readdir;
        openSource      = opendir;
        closeSource     = closedir;
        openSourceFile  = fopen;
        readSourceFile  = fscanf;
        closeSourceFile = fclose;
	clock_gettime(CLOCK_REALTIME, &time_before);

	if ( ROOT != NULL ) dp = (*openSource)(ROOT);
        if (  dp == NULL  ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMS] Product ROOT not exists %s", ROOT); return 500; }

	if ( WMTSHostname == NULL ) { WMTSHostname = (char *)malloc(MAX_STR_LEN); bzero(WMTSHostname, MAX_STR_LEN - 1 ); sprintf(WMTSHostname, "http://%s/%s", r->hostname, "wmts" ); }
	ap_set_content_type(r, "text/xml");

	addStatToPush(info, "action", "wms",  GFT_String ); 

	// Header for version 1.1.1
	if ( ! strcmp(info->version, "1.1.1") ) {
	ap_rprintf(r, "<?xml version='1.0' encoding=\"ISO-8859-1\" standalone=\"no\" ?>\n");
	ap_rprintf(r, "<!DOCTYPE WMT_MS_Capabilities SYSTEM \"http://schemas.opengis.net/wms/1.1.1/WMS_MS_Capabilities.dtd\"\n");
	ap_rprintf(r, " [\n");
	ap_rprintf(r, " <!ELEMENT VendorSpecificCapabilities EMPTY>\n");
	ap_rprintf(r, " ]>  <!-- end of DOCTYPE declaration -->\n");
	ap_rprintf(r, "<WMT_MS_Capabilities version=\"1.1.1\">\n");
	ap_rprintf(r, "<Service>\n");
	ap_rprintf(r, "  <Name>OGC:WMS</Name>\n");
	ap_rprintf(r, "  <Title>MEEO WMS</Title>\n");
	ap_rprintf(r, "  <Abstract>Where No Man Has Gone Before</Abstract>\n");
	#ifdef MWCS_VERSION
	ap_rprintf(r, "  <ServerInfo>MEEO Web Map Tile Service - (Ver. %s)</ServerInfo>\n", MWCS_VERSION);
        #else
	ap_rprintf(r, "  <ServerInfo>MEEO Web Map Tile Service</ServerInfo>\n");
        #endif
	ap_rprintf(r, "  <OnlineResource xmlns:xlink=\"http://www.w3.org/1999/xlink\" xlink:href=\"%s?service=WMS&amp;\"/>\n", 				WMTSHostname);	
	ap_rprintf(r, "  <ContactInformation>\n");
	ap_rprintf(r, "  </ContactInformation>\n");
	ap_rprintf(r, "</Service>\n");
	ap_rprintf(r, "<Capability>\n");
	ap_rprintf(r, "  <Request>\n");
	ap_rprintf(r, "    <GetCapabilities>\n");
	ap_rprintf(r, "      <Format>application/vnd.ogc.wms_xml</Format>\n");
	ap_rprintf(r, "      <DCPType>\n");
	ap_rprintf(r, "        <HTTP>\n");
	ap_rprintf(r, "          <Get><OnlineResource xmlns:xlink=\"http://www.w3.org/1999/xlink\" xlink:href=\"%s?service=WMS&amp;\"/></Get>\n", 	WMTSHostname);
	ap_rprintf(r, "        </HTTP>\n");
	ap_rprintf(r, "      </DCPType>\n");
	ap_rprintf(r, "    </GetCapabilities>\n");
	ap_rprintf(r, "    <GetMap>\n");
	ap_rprintf(r, "      <Format>image/png</Format>\n");
	ap_rprintf(r, "      <DCPType>\n");
	ap_rprintf(r, "        <HTTP>\n");
	ap_rprintf(r, "          <Get><OnlineResource xmlns:xlink=\"http://www.w3.org/1999/xlink\" xlink:href=\"%s?service=WMS&amp;\"/></Get>\n", 	WMTSHostname);
	ap_rprintf(r, "        </HTTP>\n");
	ap_rprintf(r, "      </DCPType>\n");
	ap_rprintf(r, "    </GetMap>\n");
	ap_rprintf(r, "  </Request>\n");
	ap_rprintf(r, "  <Exception>\n");
	ap_rprintf(r, "    <Format>application/vnd.ogc.se_xml</Format>\n");
	ap_rprintf(r, "    <Format>application/vnd.ogc.se_inimage</Format>\n");
	ap_rprintf(r, "    <Format>application/vnd.ogc.se_blank</Format>\n");
	ap_rprintf(r, "  </Exception>\n");
	ap_rprintf(r, "  <Layer>\n");
	ap_rprintf(r, "    <Title>All Coverage WMS</Title>\n");
	ap_rprintf(r, "    <SRS>EPSG:4326</SRS>\n");
	ap_rprintf(r, "    <SRS>EPSG:3857</SRS>\n");

	// Header for version 1.1.1
	} else if ( ! strcmp(info->version, "1.3.0") ) {	
	ap_rprintf(r, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
	ap_rprintf(r, "<WMS_Capabilities version=\"1.3.0\" updateSequence=\"0\" xmlns=\"http://www.opengis.net/wms\" xmlns:xlink=\"http://www.w3.org/1999/xlink\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"");
	ap_rprintf(r, " xsi:schemaLocation=\"http://www.opengis.net/wms http://schemas.opengis.net/wms/1.3.0/capabilities_1_3_0.xsd\">\n");
	ap_rprintf(r, "    <Service>\n");
	ap_rprintf(r, "        <Name>WMS</Name>\n");
	ap_rprintf(r, "        <Title>MEEO WMS</Title>\n");
	ap_rprintf(r, "        <Abstract>Where No Man Has Gone Before</Abstract>\n");
	ap_rprintf(r, "        <KeywordList>\n");
	ap_rprintf(r, "            <Keyword>view</Keyword>\n");
	ap_rprintf(r, "            <Keyword>stuff</Keyword>\n");
	ap_rprintf(r, "        </KeywordList>\n");
	ap_rprintf(r, "        <ContactInformation></ContactInformation>\n");
	ap_rprintf(r, "        <Fees>no conditions apply</Fees>\n");
	ap_rprintf(r, "        <AccessConstraints>None</AccessConstraints>\n");
	ap_rprintf(r, "    </Service>\n");
	ap_rprintf(r, "    <Capability>\n");
	ap_rprintf(r, "        <Request>\n");
	ap_rprintf(r, "            <GetCapabilities>\n");
	ap_rprintf(r, "                <Format>text/xml</Format>\n");
	ap_rprintf(r, "                <DCPType><HTTP><Get><OnlineResource xlink:type=\"simple\" xlink:href=\"%s?service=WMS\"/></Get></HTTP></DCPType>\n", WMTSHostname);
	ap_rprintf(r, "            </GetCapabilities>\n");
	ap_rprintf(r, "            <GetMap>\n");
	ap_rprintf(r, "                <Format>image/png</Format>\n");
	ap_rprintf(r, "                <Format>image/jpeg</Format>\n");
	ap_rprintf(r, "                <DCPType><HTTP><Get><OnlineResource xlink:type=\"simple\" xlink:href=\"%s?service=WMS\"/></Get></HTTP></DCPType>\n", WMTSHostname );
	ap_rprintf(r, "            </GetMap>\n");
	ap_rprintf(r, "        </Request>\n");
	ap_rprintf(r, "        <Layer opaque=\"0\" noSubsets=\"0\" queryable=\"0\">\n");
	ap_rprintf(r, "            <Title>All Coverage WMS</Title>\n" );
	ap_rprintf(r, "            <CRS>EPSG:4326</CRS>\n");
	ap_rprintf(r, "            <CRS>EPSG:3857</CRS>\n");
	ap_rprintf(r, "            <EX_GeographicBoundingBox>\n");
	ap_rprintf(r, "	             <westBoundLongitude>%f</westBoundLongitude>\n", -180.0);
	ap_rprintf(r, "	             <eastBoundLongitude>%f</eastBoundLongitude>\n",  180.0);
	ap_rprintf(r, "	             <southBoundLatitude>%f</southBoundLatitude>\n", -90.0);
	ap_rprintf(r, "	             <northBoundLatitude>%f</northBoundLatitude>\n",  90.0);
	ap_rprintf(r, "	           </EX_GeographicBoundingBox>\n");
	ap_rprintf(r, "            <BoundingBox CRS=\"EPSG:4326\" minx=\"%f\" miny=\"%f\" maxx=\"%f\" maxy=\"%f\"/>\n", -180.0, 	-90.0, 		180.0, 		90.0 );
	ap_rprintf(r, "            <BoundingBox CRS=\"EPSG:3857\" minx=\"%f\" miny=\"%f\" maxx=\"%f\" maxy=\"%f\"/>\n", -20026376.39, 	-20048966.10, 	20026376.39, 	20048966.10 );
	}




	 if ( ROOT != NULL ) while ( (ep = (*listSource)(dp)) ) {
        	if ( ep->d_name[0] == '.' ) continue; 

		if ( ( info->coverage != NULL ) && ( strcmp(ep->d_name,  info->coverage ) ) ) continue; // GetCoverage filter
		bzero(prod_path, MAX_STR_LEN - 1);
		snprintf(prod_path, MAX_STR_LEN, "%s/%s", ROOT, ep->d_name);

		//-------------------------------------------------------------------------------
		

		// JSON NEW PART

		snprintf(imgs_path, MAX_STR_LEN, "%s/DescribeCoverage.json", prod_path );


		DC = (*openSourceFile)( imgs_path, "r" );
		if ( DC == NULL ) continue;
		
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: [WMS] GetCapabilities Using JSON DescribeCoverage for %s ...", ep->d_name );  

		char 		*json_buff	= ( char *)malloc(sizeof(char)*4096);
		char 		*json_dc	= NULL;
		size_t 		json_len 	= 0;
		ssize_t 	json_n		= 0;
		json_object 	*jobj 		= NULL;
		json_object 	*jobj_dc	= NULL;


		while ( (*readSourceFile)(DC,"%s", json_buff) != EOF ){
			json_n = strlen(json_buff);
			json_dc = realloc(json_dc, json_len + json_n + 1);

			memcpy(json_dc + json_len, json_buff, json_n);
			json_len += json_n;
			json_dc[json_len] = '\0';
		}
		(*closeSourceFile)(DC); DC= NULL;

		jobj = json_tokener_parse(json_dc);
		if ( jobj != NULL){
			json_object_object_foreach(jobj, key, val){
				if (	(json_object_is_type(val, json_type_array)) 	&&
					( ! strcmp( key, "Collections" ))		){
					json_object_object_get_ex(jobj, key, &jobj_dc);
					break;
				}
			}
		}
		if ( jobj_dc != NULL ) {
			for( i = 0; i < json_object_array_length(jobj_dc); i++){
				proj = -1;
				json_object_object_foreach( json_object_array_get_idx(jobj_dc, i) , key, val){
					if 	( ! strcmp(key, "t_min" 	) ) t_min_tmp	 	= json_object_get_int(val);
					else if ( ! strcmp(key, "t_max" 	) ) t_max_tmp	 	= json_object_get_int(val);
                                        else if ( ! strcmp(key, "EPSG"          ) ) proj                = json_object_get_int(val);
                                        else if ( ! strcmp(key, "GeoX_ul"       ) ) x_input_ul      	= json_object_get_double(val);
                                        else if ( ! strcmp(key, "GeoY_ul"       ) ) y_input_ul      	= json_object_get_double(val);
                                        else if ( ! strcmp(key, "GeoY_lr"       ) ) y_input_lr      	= json_object_get_double(val);
                                        else if ( ! strcmp(key, "GeoX_lr"       ) ) x_input_lr      	= json_object_get_double(val);
					else continue; 
				}

				if ( proj != 4326 ){
			                OGRSpatialReferenceH geoSRSsrc = OSRNewSpatialReference(NULL);
			                OGRSpatialReferenceH geoSRSdst = OSRNewSpatialReference(NULL);

                			if ( ImportFromEPSG(&geoSRSsrc, proj ) != OGRERR_NONE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMS] EPSG for geometry `%d'", proj ); continue; }
                			if ( ImportFromEPSG(&geoSRSdst, 4326 ) != OGRERR_NONE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMS] EPSG for geometry `%d'", proj ); continue; }

                			#if GDAL_VERSION >= 304
                			OSRSetAxisMappingStrategy(geoSRSsrc, OAMS_TRADITIONAL_GIS_ORDER); // GDAL 3.0 Fix
                			OSRSetAxisMappingStrategy(geoSRSdst, OAMS_TRADITIONAL_GIS_ORDER); // GDAL 3.0 Fix
                			#endif

					OGRCoordinateTransformationH hCT = OCTNewCoordinateTransformation( geoSRSsrc, geoSRSdst ); 
					if ( hCT == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMS] CoordinateTransformation" ); continue; }
	                                if ( OCTTransform(hCT, 1, &x_input_ul, &y_input_ul, NULL) == FALSE ) continue; 
                                        if ( OCTTransform(hCT, 1, &x_input_lr, &y_input_lr, NULL) == FALSE ) continue;
					OCTDestroyCoordinateTransformation(hCT);

					OSRDestroySpatialReference(geoSRSsrc);
					OSRDestroySpatialReference(geoSRSdst);
				}					

				if ( i > 0 ) { 
					if ( t_min_tmp < t_min ) t_min = t_min_tmp; 
					if ( t_max_tmp > t_max ) t_max = t_max_tmp;  
					if ( GeoX_ul < x_input_ul ) GeoX_ul = x_input_ul;
					if ( GeoY_ul > y_input_ul ) GeoY_ul = y_input_ul;
					if ( GeoX_lr > x_input_lr ) GeoX_lr = x_input_lr;
					if ( GeoY_lr < y_input_lr ) GeoY_lr = y_input_lr;	
				} else {
					t_min = t_min_tmp; t_max = t_max_tmp;  
					GeoX_ul = x_input_ul;
					GeoY_ul = y_input_ul;
					GeoX_lr = x_input_lr;
					GeoY_lr = y_input_lr;
				}
			}
		}
		if ( proj < 0 ) continue;


		//-----------------------------------------------------------------------------------------------------
		// Core for print WMS coverage
		int DISABLE_EPSG_3857 = FALSE;
		x_input_ul = GeoX_ul; y_input_ul = GeoY_ul; x_input_lr = GeoX_lr; y_input_lr = GeoY_lr;
		OGRSpatialReferenceH geoSRSsrc = OSRNewSpatialReference(NULL);
	        OGRSpatialReferenceH geoSRSdst = OSRNewSpatialReference(NULL);
		if ( ImportFromEPSG(&geoSRSsrc, 4326 ) != OGRERR_NONE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMS] EPSG for geometry `%d'", 4326 ); return 500; }
		if ( ImportFromEPSG(&geoSRSdst, 3857 ) != OGRERR_NONE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMS] EPSG for geometry `%d'", 3857 ); return 500; }
		#if GDAL_VERSION >= 304
		OSRSetAxisMappingStrategy(geoSRSsrc, OAMS_TRADITIONAL_GIS_ORDER); // GDAL 3.0 Fix
		OSRSetAxisMappingStrategy(geoSRSdst, OAMS_TRADITIONAL_GIS_ORDER); // GDAL 3.0 Fix
		#endif

		OGRCoordinateTransformationH hCT = OCTNewCoordinateTransformation( geoSRSsrc, geoSRSdst ); if ( hCT == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMS] CoordinateTransformation" ); return 500; }

		if ( OCTTransform(hCT, 1, &x_input_ul, &y_input_ul, NULL) == FALSE ) 
			{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "WARN: [WMS] Unable to OCTTransform UL %f, %f, disable EPSG:3857 for %s", x_input_ul, y_input_ul, ep->d_name ); DISABLE_EPSG_3857 = TRUE; }
		if ( DISABLE_EPSG_3857 == FALSE )
		if ( OCTTransform(hCT, 1, &x_input_lr, &y_input_lr, NULL) == FALSE ) 
			{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "WARN: [WMS] Unable to OCTTransform LR %f, %f, disable EPSG:3857 for %s", x_input_ul, y_input_ul, ep->d_name ); DISABLE_EPSG_3857 = TRUE; }

		OCTDestroyCoordinateTransformation(hCT);
		OSRDestroySpatialReference(geoSRSsrc);
		OSRDestroySpatialReference(geoSRSdst);
	 

		// Coverage for 1.1.1
		if ( ! strcmp(info->version, "1.1.1") ) {
		ap_rprintf(r, "    <Layer queryable=\"1\" opaque=\"1\" cascaded=\"0\">\n");
		ap_rprintf(r, "        <Name>%s</Name>\n", 	ep->d_name);
		ap_rprintf(r, "        <Title>%s</Title>\n", 	ep->d_name);
		ap_rprintf(r, "        <SRS>EPSG:4326</SRS>\n");
		ap_rprintf(r, "        <BoundingBox SRS=\"EPSG:4326\" minx=\"%f\" miny=\"%f\" maxx=\"%f\" maxy=\"%f\" />\n", 	GeoX_ul, GeoY_lr, GeoX_lr, GeoY_ul );
		if ( DISABLE_EPSG_3857 == FALSE ){
		ap_rprintf(r, "        <SRS>EPSG:3857</SRS>\n");
		ap_rprintf(r, "        <BoundingBox SRS=\"EPSG:3857\" minx=\"%f\" miny=\"%f\" maxx=\"%f\" maxy=\"%f\"  />\n", 	x_input_ul, y_input_lr, x_input_lr, y_input_ul );
		}
		ap_rprintf(r, "        <LatLonBoundingBox  minx=\"%f\" miny=\"%f\" maxx=\"%f\" maxy=\"%f\" />\n", 		GeoX_ul, GeoY_lr, GeoX_lr, GeoY_ul );
		ap_rprintf(r, "        <Dimension name=\"TIME\" units=\"ISO8601\"/>\n");
		ap_rprintf(r, "        <Extent name=\"TIME\" default=\"%s\" earestValue=\"0\">%s/%s/PT1S</Extent>\n", datatimeStringFromUNIX( gmtime(&t_max) ), datatimeStringFromUNIX( gmtime(&t_min) ), datatimeStringFromUNIX( gmtime(&t_max) ));
		if ( colorTable != NULL ){
		ap_rprintf(r, "        <Style>\n");
		ap_rprintf(r, "          <Name>%s</Name>\n",   ep->d_name);
		ap_rprintf(r, "          <Title>%s</Title>\n", ep->d_name);
		ap_rprintf(r, "        </Style>\n");
		}
		ap_rprintf(r, "     </Layer>\n");
		// Coverage for 1.3.0
		} else if ( ! strcmp(info->version, "1.3.0") ) {
		ap_rprintf(r, "            <Layer queryable=\"1\" opaque=\"0\" cascaded=\"0\">\n");
		ap_rprintf(r, "                <Name>%s</Name>\n", 	ep->d_name);
		ap_rprintf(r, "                <Title>%s</Title>\n", 	ep->d_name);
		ap_rprintf(r, "                <CRS>EPSG:4326</CRS>\n");
		if ( DISABLE_EPSG_3857 == FALSE )
		ap_rprintf(r, "                <CRS>EPSG:3857</CRS>\n");
		ap_rprintf(r, "                <EX_GeographicBoundingBox>\n");
		ap_rprintf(r, "	                 <westBoundLongitude>%f</westBoundLongitude>\n", GeoX_ul);
		ap_rprintf(r, "	                 <eastBoundLongitude>%f</eastBoundLongitude>\n", GeoX_lr);
		ap_rprintf(r, "	                 <southBoundLatitude>%f</southBoundLatitude>\n", GeoY_lr);
		ap_rprintf(r, "	                 <northBoundLatitude>%f</northBoundLatitude>\n", GeoY_ul);
		ap_rprintf(r, "	               </EX_GeographicBoundingBox>\n");
		ap_rprintf(r, "                <BoundingBox CRS=\"EPSG:4326\" minx=\"%f\" miny=\"%f\" maxx=\"%f\" maxy=\"%f\"/>\n", GeoX_ul, GeoY_lr, GeoX_lr, GeoY_ul );
		if ( DISABLE_EPSG_3857 == FALSE )
		ap_rprintf(r, "                <BoundingBox CRS=\"EPSG:3857\" minx=\"%f\" miny=\"%f\" maxx=\"%f\" maxy=\"%f\"/>\n", x_input_ul, y_input_lr, x_input_lr, y_input_ul );
		ap_rprintf(r, "                <Dimension name=\"TIME\" units=\"ISO8601\" default=\"%s\" multipleValues=\"0\" nearestValue=\"0\" current=\"1\">%s/%s/PT1S</Dimension>\n", 
						datatimeStringFromUNIX(gmtime(&t_max)),datatimeStringFromUNIX(gmtime(&t_min)),datatimeStringFromUNIX( gmtime(&t_max)) );
		if ( colorTable != NULL ){
		ap_rprintf(r, "                <Style>\n");
		ap_rprintf(r, "                  <Name>%s</Name>\n",   style);
		ap_rprintf(r, "                  <Title>%s</Title>\n", style);
		ap_rprintf(r, "                </Style>\n");
		}
		ap_rprintf(r, "            </Layer>\n");
		}
	//-----------------------------------------------------------------------------------------------------

	}


	// Footer for version 1.1.1
	if ( ! strcmp(info->version, "1.1.1") ) {
	ap_rprintf(r, "   </Layer>\n");
	ap_rprintf(r, "</Capability>\n");
	ap_rprintf(r, "</WMT_MS_Capabilities>\n");
	// Footer for version 1.3.0
	} else if ( ! strcmp(info->version, "1.3.0") ) {
	ap_rprintf(r, "        </Layer>\n");
        ap_rprintf(r, "    </Capability>\n");
        ap_rprintf(r, "</WMS_Capabilities>\n");
	} 

	clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;      
        ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: [WMS] Finish in GetCapabilities (v. %s) in %.3f sec, Total time %.3f sec", info->version, 
			(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
	return 0;
}



//------------------------------------------------------------------------------------------

int WMSGetTileURL(struct info *info){
	request_rec  *r = info->r; int i;
	
        ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: [WMS] Tile request using Key and Value query ...");
	extern  char    *WMTSCachePath;
	extern  long int GeomPOSTSize;

        char 		*wmts_uri 	= NULL; wmts_uri = malloc(strlen(info->uri) + 1 ); strcpy(wmts_uri, info->uri);
        char 		*wmts_tok 	= NULL; int wmts_chek = TRUE;
	char 		*style_tok 	= NULL;
	char 		*CoverageId 	= NULL;
	char 		*time_s	 	= NULL;
	char 		*time_e	 	= NULL;
	char 		*format	 	= NULL;
	char 		*style	 	= NULL;
	char 		*merge	 	= NULL;
        int 		TileMatrix  	= -1;
        int 		TileCol     	= -1;
        int 		TileRow     	= -1;
	int 		epsg 		= 4326;
	char    	*pszKey         = NULL;
	char		*style_ptr	= NULL;
	char		*p		= NULL;
	const char      *pszValue;
	char            colortable[128] = " colortable=default", colorrange[128] = " colorrange=default" , nodata[128] = " nodata=default", token[128];
	int		width		= 0;
	int		height		= 0;
	double		bbox[4]		= { 0.0, 0.0, 0.0, 0.0 };
	char		*mwcs_extra	= NULL;

	long int        len             = 0;
	char            *query          = NULL;
	char 		*tok            = NULL;
	char 		*query_ptr	= NULL;
	
	double x_input_ul, y_input_ul, x_input_lr, y_input_lr;

	addStatToPush(info, "action", "wms",  GFT_String );

	if ( info->query_string != NULL ) {
		len = strlen(info->query_string); 	if( len 		<= 0    ){ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMS] Error in invocation - wrong FORM probably (Size zero)"); 	return FALSE; }
	        query = malloc(len + 1); 		if ( query 		== NULL ){ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMS] malloc query %s", info->query_string); 			return FALSE; }
	        bzero(query, len);  unencode(info->query_string, query);

		tok = strtok_r( query, DELIMS, &query_ptr );
		while( tok != NULL ) {
			pszValue = (const char *)CPLParseNameValue( tok, &pszKey );
			if ( pszValue == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMS] Unable to parsing '%s'", tok); return FALSE; }
			for(p = pszKey;*p;++p) *p=*p>0x40&&*p<0x5b?*p|0x60:*p;
			if 	( ( ! strcmp(pszKey, "service") ) && (  strcmp(pszValue, "WMS" 		) ) ) return FALSE;
			else if (   ! strcmp(pszKey, "format" ) ) { format  	 = malloc( strlen(pszValue) + 1) ; bzero(format,       strlen(pszValue) ); strcpy(format,     	pszValue); }
			else if (   ! strcmp(pszKey, "layers" ) ) { CoverageId   = malloc( strlen(pszValue) + 1) ; bzero(CoverageId,   strlen(pszValue) ); strcpy(CoverageId, 	pszValue); }
			else if (   ! strcmp(pszKey, "time"   ) ) { time_s       = malloc( strlen(pszValue) + 1) ; bzero(time_s,       strlen(pszValue) ); strcpy(time_s, 	pszValue); }
			else if (   ! strcmp(pszKey, "styles" ) ) { style        = malloc( strlen(pszValue) + 1) ; bzero(style,        strlen(pszValue) ); strcpy(style,      	pszValue); }
			else if (   ! strcmp(pszKey, "token"  ) ) { info->token  = malloc( strlen(pszValue) + 1) ; bzero(info->token,  strlen(pszValue) ); strcpy(info->token, 	pszValue); }
			else if (   ! strcmp(pszKey, "module" ) ) { info->module = malloc( strlen(pszValue) + 1) ; bzero(info->module, strlen(pszValue) ); strcpy(info->module, pszValue); }
			else if (   ! strcmp(pszKey, "width"  ) ) { width  	 = atoi(pszValue); if ( width  <= 0 ) return FALSE; } 
			else if (   ! strcmp(pszKey, "height" ) ) { height 	 = atoi(pszValue); if ( height <= 0 ) return FALSE; }
			else if (   ! strcmp(pszKey, "bbox"   ) ) { if ( sscanf(pszValue,"%lf,%lf,%lf,%lf", &bbox[0], &bbox[1], &bbox[2], &bbox[3] ) != 4 ) return FALSE; }
			else if (   ! strcmp(pszKey, "crs"    ) ) {
                        	if 	( ! strcmp(pszValue, "EPSG:4326") ) epsg = 4326;
				else if	( ! strcmp(pszValue, "CRS:84"	) ) epsg = 4326;
				else if ( ! strcmp(pszValue, "EPSG:3857") ) epsg = 3857;
				else	return FALSE;
				}
		//	else 				 { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMS] Unsupported request %s", pszValue); return FALSE; }
			
			query = query_ptr; tok = strtok_r( query, DELIMS, &query_ptr );	
		}
//	       	info->query_string = NULL;
	}

	if ( CoverageId == NULL ) return FALSE;
	if ( width  	<= 0 	) return FALSE;
	if ( height 	<= 0 	) return FALSE; 
	if ( ( bbox[0] == 0 ) && ( bbox[1] == 0 ) && ( bbox[2] == 0 ) && ( bbox[3] == 0 ) ) return FALSE; 





	if ( CoverageId != NULL ) addStatToPush(info, "datasetId",  CoverageId, GFT_String ); // STATS


	if 	( ! strcmp(info->version, "1.1.1") ) { x_input_ul = bbox[0]; y_input_ul = bbox[3]; x_input_lr = bbox[2]; y_input_lr = bbox[1]; } // -180,-90,180,90
	else if ( ! strcmp(info->version, "1.3.0") ) { x_input_ul = bbox[1]; y_input_ul = bbox[2]; x_input_lr = bbox[3]; y_input_lr = bbox[0]; } // -90,-180,90,180
	else	return FALSE;

	if ( y_input_ul == y_input_lr ) { WMSErrorMessage(r, 0, info->version); return FALSE; } 
	if ( x_input_ul == x_input_lr ) { WMSErrorMessage(r, 0, info->version); return FALSE; } 



	if ( epsg != 4326 ){
                OGRSpatialReferenceH geoSRSsrc = OSRNewSpatialReference(NULL);
                OGRSpatialReferenceH geoSRSdst = OSRNewSpatialReference(NULL);

		if ( ImportFromEPSG(&geoSRSsrc, epsg ) != OGRERR_NONE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMS] EPSG for geometry `%d'", epsg ); return FALSE; }
		if ( ImportFromEPSG(&geoSRSdst, 4326 ) != OGRERR_NONE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMS] EPSG for geometry `%d'", epsg ); return FALSE; }

		#if GDAL_VERSION >= 304
		OSRSetAxisMappingStrategy(geoSRSsrc, OAMS_TRADITIONAL_GIS_ORDER); // GDAL 3.0 Fix
		OSRSetAxisMappingStrategy(geoSRSdst, OAMS_TRADITIONAL_GIS_ORDER); // GDAL 3.0 Fix
		#endif

		OGRCoordinateTransformationH hCT = OCTNewCoordinateTransformation( geoSRSsrc, geoSRSdst );
                if ( OCTTransform(hCT, 1, &x_input_ul, &y_input_ul, NULL) == FALSE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMS] EPSG OCTTransform for geometry `%d'", epsg ); return FALSE; }
                if ( OCTTransform(hCT, 1, &x_input_lr, &y_input_lr, NULL) == FALSE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMS] EPSG OCTTransform for geometry `%d'", epsg ); return FALSE; }
		OCTDestroyCoordinateTransformation(hCT);

		OSRDestroySpatialReference(geoSRSsrc);
		OSRDestroySpatialReference(geoSRSdst);
	}


        info->query_string 	= malloc( MAX_STR_LEN + 1 ); bzero(info->query_string, MAX_STR_LEN);
	info->passthrough 	= TRUE; 
	info->x_input_ul 	= x_input_ul; 
	info->x_input_lr 	= x_input_lr; 
	info->y_input_ul 	= y_input_ul; 
	info->y_input_lr 	= y_input_lr; 



	// Add extra ultra secret parameters from POST to WCS query 
	if ( ! strcmp(r->method, "POST") ) {
		if ( info->pairs == NULL ) {
		        if ( GeomPOSTSize > 0 ) ap_parse_form_data(r, NULL, &info->pairs, -1, GeomPOSTSize );
		        else                    ap_parse_form_data(r, NULL, &info->pairs, -1, 1024 * 1024 * 3 );
		}
		apr_array_header_t *pairs = apr_array_copy(r->pool, info->pairs);
		while (pairs && !apr_is_empty_array(pairs)) {
	                apr_off_t       len      = 0;
	                apr_size_t      size     = 0;
	                ap_form_pair_t  *pair   = (ap_form_pair_t *) apr_array_pop(pairs);
			if ( strcmp(pair->name, "mwcs") ) continue;

	                apr_brigade_length(pair->value, 1, &len);       size = (apr_size_t) len;
	                mwcs_extra = apr_palloc(r->pool, size + 2);    	apr_brigade_flatten(pair->value, mwcs_extra + 1, &size); mwcs_extra[len+1] = '\0'; mwcs_extra[0] = ';';
			for(i = 0; i < ( len + 1 ) ; i++ ) if ( mwcs_extra[i] ==';' ) mwcs_extra[i] = '&';


			break;
       		} 
	}

	double color_range_min = 0.0, color_range_max = 0.0, nodata_user_defined = 0.0;
	if ( style == NULL )
		sprintf(info->query_string,
		         "/wcs?service=WCS&Request=GetCoverage&version=2.0.0&subset=unix(%s)&format=%s&epsg_out=EPSG:%d&crop=false&subset=Lat(%.10f,%.10f)&subset=Long(%f,%f)&size=(%d,%d)&CoverageId=%s&wmts=true&filter=false&math=single",
	        	  time_s, format, epsg, y_input_lr, y_input_ul, x_input_ul, x_input_lr, width, height, CoverageId  ); 
	else {
		sprintf(info->query_string,
		         "/wcs?service=WCS&Request=GetCoverage&version=2.0.0&subset=unix(%s)&format=%s&epsg_out=EPSG:%d&crop=false&subset=Lat(%.10f,%.10f)&subset=Long(%f,%f)&size=(%d,%d)&CoverageId=%s&wmts=true&filter=false&math=single",
	        	  time_s, format, epsg, y_input_lr, y_input_ul, x_input_ul, x_input_lr, width, height, CoverageId  ); 

		style_tok = strtok_r( style, ";", &style_ptr ); 
		if ( style_tok != NULL ) {
			sprintf(colortable, "&colortable=%s", style_tok ); strcat(info->query_string, colortable); 
			style = style_ptr; style_tok = strtok_r( style, ";", &style_ptr ); 
		}
		while( style_tok != NULL ) {
			pszValue = (const char *)CPLParseNameValue( style_tok, &pszKey );
			if ( pszValue == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WTS] Unable to parsing style option '%s'", style_tok); return FALSE; }
			for(p = pszKey;*p;++p) { *p=*p>0x40&&*p<0x5b?*p|0x60:*p; if ( *p == '&' ) return FALSE; }

			if 		( ! strcmp(pszKey, "colorrange") ) 	{ sscanf(pszValue, "(%lf,%lf)", &color_range_min, &color_range_max ); sprintf(colorrange, "&colorrange=%s", 	pszValue); 		strcat(info->query_string, colorrange); }
			else if		( ! strcmp(pszKey, "nodata") ) 		{ nodata_user_defined = atof(pszValue); 			      sprintf(nodata, 	  "&nodata=%lf", 	nodata_user_defined ); 	strcat(info->query_string, nodata); 	}
			else if 	( ! strcmp(pszKey, "proc") ) 		{ merge = malloc(strlen(pszValue) + 1); bzero(merge,strlen(pszValue));sprintf(merge, 	  "&proc=%s", 		pszValue); 		strcat(info->query_string, merge); 	}

			style = style_ptr; style_tok = strtok_r( style, ";", &style_ptr );
		}

	}

	if ( info->cache == TRUE ) {
		char *md5 = NULL; md5 = malloc(33); bzero(md5, 32); md5_tile(info, md5);
		info->tile_path = malloc(MAX_PATH_LEN); bzero( info->tile_path, MAX_PATH_LEN - 1 );

		snprintf(info->tile_path, MAX_PATH_LEN, "%s/wms/%s", 
				( WMTSCachePath != NULL ) ? WMTSCachePath : "", md5 );

	}

        if ( mwcs_extra  != NULL ) strcat(info->query_string, mwcs_extra);

        return TRUE;



}



//------------------------------------------------------------------------------------------


int WMTSUrlParser(struct info *info){
	int i;
	long int        len             = 0;
	char            *query          = NULL;
	char 		*tok            = NULL;
	char            *pszKey         = NULL;
	char 		*query_ptr	= NULL;
	char		*p		= NULL;
	char		*outFormat	= NULL;
	int		mode		= FALSE;
	int		error		= 0;
	char		*time		= NULL;
	const char      *pszValue;
	request_rec 	*r		= info->r;
	char            *query_string   = info->query_string;


	char		*layer		= NULL;
	char		*style		= NULL;
	char		*tilematrixset	= NULL;
	char		*TileMatrix	= NULL;
	char		*TileCol	= NULL;
	char		*TileRow	= NULL;
	char		*TimeStart	= NULL;
	char		*TimeEnd	= NULL;


	ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: [WMTS/WMS] Request, decoding for WCS core ..."); 
	addStatToPush(info, "wmts_query", "1", 	   GDT_Int32  ); 	
	addStatToPush(info, "action", 	  "wmts",  GFT_String ); 

	if ( ( query_string != NULL ) && ( query_string[0] == '?' ) ) query_string++; // It's a fucking stupid thing, but the OCG testing tool of my balls add a ?

	if ( ! strncmp(info->uri, "/wmts/", 6 ) ) { if ( WMTSGetTileURL(info) == TRUE ) return WCS; else { info->exit_code = HTTP_NOT_FOUND; return FALSE; } }

					if( query_string  	== NULL ){ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] Error in invocation - wrong FORM probably (NULL)"); 	return FALSE; }
	len = strlen(query_string); 	if( len 		<= 0    ){ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] Error in invocation - wrong FORM probably (Size zero)"); 	return FALSE; }
        query = malloc(len + 1); 	if ( query 		== NULL ){ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] malloc query %s", query_string); 				return FALSE; }
        bzero(query, len);  unencode(query_string, query);

	
	int Request = -1;
	info->exit_code = HTTP_BAD_REQUEST;
	tok = strtok_r( query, DELIMS, &query_ptr );
	while( tok != NULL ) {
		pszValue = (const char *)CPLParseNameValue( tok, &pszKey );
		if ( pszValue == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] Unable to parsing '%s'", tok); WMTSErrorMessage(r, MissingParameterValue); return FALSE; }
		for(p = pszKey;*p;++p) *p=*p>0x40&&*p<0x5b?*p|0x60:*p;
		
		if 		( ! strcmp(pszKey, "service") ) {	 mode = TRUE;
			       	if 	( ! strcmp(pszValue, "WMTS" ) )  mode = WMTS;
			       	else if ( ! strcmp(pszValue, "WMS"  ) )  mode = WMS;		
		}

		else if 	( ! strcmp(pszKey, "version") 	) { info->version 	= malloc( strlen(pszValue) + 1) ; bzero(info->version, 		strlen(pszValue) ); strcpy(info->version, 	pszValue); }
		else if 	( ! strcmp(pszKey, "token") 	) { info->token 	= malloc( strlen(pszValue) + 1) ; bzero(info->token, 		strlen(pszValue) ); strcpy(info->token, 	pszValue); }
		else if 	( ! strcmp(pszKey, "module") 	) { info->module 	= malloc( strlen(pszValue) + 1) ; bzero(info->module, 		strlen(pszValue) ); strcpy(info->module, 	pszValue); }
		else if 	( ! strcmp(pszKey, "coverage") 	) { info->coverage 	= malloc( strlen(pszValue) + 1) ; bzero(info->coverage, 	strlen(pszValue) ); strcpy(info->coverage, 	pszValue); }
		else if 	( ! strcmp(pszKey, "time") 	) { time 		= malloc( strlen(pszValue) + 1) ; bzero(time, 			strlen(pszValue) ); strcpy(time, 		pszValue); }

		else if		( ! strcmp(pszKey, "request") ) {
				if      ( ! strcmp(pszValue, "GetCapabilities"	) )		Request = GetCapabilities;
				else if ( ! strcmp(pszValue, "GetMap"		) )		Request = GetMap;
				else if ( ! strcmp(pszValue, "GetTile"		) )		Request = GetTile;
				else 								{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] Unsupported request %s", pszValue);  }
		}

		else if 	( ! strcmp(pszKey, "layer") 		) { layer	  = malloc( strlen(pszValue) + 1) ; bzero(layer		, 	strlen(pszValue) ); strcpy(layer	, pszValue); }
		else if 	( ! strcmp(pszKey, "style") 		) { style	  = malloc( strlen(pszValue) + 1) ; bzero(style		, 	strlen(pszValue) ); strcpy(style	, pszValue); }
		else if 	( ! strcmp(pszKey, "tilematrixset") 	) { tilematrixset = malloc( strlen(pszValue) + 1) ; bzero(tilematrixset	, 	strlen(pszValue) ); strcpy(tilematrixset, pszValue); }
		else if 	( ! strcmp(pszKey, "tilematrix") 	) { TileMatrix	  = malloc( strlen(pszValue) + 1) ; bzero(TileMatrix	, 	strlen(pszValue) ); strcpy(TileMatrix	, pszValue); }
		else if 	( ! strcmp(pszKey, "tilecol") 		) { TileCol	  = malloc( strlen(pszValue) + 1) ; bzero(TileCol	, 	strlen(pszValue) ); strcpy(TileCol	, pszValue); }
		else if 	( ! strcmp(pszKey, "tilerow") 		) { TileRow	  = malloc( strlen(pszValue) + 1) ; bzero(TileRow	, 	strlen(pszValue) ); strcpy(TileRow	, pszValue); }
		else if 	( ! strcmp(pszKey, "timestart") 	) { TimeStart	  = malloc( strlen(pszValue) + 1) ; bzero(TimeStart	, 	strlen(pszValue) ); strcpy(TimeStart	, pszValue); }
		else if 	( ! strcmp(pszKey, "timeend") 		) { TimeEnd	  = malloc( strlen(pszValue) + 1) ; bzero(TimeEnd	, 	strlen(pszValue) ); strcpy(TimeEnd	, pszValue); }



		query = query_ptr; tok = strtok_r( query, DELIMS, &query_ptr );
	}


	if ( info->version == NULL ) {
		if ( mode == WMTS ) { info->version = malloc(10) ; bzero(info->version, 9 ); strcpy(info->version, "1.0.0" ); }
		if ( mode == WMS  ) { info->version = malloc(10) ; bzero(info->version, 9 ); strcpy(info->version, "1.3.0" ); }
	}

	if ( mode == TRUE  ) { WMTSErrorMessage(r, InvalidParameterValue); return FALSE; }
	if ( mode == FALSE ) { WMTSErrorMessage(r, MissingParameterValue); return FALSE; }
	if ( Request < 0   ) { WMTSErrorMessage(r, InvalidParameterValue); return FALSE; }

	if ( info->version == NULL ) 										return FALSE;
	if ( ( mode == WMTS ) && ( strcmp(info->version, "1.0.0") ) ) 						return FALSE;
	if ( ( mode == WMS  ) && ( strcmp(info->version, "1.1.1") ) && ( strcmp(info->version, "1.3.0") ) ) 	return FALSE;

	if ( ( mode == WMS  ) && ( Request == GetCapabilities ) && ( layer != NULL ) ) { info->coverage = malloc( strlen(layer) + 1) ; bzero(info->coverage, strlen(layer) ); strcpy(info->coverage,  layer); }
	if ( ( mode == WMTS ) && ( Request == GetCapabilities ) ) { info->func = WMTSGetCapabilities;  return TRUE; 		}
	if ( ( mode == WMS  ) && ( Request == GetCapabilities ) ) { info->func = WMSGetCapabilities;   return TRUE; 		}
	if ( ( mode == WMS  ) && ( Request == GetMap 	      ) ) { if ( WMSGetTileURL(info) == TRUE ) return WCS; else FALSE;  }
	if ( ( mode == WMTS ) && ( Request == GetTile 	      ) ) {
		if ( ( layer != NULL ) && ( style != NULL ) && ( TimeStart != NULL ) && ( TimeEnd != NULL ) && ( tilematrixset != NULL ) && ( TileMatrix != NULL ) && ( TileCol != NULL ) && ( TileRow != NULL ) ) {
			free(info->uri); info->uri = (char *)malloc( MAX_STR_LEN ); bzero(info->uri, MAX_STR_LEN - 1); free(info->query_string); info->query_string = NULL;
			snprintf(info->uri, MAX_STR_LEN, "/wmts/%s/%s/%s/%s/%s/%s/%s/%s.png", layer, style, TimeStart, TimeEnd, tilematrixset, TileMatrix, TileCol, TileRow );
			if ( WMTSGetTileURL(info) == TRUE ) return WCS; else { info->exit_code = HTTP_NOT_FOUND; return FALSE; }
		}


	}

	return FALSE;
}



