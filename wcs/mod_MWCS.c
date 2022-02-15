#include "includes.h"
#include "datatypes.h"


// Global Varibles

enum assoc { A_NONE, A_L, A_R };
pat_t pat_eos = {"", A_NONE, 0};

pat_t pat_ops[] = {
        {"^\\)",        A_NONE, -1},
        {"^AND",        A_L,    12},
        {"^OR",         A_L,    11},
        {"^\\*\\*",     A_R,    10},
        {"^\\^",        A_R,    10},
        {"^\\*",        A_L,    9},
        {"^/",          A_L,    9},
        {"^%",          A_L,    9},
        {"^\\+",        A_L,    8},
        {"^-",          A_L,    8},
        {"^<<",         A_L,    7},
        {"^>>",         A_L,    7},
        {"^<=",         A_L,    6},
        {"^>=",         A_L,    6},
        {"^<",          A_L,    6},
        {"^>",          A_L,    6},
        {"^!=",         A_L,    5},
        {"^==",         A_L,    5},
        {"^&",          A_L,    4},
        {"^\\|",        A_L,    3},
        {0}
};



pat_t pat_arg[] = {
        {"^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?"},
        {"^[a-zA-Z_][a-zA-Z_0-9]*[@0-9_0-9]*[:a-z]*"},
        {"^\\(", A_L,   -1},
        {0}
};


char 	**ENVIRONMENT_VARIABLE[2] = { NULL, NULL };
char    OGC_dataTypes[20][60] = {
                                "none",                                                         // GDT_Unknown
                                "http://www.opengis.net/def/dataType/OGC/0/unsignedByte",       // GDT_Byte
                                "http://www.opengis.net/def/dataType/OGC/0/unsignedShort",      // GDT_UInt16
                                "http://www.opengis.net/def/dataType/OGC/0/signedShort",        // GDT_Int16
                                "http://www.opengis.net/def/dataType/OGC/0/unsignedInt",        // GDT_UInt32
                                "http://www.opengis.net/def/dataType/OGC/0/signedInt",          // GDT_Int32
                                "http://www.opengis.net/def/dataType/OGC/0/float32",            // GDT_Float32
                                "http://www.opengis.net/def/dataType/OGC/0/float64",            // GDT_Float64
                        };

char    dimenstion_unit[3][20]          = { "degree", "meter", "high" };
char    dimenstion_Label[3][2][20]      = {
                                                { "Lat",       "Long"   },
                                                { "N",          "E"     },
                                                { "h",          "h"  }

                                        };


char    time_unit[2][20]                = { "d",        "s" };
char    time_Label[2][20]               = { "ansi",     "t" };
char    time_URL[2][120]                = {
                                                "http://www.opengis.net/def/crs/OGC/0/AnsiDate",
                                                "http://www.opengis.net/def/crs/OGC/0/Temporal?epoch=&quot;1970-01-01T00:00:00&quot;&amp;uom=&quot;s&quot;"
                                        };


char 			*ROOT 			= NULL;
char  			*AWS_ACCESS_KEY_ID      = NULL;
char			*AWS_SECRET_ACCESS_KEY  = NULL;
char			*AWS_DEFAULT_REGION	= NULL;
char			*AWS_S3_ENDPOINT	= NULL;
char			*IOThreads		= NULL;
char			*MaxMGRSTiles		= NULL;
char			*WMTSCachePath		= NULL;
long int		WMTSMaxTileAge		= 3600 * 24 * 7;
double			WMTSMaxTileRatio	= 0;
char			*ColorTableUrl		= NULL;
time_t			L_MaxTimeRange		= 0;
long int		L_MaxHitsNum		= 0;
long int 		L_MaxMemoryUse		= 0;
int			L_MaxTasks		= 0;
double			MemoryCalcFactor	= 1.0;
double			L_MaxSixeX		= 0.0;
double			L_MaxSixeY		= 0.0;
long int		GeomPOSTSize		= 0;
char			*WaterMark		= NULL;
int			KEEP_OLD_STYLE_NAME	= FALSE;
int			ENABLE_DEBUG_TOKEN	= FALSE;
int			EXPERIMENTAL_MODE	= FALSE;
double			MAX_USABLE_MEMORY	= 90; 	// % of total ram for a single process
long int		MAX_TIME_QUERY		= 1800; // sec

Unknown_EPSG_proj4 	*EPSG_proj4 		= NULL;
char			*OAuthServer		= NULL;
char			*OAuthId		= NULL;

char			*LogServerURL		= NULL;
char			*ExtHostname		= NULL;
char			*WMTSHostname		= NULL;
char			*APIDatacubeUrl		= NULL;


memcached_server_st 	*MEMC_servers 		= NULL;
memcached_st 		*MEMC_memc		= NULL;
struct module_info 	*module_table		= NULL;

pthread_mutex_t 	gate;


char *MWCSTitle			= NULL;
char *MWCSProviderName		= NULL;
char *MWCSProviderSite		= NULL;
char *MWCSIndividualName	= NULL;
char *MWCSElectronicMailAddress	= NULL;
char *MWCSCity			= NULL;
char *MWCSPostalCode		= NULL;
char *MWCSCountry		= NULL;


//------------------------------------------------------------------------------------------------

// Prototype function
int 	getData( struct info *info );
int 	WMTSUrlParser(struct info *info);
int 	APIUrlParser(struct info *info);
void 	*thread_worker(void *data);
void 	*imagepackmule(void *data);
int 	addStatToPush( struct info *info, char *key, char *value, int type);
int 	ApplyMagicFilter(struct info *info, GByte  **realRaster, vsi_l_offset *pnOutDataLength, char *func, char *params );

//------------------------------------------------------------------------------------------------

static void lock_cb(  CURL *handle, curl_lock_data data, curl_lock_access access, void *userptr) { struct info *info = (struct info *)userptr; pthread_mutex_lock(     &info->connlock ); }
static void unlock_cb(CURL *handle, curl_lock_data data,			  void *userptr) { struct info *info = (struct info *)userptr; pthread_mutex_unlock(   &info->connlock ); }
static void init_curl_locks(pthread_mutex_t  *connlock){  pthread_mutex_init(connlock, NULL); }
static void kill_curl_locks(pthread_mutex_t  *connlock){  pthread_mutex_destroy(connlock); }


void *thread_worker(void *data){
	struct info *info = data;
	int *status 	= (int *)malloc(sizeof(int));
	*status		= 0;
	int (*func)(struct info *) = info->func;

	//------------------------------------------------------------------------------------------------
	if ( (info->query_string) || (info->uri) ) {
		*status 	= func(info); 
		// Backup content type ... Why? Dunno BRO! I fucking dont know!
		if ( info->r->content_type != NULL ) { info->content_type = (char *)malloc(strlen(info->r->content_type) + 1); strcpy(info->content_type, info->r->content_type); }

		addStatToPush(info, "output_format",  (char *)info->r->content_type, GFT_String ); // STATS

		pthread_exit( (void *) status );
	}
	//------------------------------------------------------------------------------------------------
	

	pthread_exit( (void *) status );

}

unsigned long fsize(FILE *f){
	fseek(f, 0, SEEK_END);
	unsigned long len = (unsigned long)ftell(f);
	return len;
}

int fexists(const char *fname){
	FILE *file;
	if ( (file = fopen(fname, "r")) ){ fclose(file);  return 1; }
	else return 0;
							    
}

const char *get_filename_ext(const char *filename) {
	const char *dot = strrchr(filename, '.');
	if(!dot || dot == filename) return NULL;
	return dot + 1;
}

int init_nfs_table(struct info *info){
	FILE *mtab = NULL;

	char	mount_dev[256]	= {};
	char	mount_dir[256]	= {};
	char	mount_type[256]	= {};
	char	mount_opts[256]	= {};
	int 	mount_freq;
	int 	mount_passno;
	int	mount_dev_len;
	int	mount_dir_len;	


 		
	mtab = 	fopen ("/proc/mounts", "r");
	if ( mtab == NULL ) return 1;

	struct  nfs_mounts *nfs_cursor = NULL;

//	struct  nfs_mounts *nfs_cursor = info->nfs_mount_points;
//	if ( nfs_cursor != NULL ) for ( nfs_cursor = info->nfs_mount_points; nfs_cursor->next != NULL; nfs_cursor = nfs_cursor->next );

	int i = 0; bzero(mount_dev, 255); bzero(mount_dir, 255); bzero(mount_type, 255); bzero(mount_opts, 255);
	while ( fscanf(mtab, "%255s %255s %255s %255s %d %d\n", mount_dev, mount_dir, mount_type, mount_opts, &mount_freq, &mount_passno) != EOF ){

		if ( strncmp(mount_type, "nfs", 3) ) continue;
		if ( info->nfs_mount_points == NULL )   { info->nfs_mount_points 	= nfs_cursor = (struct nfs_mounts *)malloc(sizeof(struct  nfs_mounts)); 		 nfs_cursor->next = NULL; }
                else                 		   	{ nfs_cursor->next   		= (struct nfs_mounts *)malloc(sizeof(struct nfs_mounts)); nfs_cursor = nfs_cursor->next; nfs_cursor->next = NULL; }

		mount_dev_len = strlen(mount_dev);
		mount_dir_len = strlen(mount_dir);
		if ( mount_dev[mount_dev_len - 1] == '/' ) { mount_dev[mount_dev_len - 1] = '\0'; mount_dev_len -= 1; }
		if ( mount_dir[mount_dir_len - 1] == '/' ) { mount_dir[mount_dir_len - 1] = '\0'; mount_dir_len -= 1; }

		
		nfs_cursor->src = malloc( mount_dev_len + 1 ); bzero(nfs_cursor->src, mount_dev_len); strncpy(nfs_cursor->src, mount_dev, mount_dev_len ); nfs_cursor->src[mount_dev_len] = '\0';
		nfs_cursor->dst = malloc( mount_dir_len + 1 ); bzero(nfs_cursor->dst, mount_dir_len); strncpy(nfs_cursor->dst, mount_dir, mount_dir_len ); nfs_cursor->dst[mount_dir_len] = '\0';
		bzero(mount_dev, 255); bzero(mount_dir, 255); bzero(mount_type, 255); bzero(mount_opts, 255);
	}
	fclose(mtab);

	return 0;

}

char *find_nfs_table(struct info *info, char *src){ 
	for ( struct  nfs_mounts *nfs_cursor = info->nfs_mount_points; nfs_cursor != NULL; nfs_cursor = nfs_cursor->next ) if ( ! strcmp(nfs_cursor->src, src ) ) return nfs_cursor->dst; 
	return NULL;
}


//------------------------------------------------------------------------------------------------

int init_module_table(){
	DIR *d;
	struct dirent *dir;
	void *handle;
	int reti;
	int (*mod)	( GDALDatasetH  *, struct loadmule *);
	int (*manifest)	(char *, char *, char *);
	char name[256], version[256], regex[256];

	char path[MAX_PATH_LEN];
	struct module_info *cursor;

	d = opendir(MODULES_PATH); if (d) {
		while ((dir = readdir(d)) != NULL) { if ( dir->d_name[0]  == '.' ) continue; if ( ! strstr(dir->d_name, ".so") ) continue;
			bzero(path, MAX_PATH_LEN - 1); sprintf(path, "%s/%s", MODULES_PATH, dir->d_name);

			handle 		= dlopen(path, RTLD_LAZY); 		if (!handle) 	 	continue;
			manifest	= dlsym(handle, "manifest");		if (manifest == NULL) 	{ dlclose(handle); continue; }
			mod    		= dlsym(handle, "core");		if (mod      == NULL) 	{ dlclose(handle); continue; }


			bzero(name, 255); bzero(version, 255); bzero(regex, 255);
			if ( (*manifest)(  name, version, regex)  == FALSE ) { dlclose(handle); continue; }


			if ( module_table == NULL ) 	{ module_table = cursor = (struct module_info *)malloc(sizeof(struct module_info)); 			}
			else				{ cursor->next = (struct module_info *)malloc(sizeof(struct module_info)); cursor = cursor->next;	}

			cursor->next 	= NULL;
			cursor->handle	= handle;
			cursor->mod	= mod;
			cursor->name 	= malloc(strlen(name) 	 + 1); strcpy(cursor->name, 	name);
			cursor->version = malloc(strlen(version) + 1); strcpy(cursor->version, 	version);
			if ( regex[0] != '\0' ) { cursor->regex = malloc(sizeof(regex_t)); reti = regcomp(cursor->regex, regex, REG_EXTENDED); if (reti) cursor->regex = NULL; }
			else cursor->regex = NULL; 

			fprintf(stderr, "Loading module %s (%s): %s\n", cursor->name, cursor->version, cursor->regex != NULL ? regex : "no regex defined" ); fflush(stderr);
		}
		closedir(d);
	}
	return 0;
}

void *use_module_by_name(char *name, char *version){
	struct module_info *cursor; if ( name == NULL ) return NULL;

	for ( cursor = module_table; cursor != NULL; cursor = cursor->next){
		if ( strcmp(cursor->name, name) ) 				continue;
		if ( ( version != NULL ) &&  strcmp(cursor->version, version) ) continue;
		return cursor->mod;
	}

	return NULL;
}

void *use_module_by_regex(char *name, char *version){
	struct module_info *cursor; if ( name == NULL ) return NULL;

	for ( cursor = module_table; cursor != NULL; cursor = cursor->next){
		if ( cursor->regex == NULL ) continue;
	        if ( regexec(cursor->regex, name, 0, NULL, 0) != 0 )		continue;
		if ( ( version != NULL ) &&  strcmp(cursor->version, version) ) continue;
		return cursor->mod;
	}

	return NULL;
}


//------------------------------------------------------------------------------------------------

char *getValueFromKey(const char *fname, char *key){
	FILE *file;
	char 	*keyname = NULL;
	char	*val	 = NULL;
	char	line[MAX_STR_LEN];
	char	*none 	 = malloc(2); none[0] = '0'; none[1] = '\0';
	int i;	

	if ( (file = fopen(fname, "r")) ){ 
	
		while ( fgets(line, MAX_STR_LEN, file)  ){
			if ( line[0] == '\n' ) continue;
			if ( line[0] == '#'  ) continue;
			val = (char *)CPLParseNameValue( line, &keyname );
			for (i = 0; i < strlen(val); 	 i++ ) if (( val[i]     == ' ' ) || ( val[i]     == '\n' )) { val[i]     = '\0'; break; }
			for (i = 0; i < strlen(keyname); i++ ) if (( keyname[i] == ' ' ) || ( val[i]     == '\t' )) { keyname[i] = '\0'; break; }

			if ( ! strcmp(keyname, key) ) { fclose(file); return val;}

			bzero(line, MAX_STR_LEN	- 1);
		}
		fclose(file);  
	}
	return none;
							    
}

int mkdir_recursive(const char *path){
	char *subpath, *fullpath; DIR* dir = NULL;
	
	if ( ( strlen(path) == 1 ) && ( path[0] == '/' ) ) return TRUE;

	fullpath = strdup(path); subpath = dirname(fullpath);
	if ( mkdir_recursive(subpath) == FALSE ) 		{ free(fullpath); return FALSE; }
	dir = opendir(path); if (dir != NULL ) { closedir(dir);	  free(fullpath); return TRUE;  }
	if ( mkdir(path, 0755 ) != 0 ) return FALSE;
	free(fullpath);
	return TRUE;
}


char *removet_filename_ext(char* mystr) {
    char *retstr;
    char *lastdot;
    if (mystr == NULL)
         return NULL;
    if ((retstr = malloc (strlen (mystr) + 1)) == NULL)
        return NULL;
    strcpy (retstr, mystr);
    lastdot = strrchr (retstr, '.');
    if (lastdot != NULL)
        *lastdot = '\0';
    return retstr;
}


int timecmpfunc(const void * a, const void * b){ return ( *(long int*)a - *(long int*)b ); }
int intcmpfunc (const void * a, const void * b){ return ( *(int*)a 	- *(int*)b 	); }


char *remove_filename_ext(char* mystr) {
	char *retstr;
	char *lastdot;
	if (mystr == NULL) 					return NULL;
	if ((retstr = malloc (strlen (mystr) + 1)) == NULL) 	return NULL;
	strcpy (retstr, mystr);
	lastdot = strrchr (retstr, '.');
	if (lastdot != NULL) *lastdot = '\0';
	return retstr;
}

char *trim_gdal_path(char *name){
	int i, j, l;	char *tmp;

	l 	= strlen(name);
	for (i = 0; i < l ; i++) if ( name[i] == '"') break; i = ( i == l ) ? 0 : i+1; 	if ( i == l ) return name;
	for (j = i; j < l ; j++) if ( name[j] == '"') break; 

       	tmp = (char *)malloc(MAX_PATH_LEN); bzero(tmp, MAX_PATH_LEN - 1);

	strncpy(tmp, (const char *)(name + i), j - i );
	

	return tmp;
}


int cleanList(block head, request_rec *r){
	block cursor; int i; struct vsblock *shp_cursor;
	for( cursor = head; cursor != NULL; cursor = cursor->next) {
		if (cursor->shp_head  != NULL ) while (( shp_cursor = cursor->shp_head ) != NULL) { cursor->shp_head = cursor->shp_head->next; if ( shp_cursor->hGeom != NULL ) OGR_G_DestroyGeometry(shp_cursor->hGeom); free(shp_cursor);  }
		if ( cursor->dataset  != NULL ) { GDALClose(cursor->dataset); cursor->dataset = NULL; }
		if ( cursor->raster   != NULL ) { for ( i = 0 ; i < cursor->nband; i++ ) if ( cursor->raster[i]  != NULL ) { free(cursor->raster[i]);	cursor->raster[i] = NULL;   } free(cursor->raster);	cursor->raster   = NULL; }
		if ( ( cursor->MetadataCount > 0 ) && ( cursor->Metadata != NULL ) )
			{ for ( i = 0 ; i < cursor->MetadataCount; 	i++ ) if ( cursor->Metadata[i] 	!= NULL ) { free(cursor->Metadata[i]); cursor->Metadata[i] = NULL; } free(cursor->Metadata); cursor->Metadata = NULL; }
		if ( ( cursor->gcps != NULL ) && ( cursor->gcps_cnt > 0 ) ) GDALDeinitGCPs(cursor->gcps_cnt, cursor->gcps);

	}
	while ((cursor = head) != NULL) { head = head->next; free(cursor); }

//	ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Memory clean ...");

	return 0;
}


int initBlock(block head){
        bzero(head->file, 	MAX_PATH_LEN - 1);
        bzero(head->math_id,	MAX_STR_LEN  - 1);
        head->sizeX		= 0;
        head->sizeY		= 0;
        head->pxSizeX		= 0;
        head->pxSizeY		= 0;
        head->srcSizeX		= 0;
        head->srcSizeY		= 0;
        head->upX		= 0;
        head->upY		= 0;
        head->offsetX		= 0;
        head->offsetY		= 0;
        head->proj		= NULL;
	head->epsg		= 0;
        head->nPixels		= 0;
        head->nLines		= 0;
        head->nband		= 0;
        head->tband		= NULL;
        head->type		= 0;
        head->typeSize		= 0;
        head->time		= 0L;
	head->high		= 0.0;
        head->wkt		= NULL;
        head->raster		= NULL;
        head->nodata		= NULL;
        head->scale		= NULL;
        head->offset		= NULL;
	head->vrt		= NULL;
	head->warp		= FALSE;
        head->psWarpOptions	= NULL;
        head->dataset		= NULL;
	head->datasetId		= NULL;
	head->Metadata		= NULL;
	head->MetadataCount	= 0;
	head->gcps		= NULL;	
	head->gcps_cnt		= 0;

	head->shp_head		= NULL;
	head->shp_cursor	= NULL;
	head->shp_num		= 0;

        head->next		= NULL;
        head->prev		= NULL;


	return 0;
}


const char *datatimeStringFromUNIX(struct tm *ts) {
	char  *buf;
	buf = (char *)malloc(80);
	// Format time, "ddd yyyy-mm-dd hh:mm:ss zzz"
	strftime(buf, 80, "%Y-%m-%dT%H:%M:%SZ", ts);
	return buf;			 
}


double degreeToRadian(double degree) { return degree * M_PI / 180.0; }

OGRErr ImportFromEPSG(	OGRSpatialReferenceH *hSRS, int nCode ){

	if ( nCode == 0 ) { hSRS = NULL; return OGRERR_NONE; }
	Unknown_EPSG_proj4 *cursor = NULL;
	for ( cursor = EPSG_proj4; cursor != NULL; cursor = cursor->next) if ( nCode == cursor->epsg ) { (*hSRS) = OSRClone(cursor->hSRS); return OGRERR_NONE; }
			
	if ( OSRImportFromEPSG( (OGRSpatialReferenceH)(*hSRS), nCode ) == OGRERR_NONE ) return OGRERR_NONE;

	return OGRERR_FAILURE;
}	

static size_t WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp) {
	size_t realsize = size * nmemb;
	struct MemoryStruct *mem = (struct MemoryStruct *)userp;

	mem->memory = realloc(mem->memory, mem->size + realsize + 1);
	if(mem->memory == NULL) {
		fprintf(stderr, "ERROR: Not enough memory (realloc returned NULL)\n"); fflush(stderr);
		return 0;
	}

	memcpy(&(mem->memory[mem->size]), contents, realsize);
	mem->size += realsize;
	mem->memory[mem->size] = 0;

	return realsize;
}


int addStatToPush( struct info *info, char *key, char *value, int type ){
	struct log_stats *cursor;
	int len;
	if ( key   == NULL ) return FALSE;
	if ( value == NULL ) return FALSE;
	if ( info->STATS_TO_PUSH != NULL ) { 
		for ( cursor = info->STATS_TO_PUSH; cursor->next != NULL; cursor = cursor->next ) { 
			if ( ! strcmp(cursor->key, key ) ) { if ( cursor->value != NULL ) free(cursor->value); 	len = strlen(value); cursor->value = malloc( len + 1 ); strcpy(cursor->value,   value); cursor->value[len] = '\0'; return TRUE; } 
		}
		cursor->next = (struct log_stats *)malloc( sizeof(struct log_stats) ); cursor = cursor->next; 
	} else { info->STATS_TO_PUSH = cursor = (struct log_stats *)malloc( sizeof(struct log_stats) ); }

	len = strlen(key); 	cursor->key   = malloc( len + 1 ); strcpy(cursor->key,   key); 	 cursor->key[len] 	= '\0';
	len = strlen(value);	cursor->value = malloc( len + 1 ); strcpy(cursor->value, value); cursor->value[len]	= '\0';
				cursor->type  = type;
	cursor->next  = NULL;

	return TRUE;
}

int unencode(char *src, char *dest){
	int code, i, len;
	len = strlen(src);
	for(i = 0;  i < len; i++,  src++ , dest++){
		if(*src == '%') {
			if(sscanf(src+1, "%2x", &code) != 1) code = '?';
			*dest = code;
			src +=2; 
		} else	*dest = *src;
		
	}
	*dest = '\0';

	return 0;
}


//------------------------------------------------------------------------------------------------


int pushStatsLogServer(struct info *info){
	int i, len;
	CURL 			*curl  		= NULL;
	char                    *token          = NULL;
	char			*tmp_post	= NULL;
	struct log_stats 	*cursor 	= NULL;
	struct log_stats 	*STATS_TO_PUSH	= info->STATS_TO_PUSH;
	request_rec 		*r		= info->r;
	char			tmp[MAX_STR_LEN];
	CURLcode 		res;
	long 			response_code;



	//---------------------------------------------------------------

	// db.accessAnalytics.insertOne({
	// datasetId:"",
	// user_id:"",
	// requestDate:"%Y-%m-%d",
	// execution_time:#,
	// access_prod:#,
	// output_format:"",
	// wcs_query:"",
	// wcs_host:"",
	// wcs_status_code:#,
	// time_limit_start:#,
	// time_limit_end :#,
	// size:#,
	// wms_query:#,
	// wms_prod:#,
	// wms_size:#,
	// geometry: json})
	// obbligatori solo datasetId ,user_id e requestDate

	// datasetId:		<string>	required;
	// user_id:		<string> 	identificativo numerico dell'utente se c'è, anonymous altrimenti,required;
	// requestDate:		<string 	in formato %Y-%m-%d> data di esecuzione della richiesta,required;
	// execution_time:	<int>		tempo di esecuzione della richiesta in secondi;
	// access_prod:		<int>		numero di prodotti a acceduti;
	// output_format:	<string> 	tipo di dato richiesto(tif,png,...);
	// wcs_query:		<string> 	query wcs/wmts effettuata;
	// wcs_host:		<string> 	host wcs/wmts;
	// wcs_status_code:	<int> 		status code wcs/wmts
	// size:		<int> 		byte scaricati;
	// wmts_query:		<int> 		numero di query wms;
	// wmts_prod:		<int> 		numero di prodotti wms acceduti;
	// wmts_size:		<int> 		byte wms scaricati;geometry: json


	//wcs_query /wcs?service=WCS&R
	//wcs_host http://192.168.10.130/wcs
	//accessed_prod 1
	//exec_time 0.431886
	//accessed_prod 1
	//exec_time 0.397417


	//---------------------------------------------------------------
	if ( LogServerURL  == NULL ) return TRUE; // if not set is not an error, it only 'couse you dont want
	if ( STATS_TO_PUSH == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "WARN: Nothing to push Log Server %s (info is empty) ...", LogServerURL ); return FALSE; }


	ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Push Info to Log Server %s ...", LogServerURL ); 

	for ( cursor = STATS_TO_PUSH, len = 0, i = 0; cursor != NULL ; cursor = cursor->next ) { 
		if ( cursor->key   == NULL ) continue;
		if ( cursor->value == NULL ) continue;
		len += ( strlen( cursor->key ) + strlen( cursor->value ) * 3  + 5 ); i += strlen( cursor->value ); 
	}

	if ( i <= 0 ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Nothing to push Log Server %s (info is zero) ...", LogServerURL ); return FALSE; }
	tmp_post = (char *)malloc(len); bzero(tmp_post, len - 1 );
	curl = curl_easy_init();
	if(curl) {
		for ( cursor = STATS_TO_PUSH; cursor != NULL ; cursor = cursor->next ) { 
			if ( cursor->key   == NULL ) continue;
			if ( cursor->value == NULL ) continue;
			char *value_escape = curl_easy_escape(curl, cursor->value, 0 );
			bzero(tmp, MAX_STR_LEN - 1); snprintf(tmp, MAX_STR_LEN, "%s=%s%c", cursor->key, value_escape, ( cursor->next != NULL ) ? '&' : '\0' ); strcat(tmp_post, tmp); 
			curl_free(value_escape); 
		}
//		fprintf(stderr, "%s\n", tmp_post); fflush(stderr);
	
		while ((cursor = STATS_TO_PUSH) != NULL) { STATS_TO_PUSH = STATS_TO_PUSH->next; free(cursor); }

		curl_easy_setopt(curl, CURLOPT_URL, 		LogServerURL);
		curl_easy_setopt(curl, CURLOPT_USERAGENT, 	"MWCS/1.0");
//		curl_easy_setopt(curl, CURLOPT_VERBOSE, 	1L);
		curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 	FALSE);
		curl_easy_setopt(curl, CURLOPT_SHARE, 		info->share);
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, 	tmp_post);

		res = curl_easy_perform(curl); fflush(stderr);
		if(res != CURLE_OK) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: pushStats curl_easy_perform() failed: %s", curl_easy_strerror(res) ); return FALSE; }
		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code); if ( response_code != 200 ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: pushStats get HTTP code %ld", response_code ); return FALSE; }
		curl_easy_cleanup(curl);
		free(tmp_post);
		return TRUE;
	} 
	free(tmp_post);
	return FALSE;

}

int GoodbyeMessage(struct info *info, const char *fmt, ... ){
	va_list args;

	if ( info == NULL ) return FALSE;
	if ( info->error_msg != NULL ) free(info->error_msg);
	info->error_msg = malloc(MAX_STR_LEN+1); bzero(info->error_msg, MAX_STR_LEN); 
	va_start(args, fmt);
	vsprintf(info->error_msg, fmt, args);
	va_end(args);
	
	return TRUE;
}


//------------------------------------------------------------------------------------------
int getTokenInfo(char *user_token, char *prod, struct info *info){
	int i, len;
	CURL 			*curl  		= NULL;
	char                    *token          = NULL;
	char			*tmp		= NULL;
	request_rec 		*r		= info->r;
	CURLcode 		res;
	char			tmp_opt[MAX_STR_LEN];
        struct 			MemoryStruct chunk;
	long 			response_code;
        chunk.memory    = malloc(1);
        chunk.size      = 0;


	// https://eodataservice.org/openid/authorization/get_resources?token=3ea63555fcf94a138c68640e0fd24ca6&host=30&resource_name=MOD11C1_LSTDAY_4326_005	

	if ( ( user_token != NULL ) && ( ENABLE_DEBUG_TOKEN == TRUE ) && ( ! strncmp( user_token, DEBUG_TOKEN, strlen(DEBUG_TOKEN) ) ) ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "WARN: Access using debug Token!" ); return TRUE; }


	ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Performing request to OAuth Server %s ...", OAuthServer ); 

	curl = curl_easy_init();
	if(curl) {
					  len  = strlen(OAuthServer) + MAX_STR_LEN;
		if ( user_token != NULL ) len += strlen(user_token);
		if ( prod 	!= NULL ) len += strlen(prod);
		tmp = malloc(len); bzero(tmp, len - 1); 	 	 

						  sprintf(tmp, 	   "%s/authorization/get_resources?", 		OAuthServer); 
		if ( user_token != NULL)	{ sprintf(tmp_opt, "token=%s&", 				user_token); 	strcat(tmp, tmp_opt); }
		if ( prod 	!= NULL)	{ sprintf(tmp_opt, "resource_name=%s&perm=read",		prod);	  	strcat(tmp, tmp_opt); }

		curl_easy_setopt(curl, CURLOPT_URL, 		tmp);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, 	WriteMemoryCallback);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, 	(void *)&chunk);
		curl_easy_setopt(curl, CURLOPT_USERAGENT, 	"MWCS/1.0");
//		curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
		curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 	FALSE);
		curl_easy_setopt(curl, CURLOPT_SHARE, 		info->share);


		res = curl_easy_perform(curl); fflush(stderr);
		if(res != CURLE_OK) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: getTokenInfo curl_easy_perform() failed: %s", curl_easy_strerror(res) ); return FALSE; }
		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code); if ( response_code != 200 ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: getTokenInfo get HTTP code %ld", response_code ); return FALSE; }

		char 		*json  		= NULL;
		size_t          json_len        = 0;
		ssize_t         json_n          = 0;
		json_object     *jobj           = NULL;
		json_object     *jobj_resources = NULL;
		json_object     *jobj_userinfo  = NULL;
		json_object     *jobj_client	= NULL;

		int		readble		= FALSE;
		char		resource_name[MAX_STR_LEN];
		char		host[MAX_STR_LEN];
		char		id[MAX_STR_LEN]; bzero(id, MAX_STR_LEN - 1 );


		if ( ExtHostname == NULL ){ ExtHostname = (char *)malloc(MAX_STR_LEN); bzero(ExtHostname, MAX_STR_LEN - 1 ); sprintf(ExtHostname, "http://%s/%s", r->hostname, "wcs" ); }
		if ( OAuthId != NULL ) 	strcpy(id, OAuthId);
		else			strcpy(id, ExtHostname);

	        for( token = strtok(chunk.memory, "\n"); token != NULL;  token = strtok(NULL, "\n") ) {
			json_n = strlen(token);
			json   = realloc(json, json_len + json_n + 1);
			memcpy(json + json_len, token, json_n);
			json_len += json_n;
			json[json_len] = '\0';
		}


        	if(chunk.memory) free(chunk.memory);
		curl_easy_cleanup(curl);

		jobj = json_tokener_parse(json);
			
		if ( jobj != NULL) { json_object_object_foreach(jobj, key, val){ if ( ( json_object_is_type(val,  json_type_object)) && ( ! strcmp( key, "userinfo" )) ){ json_object_object_get_ex(jobj, key, &jobj_userinfo); break; } } }
		if ( jobj_userinfo != NULL ) {	
			json_object_object_foreach( jobj_userinfo, key, val){
				if        ( ! strcmp(key, "uid"     ) ) addStatToPush(info, "user_id",   ( json_object_is_type(val,  json_type_null ) ) ? "null" : (char *)json_object_get_string(val), GFT_String ); // TO REMOVE
				else if ( ( ! strcmp(key, "client"  ) ) && ( json_object_is_type(val,  json_type_object ) ) ) { 
					json_object_object_get_ex(jobj_userinfo, key, &jobj_client);
					json_object_object_foreach(jobj_client, key, val){
						if      ( ! strcmp(key, "client_id"  ) ) { addStatToPush(info, "client_id",  ( json_object_is_type(val,  json_type_null ) ) ? "null" : (char *)json_object_get_string(val), GFT_String );  }
						else if ( ! strcmp(key, "client_url" ) ) { addStatToPush(info, "client_url", ( json_object_is_type(val,  json_type_null ) ) ? "null" : (char *)json_object_get_string(val), GFT_String ); }
					}
				}
			}
		}

		if ( jobj != NULL) { json_object_object_foreach(jobj, key, val){ if ( ( json_object_is_type(val, json_type_array)) && ( ! strcmp( key, "resources" )) ){ json_object_object_get_ex(jobj, key, &jobj_resources); break; } } }
		if ( jobj_resources != NULL ) {
			for( i = 0; i < json_object_array_length(jobj_resources); i++){
				readble = FALSE; bzero(resource_name, MAX_STR_LEN - 1); bzero(host, MAX_STR_LEN - 1);
				json_object_object_foreach( json_object_array_get_idx(jobj_resources, i) , key, val){
					// fprintf(stderr, "%s %s\n", key, json_object_get_string(val)); fflush(stderr);
					if      ( ! strcmp(key, "resource_name"  	) ) strcpy( resource_name, json_object_get_string(val)); 
					else if ( ! strcmp(key, "host"  		) ) strcpy( host, 	   json_object_get_string(val)); // strcpy( host,  ExtHostname );
				}

				if ( 	( ! strcmp( host, 		id )		) &&			// Check the policy for my host
					( ! strcmp( resource_name, 	prod )		) ) return TRUE; 	// Check the prod

			}
		}

			
        
		return FALSE;
	} 
	return FALSE;

}


//------------------------------------------------------------------------------------------
// Function for upload file ... to define what fuck I can do
/*
static int util_read(request_rec *r, const char **rbuf){
	int rc;
     	if ((rc = ap_setup_client_block(r, REQUEST_CHUNKED_ERROR)) != OK) return rc; 
	if (ap_should_client_block(r)) {
		char argsbuffer[HUGE_STRING_LEN];
	       	int rsize, len_read, rpos = 0;
	       	long length = r->remaining;
	       	*rbuf = apr_palloc(r->pool, length + 1);

	       	while ((len_read = ap_get_client_block(r, argsbuffer, sizeof(argsbuffer))) > 0) { 
	           	if ((rpos + len_read) > length) rsize = length - rpos;
	                else 				rsize = len_read;	  		
		    	memcpy((char*)*rbuf + rpos, argsbuffer, rsize);
		        rpos += rsize;
		}
	}
	return rc;
}*/


//------------------------------------------------------------------------------------------

int UpdateMemoryUsage(request_rec *r){
	if ( L_MaxMemoryUse <= 0 ) return 0;
	char line[1024];
	int  port = r->connection->client_addr->port;

	//---------------------------------------------
	bzero(line, 1023); sprintf(line, "%s_%d", SHMOBJ_PATH, port );

        // Create a semaphore and waiting
        sem_t  *sem_id = sem_open(SEM_PATH, O_CREAT, S_IRUSR | S_IWUSR, 1);
        sem_wait(sem_id);

	int i;
	unsigned long wcs_memory_info 	= 0;
	unsigned long memory_needed	= 0;

	// Open common memory usage
	int mem_info = shm_open(SHMOBJ_PATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR); i = ftruncate(mem_info, sizeof(unsigned long) ); memcpy( &wcs_memory_info,  mmap(NULL, sizeof(unsigned long), PROT_READ|PROT_WRITE, MAP_SHARED, mem_info, 0), sizeof(unsigned long) );
	if ( i != 0 ) { fprintf(stderr, "ERROR: ftruncate in UpdateMemoryUsage!\n"); fflush(stderr); }
	// Open local memory usage read, close and delete
	int mem_proc = shm_open(line,        O_CREAT | O_RDWR, S_IRUSR | S_IWUSR); i = ftruncate(mem_proc, sizeof(unsigned long) ); memcpy( &memory_needed,    mmap(NULL, sizeof(unsigned long), PROT_READ|PROT_WRITE, MAP_SHARED, mem_proc, 0), sizeof(unsigned long) );
	if ( i != 0 ) { fprintf(stderr, "ERROR: ftruncate in UpdateMemoryUsage!\n"); fflush(stderr); }
	close(mem_proc); shm_unlink(line);


	// Adding the requested memory
	wcs_memory_info = wcs_memory_info + memory_needed; if ( wcs_memory_info > L_MaxMemoryUse ) wcs_memory_info = L_MaxMemoryUse; // I've added this check in caso something goes wrong
	// Update total memory free
	memcpy( mmap(NULL, sizeof(unsigned long), PROT_READ|PROT_WRITE, MAP_SHARED, mem_info, 0), &wcs_memory_info, sizeof(unsigned long) ); close(mem_info);
	ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Released Memory %.2f MByte, Free Memory available: %.2f MByte" , (double)memory_needed / 1024.0 / 1024.0, (double)wcs_memory_info / 1024.0 / 1024.0 );

	// Release semaphore
	sem_post(sem_id);
	sem_close(sem_id);
	sem_unlink(SEM_PATH);

	if ( ( L_MaxTasks > 0 ) && ( wcs_memory_info == L_MaxMemoryUse ) ) { // Ok, this thing should not happen ... but ...
		sem_t 	*sem_id;
		int	task_info;
		int	wcs_task_info = L_MaxTasks;

		// Create a semaphore and waiting
		sem_id = sem_open(SEM_PATH, O_CREAT, S_IRUSR | S_IWUSR, 1); sem_wait(sem_id);

		// Open common tasks number
		task_info = shm_open(SHTOBJ_PATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
		if ( ftruncate(task_info, sizeof(int) ) != 0 ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: ftruncate for tasks!" ); sem_post(sem_id); sem_close(sem_id); sem_unlink(SEM_PATH); return FALSE; }

		memcpy( mmap(NULL, sizeof(int), PROT_READ|PROT_WRITE, MAP_SHARED, task_info, 0), &wcs_task_info, sizeof(int) );
		close(task_info);

		// Release lock on semaphore    
		sem_post(sem_id);
		sem_close(sem_id);
		sem_unlink(SEM_PATH);

	}


	return 0;
}

//------------------------------------------------------------------------------------------

int RequestTask(request_rec *r){
	sem_t 	*sem_id;
	int	task_info;
	int	wcs_task_info;
	int	pass = FALSE;

	// Create a semaphore and waiting
	sem_id = sem_open(SEM_PATH, O_CREAT, S_IRUSR | S_IWUSR, 1); sem_wait(sem_id);

	// Open common tasks number
	task_info = shm_open(SHTOBJ_PATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
	if ( ftruncate(task_info, sizeof(int) ) != 0 ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: ftruncate for tasks!" ); sem_post(sem_id); sem_close(sem_id); sem_unlink(SEM_PATH); return FALSE; }
	memcpy( &wcs_task_info,  mmap(NULL, sizeof(int), PROT_READ|PROT_WRITE, MAP_SHARED, task_info, 0), sizeof(int) ); 

	if ( wcs_task_info <= 0 ) wcs_task_info = ( L_MaxTasks + 1 ); // Init task number

	if 	( wcs_task_info <= 1 ) 	{ wcs_task_info = 1;  	pass = FALSE; }	// No task free
	else 				{ wcs_task_info--;	pass = TRUE;  }	// Task free

	memcpy( mmap(NULL, sizeof(int), PROT_READ|PROT_WRITE, MAP_SHARED, task_info, 0), &wcs_task_info, sizeof(int) );

	close(task_info);

	// Release lock on semaphore    
	sem_post(sem_id);
	sem_close(sem_id);

	sem_unlink(SEM_PATH);

	if ( pass == TRUE ) 	{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Task can be run! (%d/%d)", L_MaxTasks - wcs_task_info + 1, L_MaxTasks );  return TRUE;  }
	else			{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: No Task free! (%d/%d)",   L_MaxTasks - wcs_task_info + 1, L_MaxTasks );  return FALSE; }	

}

int ReleaseTask(request_rec *r){
	sem_t 	*sem_id;
	int	task_info;
	int	wcs_task_info;

	// Create a semaphore and waiting
	sem_id = sem_open(SEM_PATH, O_CREAT, S_IRUSR | S_IWUSR, 1); sem_wait(sem_id);

	// Open common tasks number
	task_info = shm_open(SHTOBJ_PATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
	if ( ftruncate(task_info, sizeof(int) ) != 0 ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: ftruncate for tasks!" ); sem_post(sem_id); sem_close(sem_id); sem_unlink(SEM_PATH); return FALSE; }
	memcpy( &wcs_task_info,  mmap(NULL, sizeof(int), PROT_READ|PROT_WRITE, MAP_SHARED, task_info, 0), sizeof(int) ); 

	wcs_task_info++;
	if ( wcs_task_info > ( L_MaxTasks + 1 ) ) wcs_task_info = ( L_MaxTasks + 1 ); // Fix possibile error

	memcpy( mmap(NULL, sizeof(int), PROT_READ|PROT_WRITE, MAP_SHARED, task_info, 0), &wcs_task_info, sizeof(int) );
	close(task_info);

	// Release lock on semaphore    
	sem_post(sem_id);
	sem_close(sem_id);
	sem_unlink(SEM_PATH);

	ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Release Task ... (%d/%d)", L_MaxTasks - wcs_task_info + 1, L_MaxTasks );

	if ( ( L_MaxMemoryUse > 0 ) && ( ( L_MaxTasks - wcs_task_info + 1 ) == 0 ) ) { // Ok, this thing should not happen ... but ...
		int i;
		unsigned long wcs_memory_info = L_MaxMemoryUse;

	        // Create a semaphore and waiting
	        sem_t  *sem_id = sem_open(SEM_PATH, O_CREAT, S_IRUSR | S_IWUSR, 1);
	        sem_wait(sem_id);

		// Open common memory usage
		int mem_info = shm_open(SHMOBJ_PATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR); i = ftruncate(mem_info, sizeof(unsigned long) ); 
		if ( i != 0 ) { fprintf(stderr, "ERROR: ftruncate in ReleaseTask!\n"); fflush(stderr); }
		// Update total memory free
		memcpy( mmap(NULL, sizeof(unsigned long), PROT_READ|PROT_WRITE, MAP_SHARED, mem_info, 0), &wcs_memory_info, sizeof(unsigned long) ); close(mem_info);

		// Release semaphore
		sem_post(sem_id);
		sem_close(sem_id);
		sem_unlink(SEM_PATH);

	}
	return TRUE;  

}


//------------------------------------------------------------------------------------------

void debug_signal_handler( int signo ){ fprintf(stderr, "FU*K, signal %d for %ld is a Segmentation fault !!! ... Time to die.\n", signo, (long)getpid() ); fflush(stderr); exit (0); }


//------------------------------------------------------------------------------------------

static int MWCS_handler(request_rec *r){ // INIT
	if (!r->handler || strcmp(r->handler, "mwcs")) 		return DECLINED;
	if (  ROOT == NULL ) 					return DECLINED;

	pthread_t 	thread_id;
	struct info 	*info 	= malloc(sizeof(struct info));	
	void 		*status = 0;
	info->query_string	= NULL;
	info->r			= r; 
	info->killable		= TRUE;
	info->version		= NULL;
	info->uri		= NULL;
	info->STATS_TO_PUSH	= NULL;
	info->content_type 	= NULL;
	info->func		= getData;
	info->token		= NULL;
	info->module		= NULL;
	info->coverage		= NULL;
	info->error_msg		= NULL;
	info->tile_path		= NULL;
	info->passthrough	= FALSE;
	info->nfs_mount_points	= NULL;
	info->pairs		= NULL;
	info->cache		= TRUE;

	info->ENVIRONMENT_VARIABLE[0] = NULL; info->ENVIRONMENT_VARIABLE[1] = NULL;

	int i;
	FILE 		*fp = NULL;
	char 		path[3][MAX_PATH_LEN];
	int 		sl;
	long int	local_address;	int local_port; 	
	long int	rem_address;	int rem_port; 
	int		st;
	long int	tx_queue,	rx_queue;
	int		tr;		
	long int	tm_when;
	long int	retrnsmt;
	int		uid;
	int		timeout;
	int		inode;
	char		line[1024];
	int		ec;
	char		log_tmp_str[MAX_STR_LEN];

	char 		*query		= NULL;
	char 		*tok		= NULL;
	char 		*pszKey		= NULL;
	const char 	*pszValue	= NULL;
	char 		*p		= NULL;
	char		*token		= NULL;
	char		*coverageid	= NULL;
	int		Request		= -1;

	unsigned long mem_size, mem_resident, mem_share, mem_text, mem_lib, mem_data, mem_dt;
	struct sysinfo myinfo; sysinfo(&myinfo);
	unsigned long pagesize = sysconf(_SC_PAGESIZE);


//	if(signal(SIGSEGV, debug_signal_handler) == SIG_ERR) ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "WARN: can't catch SIGSEGV"); 


	init_curl_locks(&info->connlock);
	curl_global_init(CURL_GLOBAL_ALL);
	info->share = curl_share_init();
	curl_share_setopt(info->share, CURLSHOPT_LOCKFUNC, 	lock_cb);
	curl_share_setopt(info->share, CURLSHOPT_UNLOCKFUNC, 	unlock_cb);
	curl_share_setopt(info->share, CURLSHOPT_USERDATA,	info);

	curl_share_setopt(info->share, CURLSHOPT_UNSHARE, 	CURL_LOCK_DATA_COOKIE);		
	curl_share_setopt(info->share, CURLSHOPT_UNSHARE, 	CURL_LOCK_DATA_DNS);		
	curl_share_setopt(info->share, CURLSHOPT_UNSHARE, 	CURL_LOCK_DATA_SSL_SESSION);		
	curl_share_setopt(info->share, CURLSHOPT_UNSHARE, 	CURL_LOCK_DATA_CONNECT);		
	curl_share_setopt(info->share, CURLSHOPT_UNSHARE, 	CURL_LOCK_DATA_LAST);		

	// Craete a table of nfs mount points
	if ( r->uri  != NULL ) { info->uri 	    = malloc( strlen(r->uri)  + 1 ); bzero(info->uri, 		strlen(r->uri) ); strcpy(info->uri, 		r->uri);  }
	if ( r->args != NULL ) { info->query_string = malloc( strlen(r->args) + 1 ); bzero(info->query_string,	strlen(r->args)); strcpy(info->query_string, 	r->args); }


	bzero(log_tmp_str, MAX_STR_LEN - 1); 
	if ( ( info->query_string != NULL ) && ( strlen(info->query_string) > 0 ) ) 	snprintf(log_tmp_str, MAX_STR_LEN, "%s?%s", info->uri, info->query_string );
	else									 	snprintf(log_tmp_str, MAX_STR_LEN, "%s",    info->uri ); 
	addStatToPush(info, "wcs_query", 	log_tmp_str, 	GFT_String);  // STATS
	addStatToPush(info, "size",  	 	"0", 		GDT_Int32 );  // STATS Init for size
	addStatToPush(info, "accessed_prod",  	"0", 		GDT_Int32 );  // STATS Init for size
	addStatToPush(info, "datasetId",  	"nodatasetid",	GFT_String ); // STATS Init for size
	addStatToPush(info, "measure",  	"byte",		GFT_String ); // STATS Init for size
	addStatToPush(info, "user_id",  	"anonymous",	GFT_String ); // STATS Init for size
	addStatToPush(info, "client_id",  	"anonymous",	GFT_String ); // STATS Init for size
	addStatToPush(info, "token", 	 	"anonymous",	GFT_String ); // STATS Init for size
	addStatToPush(info, "action", 	 	"wcs",		GFT_String ); // STATS Init for size
	addStatToPush(info, "moduleId",  	"mwcs", 	GFT_String );  
        char buff[100]; struct timeval now; gettimeofday(&now, NULL); strftime(log_tmp_str, MAX_STR_LEN, "%Y-%m-%dT%H:%M:%S", gmtime(&now.tv_sec) ); bzero(buff, 99); snprintf(buff, 100, ".%ldZ", now.tv_usec); strcat(log_tmp_str, buff);
	addStatToPush(info, "requestDate",  log_tmp_str, GFT_String ); // STATS


	// WMTS interpreter ... yes i know ... it's cool
	if ( ( info->uri != NULL ) && ( ! strncmp(info->uri, "/wmts", 5 ) ) ) if ( WMTSUrlParser(info) == FALSE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Failed WMTS query ..."); return info->exit_code; }
	 
	// Enter in the new world of API
	if ( ( info->uri != NULL ) && ( ! strncmp(info->uri, "/api",  4 ) ) ) if ( APIUrlParser(info)  == FALSE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Failed API query ...");  return info->exit_code; }
	
	if ( OAuthServer != NULL ) { // Evrything must be set
		query = (char *)malloc( strlen(info->query_string) + 1); bzero(query, strlen(info->query_string)); strcpy(query, info->query_string);
	        tok = strtok( query, DELIMS );
	        while( tok != NULL ) {
	                pszValue = (const char *)CPLParseNameValue( tok, &pszKey );
	                if ( pszValue == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to parsing '%s'", tok); return FALSE; }

	                for(p = pszKey;*p;++p) *p=*p>0x40&&*p<0x5b?*p|0x60:*p;
			if ( ! strcmp(pszKey, "token"  		) ) { token 	 = malloc(strlen(pszValue)+1); strcpy( token, 	   pszValue ); }
			if ( ! strcmp(pszKey, "coverageid"  	) ) { coverageid = malloc(strlen(pszValue)+1); strcpy( coverageid, pszValue ); }
			if ( ! strcmp(pszKey, "request"  	) ) {
				if 	( ! strcmp(pszValue, "GetCoverage"	) ) 	Request = GetCoverage;
				else if ( ! strcmp(pszValue, "GetCapabilities"	) )	Request = GetCapabilities;
				else if ( ! strcmp(pszValue, "DescribeCoverage"	) )	Request = DescribeCoverage;
				else if ( ! strcmp(pszValue, "GetList"		) )	Request = GetList;
				else if ( ! strcmp(pszValue, "GetInfo"		) )	Request = GetInfo;
				else if ( ! strcmp(pszValue, "GetFile"		) )	Request = GetFile;
				else if ( ! strcmp(pszValue, "Status"		) )	Request = Status;
				else { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unsupported request %s", pszValue); return HTTP_BAD_REQUEST; }
			}

			tok = strtok( NULL, DELIMS );
		}
		if ( info->token != NULL ) { token = malloc(strlen(info->token)+1); strcpy( token, info->token ); }

		if ( token != NULL ) addStatToPush(info, "token", token, GFT_String ); else addStatToPush(info, "token", "none", GFT_String );


		if ( ( Request == GetCoverage ) || ( Request == DescribeCoverage ) || ( Request == GetList ) || ( Request == GetInfo ) || ( Request == GetFile ) || ( Request == Status ) ) {
			/*
			if ( token == NULL ){
				ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Client didn't provide a token ..." );
				ap_set_content_type(r, "text/html");
				return HTTP_FORBIDDEN;
			}
			*/
			if ( coverageid == NULL ){
				ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Client didn't provide a CoverageId ..." );
				ap_set_content_type(r, "text/html");
				return HTTP_FORBIDDEN;
			}

			if ( getTokenInfo(token, coverageid, info ) == FALSE ) {
				ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Access denied for this client ..." );
				ap_set_content_type(r, "text/html");
				return HTTP_FORBIDDEN;
			}

			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Access granted for this client ..." );

		}
	} 
	
	bzero(path[0], MAX_PATH_LEN - 1); sprintf(path[0], "/proc/%d/net/tcp",  (int)getpid() );
	bzero(path[1], MAX_PATH_LEN - 1); sprintf(path[1], "/proc/%d/net/tcp6", (int)getpid() );
	bzero(path[2], MAX_PATH_LEN - 1); sprintf(path[2], "/proc/%d/statm",    (int)getpid() );


	//---------------------------------------------------------------------------------



	// This part is put here 'couse maybe is the fucking right to do this
	if ( ( info->tile_path != NULL ) && ( info->cache == TRUE ) ) {
		GByte *realRaster = NULL;
		if ( MEMC_memc != NULL ) {
			size_t value_length; uint32_t flags; memcached_return rc;
			realRaster = (GByte *)memcached_get(MEMC_memc, info->tile_path, strlen(info->tile_path), &value_length, &flags, &rc);
			if (rc == MEMCACHED_SUCCESS) {
				ap_set_content_type(r, "image/png");
				i = ap_rwrite( (const void *)realRaster, value_length, r );
				free(realRaster);
				ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: [WMTS] Used Memcached tile %s ...", info->tile_path );
				return 0;
			}
		} else if ( WMTSCachePath != NULL ) {
			struct stat tile_info; 
			if ( (stat(info->tile_path, &tile_info) == 0) && ( ( time(NULL) - tile_info.st_ctime ) < WMTSMaxTileAge ) ) {
				if ( tile_info.st_size == 0 ) { GoodbyeMessage(info, "No images found"); ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: [WMTS] Empty cache file %s return tile not found ...", info->tile_path); return 404; }

				realRaster = malloc(tile_info.st_size);
				if ( realRaster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc realRaster "); return 500; }
				FILE *fp = fopen(info->tile_path, "rb");
				if (fp != NULL) {
					size_t blocks_read = fread(realRaster, tile_info.st_size, 1, fp);
					if (blocks_read == 1) {
						ap_set_content_type(r, "image/png");
						i = ap_rwrite( (const void *)realRaster, tile_info.st_size, r );
						fclose(fp); free(realRaster);
						ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: [WMTS] Used cache file %s ...", info->tile_path );
						return 0;
					}
				}
			}
		}
	}


	//---------------------------------------------------------------------------------
		
	if ( ( L_MaxTasks > 0 ) && ( RequestTask(r) == FALSE ) ) { GoodbyeMessage(info,"Server busy");  return HTTP_TOO_MANY_REQUESTS; }

	//---------------------------------------------------------------------------------

	if (pthread_create(&thread_id, NULL, thread_worker, info) != 0 ) {
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: No threads for you.");
		return DECLINED;
	}
	
	struct timespec ts; 
	long int born_time = time(NULL);	
	int 	OMG_EXIT_NOW = FALSE;
	while( pthread_kill( thread_id, 0 ) == 0 ){
		for (i = 0; i < 2 ; i++){
			if ( ( fp = fopen (path[i], "r") ) == NULL ) continue;
			while ( fgets(line, 1024, fp) != NULL){

				sscanf(line, " %d: %lX:%04X %lX:%04X %02X %08lX:%08lX %02X:%08lX %08lX %d %d %d\n",	&sl, 
															&local_address, &local_port,
															&rem_address, 	&rem_port,
															&st, 
															&tx_queue,	&rx_queue,
															&tr,		&tm_when,
															&retrnsmt,	&uid,
															&timeout,	&inode );


				if ( r->connection->client_addr->port == rem_port ) {
					if ( st != 0x01 ) { 
						if ( info->killable == TRUE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Connection close from client ... STOP!"); 	
							if ( ( L_MaxTasks > 0 ) && ( ReleaseTask(r) == FALSE ) ) return HTTP_INTERNAL_SERVER_ERROR;
							UpdateMemoryUsage(r); pthread_kill(thread_id, SIGKILL);  return DECLINED; }
						else {
							ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Connection close from client, but I can't stop, waiting ...");	while ( info->killable == FALSE ) sleep(POLLING_CLIENT);
							ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Connection close from client, now I can kill it ... STOP!");
							if ( ( L_MaxTasks > 0 ) && ( ReleaseTask(r) == FALSE ) ) return HTTP_INTERNAL_SERVER_ERROR;
							UpdateMemoryUsage(r); pthread_kill(thread_id, SIGKILL);  return DECLINED;
						}
					}

				}
			}
			fclose(fp);
		}

		if ( ( fp = fopen (path[i], "r") ) != NULL ){
			if ( fscanf(fp,"%ld %ld %ld %ld %ld %ld %ld", &mem_size, &mem_resident, &mem_share, &mem_text, &mem_lib, &mem_data, &mem_dt) == 7 ) {
				if ( ( info->killable == TRUE ) && ( ( (double)( mem_size * pagesize ) / (double)( myinfo.mem_unit *  myinfo.totalram ) * 100 ) > MAX_USABLE_MEMORY ) ) {
					ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Auto Protection System, this process must STOP! ( > MAX_USABLE_MEMORY, Time to die )");
					if ( ( L_MaxTasks > 0 ) && ( ReleaseTask(r) == FALSE ) ) return HTTP_INTERNAL_SERVER_ERROR;
					UpdateMemoryUsage(r); pthread_kill(thread_id, SIGKILL); status = malloc(sizeof(int)); *(int *)status = HTTP_REQUEST_ENTITY_TOO_LARGE; OMG_EXIT_NOW = TRUE; 
				
				}
			}
			fclose(fp);
		} 

		if ( ( ts.tv_sec - born_time ) > MAX_TIME_QUERY ) { 
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Auto Protection System, this process must STOP! ( > MAX_TIME_QUERY, Time to die ) ");
			if ( ( L_MaxTasks > 0 ) && ( ReleaseTask(r) == FALSE ) ) return HTTP_INTERNAL_SERVER_ERROR;
			UpdateMemoryUsage(r); pthread_kill(thread_id, SIGKILL); status = malloc(sizeof(int)); *(int *)status = HTTP_REQUEST_ENTITY_TOO_LARGE; OMG_EXIT_NOW = TRUE; 
		}

		if ( OMG_EXIT_NOW == TRUE ) break;

		if (clock_gettime(CLOCK_REALTIME, &ts) == -1) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: clock_gettime for thread"); return 500; }
		ts.tv_sec += POLLING_CLIENT; if ( pthread_timedjoin_np(thread_id, (void **) &(status), &ts) == 0 ) break;
	}

	if ( OMG_EXIT_NOW != TRUE ) pthread_join(thread_id, (void **) &(status)); 	


	// RESTORED Content-type ... mother of all gods and devils ... WTF?!?!? Where is it?!?!? 
	if ( info->content_type != NULL ) ap_set_content_type(r, info->content_type); 

	// wcs_status_code	
	bzero(log_tmp_str, MAX_STR_LEN - 1); sprintf(log_tmp_str, "%d", ( *(int *)status == 0 ) ? 200 : *(int *)status ); addStatToPush(info, "wcs_status_code",  log_tmp_str, GDT_Int32 ); // STATS
	
	UpdateMemoryUsage(r);	


	//---------------------------------------------------------------------------------
	if ( ( L_MaxTasks > 0 ) && ( ReleaseTask(r) == FALSE ) ) return HTTP_INTERNAL_SERVER_ERROR;	
	//---------------------------------------------------------------------------------

	if ( pushStatsLogServer(info) == FALSE ) ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Problem during push on Log Server!");  ; // STATS


	kill_curl_locks(&info->connlock);
	curl_share_cleanup(info->share);
       	curl_global_cleanup();

	//---------------------------------------------------------------------------------
	if ( ( *(int *)status == 404   ) && ( info->uri != NULL ) && ( WMTSCachePath != NULL  ) && ( info->tile_path != NULL ) && ( info->cache == TRUE ) ) { 

                struct stat tile_info;
                if (stat(info->tile_path, &tile_info) != 0) {
			char imgs_path[MAX_PATH_LEN]; bzero(imgs_path,  MAX_PATH_LEN - 1); strcpy(imgs_path, info->tile_path ); 
			if ( mkdir_recursive(dirname(imgs_path)) == FALSE ) 				{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] Unable to create cache directory for %s !!", 	info->tile_path ); return 500; }
			FILE *tile_cache = fopen(info->tile_path, "wb");
			if ( tile_cache == NULL  ) 			    				{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] Unable to create empty cache file %s !!", 		info->tile_path ); return 500; }
			if ( fclose(tile_cache)	!= 0 )							{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] Unable to close empry cache file %s !!",           info->tile_path ); return 500; }
		}
	
	}

	//---------------------------------------------------------------------------------

	if ( ( *(int *)status != 0   ) && ( *(int *)status != 200   ) && ( info->error_msg != NULL ) ) {
	        char string[MAX_STR_LEN]; bzero(string, MAX_STR_LEN); char *target = string;

	        target += sprintf(target, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		target += sprintf(target, "<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">\n");
		target += sprintf(target, "<html><head>\n");
		target += sprintf(target, "<title>%d ERROR</title>\n", *(int *)status );
		target += sprintf(target, "</head><body>\n");
		target += sprintf(target, "<h1>%s</h1>\n", info->error_msg );
		target += sprintf(target, "<pre>\n");
		target += sprintf(target, "               ,,,,		\n");
		target += sprintf(target, "              /   '		\n");
		target += sprintf(target, "             /.. /		\n");
		target += sprintf(target, "            ( c  D		\n");
		target += sprintf(target, "             \\- '\\_	\n");
		target += sprintf(target, "              `-'\\)\\	\n");
		target += sprintf(target, "                 |_ \\	\n");
		target += sprintf(target, "                 |U \\\\	\n");
		target += sprintf(target, "                (__,//	\n");
		target += sprintf(target, "                |. \\/	\n");
		target += sprintf(target, "                LL__I	\n");
		target += sprintf(target, "                 |||		\n");
		target += sprintf(target, "                 |||		\n");
		target += sprintf(target, "              ,,-``'\\  	\n");
		target += sprintf(target, "</pre>\n");
		target += sprintf(target, "<hr>\n");
		#ifdef MWCS_VERSION
		target += sprintf(target, "<address>MWCS/%s Server by <a href=\"https://www.meeo.it\">MEEO</a></address>\n", MWCS_VERSION);
		#else
		target += sprintf(target, "<address>MWCS Server by <a href=\"https://www.meeo.it\">MEEO</a></address>\n");
		#endif
		target += sprintf(target, "</body></html>\n");	
		ap_custom_response( r, *(int *)status, string);
	}


	if (  *(int *)status == 500 ) {
	        char string[MAX_STR_LEN]; bzero(string, MAX_STR_LEN); char *target = string;
	        target += sprintf(target, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		target += sprintf(target, "<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">\n");
		target += sprintf(target, "<html><head>\n");
		target += sprintf(target, "<title>Internal Server Error 500</title>\n");
		target += sprintf(target, "</head><body>\n");
		target += sprintf(target, "<h1>Oh No, Internal Server Error !</h1>\n" );
		target += sprintf(target, "<pre>\n");
		target += sprintf(target, "         _______           \n");
		target += sprintf(target, "        |.-----.|          \n");
		target += sprintf(target, "        ||x . x||          \n");
		target += sprintf(target, "        ||_.-._||          \n");
		target += sprintf(target, "        `--)-(--`          \n");
		target += sprintf(target, "       __[=== o]___        \n");
		target += sprintf(target, "      |:::::::::::|\\       \n");
		target += sprintf(target, "      `-=========-`()      \n");
		target += sprintf(target, "</pre>\n");
		target += sprintf(target, "A team of highly trained monkeys has been dispatched to deal with this situation.\n" );
		target += sprintf(target, "<hr>\n");
		#ifdef MWCS_VERSION
		target += sprintf(target, "<address>MWCS/%s Server by <a href=\"https://www.meeo.it\">MEEO</a></address>\n", MWCS_VERSION);
		#else
		target += sprintf(target, "<address>MWCS Server by <a href=\"https://www.meeo.it\">MEEO</a></address>\n");
		#endif
		target += sprintf(target, "</body></html>\n");	
		ap_custom_response( r, *(int *)status, string);
	}


	//---------------------------------------------------------------------------------

	if 	( *(int *)status == 0 	) return OK;
	else if ( *(int *)status == 100 ) return HTTP_CONTINUE;
	else if ( *(int *)status == 101 ) return HTTP_SWITCHING_PROTOCOLS;
	else if ( *(int *)status == 102 ) return HTTP_PROCESSING;
	else if ( *(int *)status == 103 ) return RESPONSE_CODES;
	else if ( *(int *)status == 200 ) return HTTP_OK;
	else if ( *(int *)status == 201 ) return HTTP_CREATED;
	else if ( *(int *)status == 202 ) return HTTP_ACCEPTED;
	else if ( *(int *)status == 203 ) return HTTP_NON_AUTHORITATIVE;
	else if ( *(int *)status == 204 ) return HTTP_NO_CONTENT;
	else if ( *(int *)status == 205 ) return HTTP_RESET_CONTENT;
	else if ( *(int *)status == 206 ) return HTTP_PARTIAL_CONTENT;
	else if ( *(int *)status == 207 ) return HTTP_MULTI_STATUS;
	else if ( *(int *)status == 208 ) return HTTP_ALREADY_REPORTED;
	else if ( *(int *)status == 226 ) return HTTP_IM_USED;
	else if ( *(int *)status == 300 ) return HTTP_MULTIPLE_CHOICES;
	else if ( *(int *)status == 301 ) return HTTP_MOVED_PERMANENTLY;
	else if ( *(int *)status == 302 ) return HTTP_MOVED_TEMPORARILY;
	else if ( *(int *)status == 303 ) return HTTP_SEE_OTHER;
	else if ( *(int *)status == 304 ) return HTTP_NOT_MODIFIED;
	else if ( *(int *)status == 305 ) return HTTP_USE_PROXY;
	else if ( *(int *)status == 307 ) return HTTP_TEMPORARY_REDIRECT;
	else if ( *(int *)status == 308 ) return HTTP_PERMANENT_REDIRECT;
	else if ( *(int *)status == 400 ) return HTTP_BAD_REQUEST;
	else if ( *(int *)status == 401 ) return HTTP_UNAUTHORIZED;
	else if ( *(int *)status == 402 ) return HTTP_PAYMENT_REQUIRED;
	else if ( *(int *)status == 403 ) return HTTP_FORBIDDEN;
	else if ( *(int *)status == 404 ) return HTTP_NOT_FOUND;
	else if ( *(int *)status == 405 ) return HTTP_METHOD_NOT_ALLOWED;
	else if ( *(int *)status == 406 ) return HTTP_NOT_ACCEPTABLE;
	else if ( *(int *)status == 407 ) return HTTP_PROXY_AUTHENTICATION_REQUIRED;
	else if ( *(int *)status == 408 ) return HTTP_REQUEST_TIME_OUT;
	else if ( *(int *)status == 409 ) return HTTP_CONFLICT;
	else if ( *(int *)status == 410 ) return HTTP_GONE;
	else if ( *(int *)status == 411 ) return HTTP_LENGTH_REQUIRED;
	else if ( *(int *)status == 412 ) return HTTP_PRECONDITION_FAILED;
	else if ( *(int *)status == 413 ) return HTTP_REQUEST_ENTITY_TOO_LARGE;
	else if ( *(int *)status == 414 ) return HTTP_REQUEST_URI_TOO_LARGE;
	else if ( *(int *)status == 415 ) return HTTP_UNSUPPORTED_MEDIA_TYPE;
	else if ( *(int *)status == 416 ) return HTTP_RANGE_NOT_SATISFIABLE;
	else if ( *(int *)status == 417 ) return HTTP_EXPECTATION_FAILED;
//	else if ( *(int *)status == 418 ) return HTTP_IM_A_TEAPOT;
//	else if ( *(int *)status == 421 ) return HTTP_MISDIRECTED_REQUEST;
	else if ( *(int *)status == 422 ) return HTTP_UNPROCESSABLE_ENTITY;
	else if ( *(int *)status == 423 ) return HTTP_LOCKED;
	else if ( *(int *)status == 424 ) return HTTP_FAILED_DEPENDENCY;
//	else if ( *(int *)status == 425 ) return HTTP_TOO_EARLY;
	else if ( *(int *)status == 426 ) return HTTP_UPGRADE_REQUIRED;
	else if ( *(int *)status == 428 ) return HTTP_PRECONDITION_REQUIRED;
	else if ( *(int *)status == 429 ) return HTTP_TOO_MANY_REQUESTS;
	else if ( *(int *)status == 431 ) return HTTP_REQUEST_HEADER_FIELDS_TOO_LARGE;
	else if ( *(int *)status == 451 ) return HTTP_UNAVAILABLE_FOR_LEGAL_REASONS;
	else if ( *(int *)status == 500 ) return HTTP_INTERNAL_SERVER_ERROR;
	else if ( *(int *)status == 501 ) return HTTP_NOT_IMPLEMENTED;
	else if ( *(int *)status == 502 ) return HTTP_BAD_GATEWAY;
	else if ( *(int *)status == 503 ) return HTTP_SERVICE_UNAVAILABLE;
	else if ( *(int *)status == 504 ) return HTTP_GATEWAY_TIME_OUT;
	else if ( *(int *)status == 505 ) return HTTP_VERSION_NOT_SUPPORTED;
	else if ( *(int *)status == 506 ) return HTTP_VARIANT_ALSO_VARIES;
	else if ( *(int *)status == 507 ) return HTTP_INSUFFICIENT_STORAGE;
	else if ( *(int *)status == 508 ) return HTTP_LOOP_DETECTED;
	else if ( *(int *)status == 510 ) return HTTP_NOT_EXTENDED;
	else if ( *(int *)status == 511 ) return HTTP_NETWORK_AUTHENTICATION_REQUIRED;
	else				  return DECLINED;

}

static void 	   MWCS_register_hooks(	apr_pool_t *pool) {
	ap_hook_handler(MWCS_handler, NULL, NULL, APR_HOOK_LAST); pthread_mutex_init(&gate, NULL);  init_module_table();
}


static const char *MWCS_set_root(		cmd_parms *cmd, void *cfg, const char *arg) { ROOT 	    = malloc(strlen(arg)+1); strcpy(ROOT, 	   arg); fprintf(stderr, "INFO: Setting ROOT PATH: %s ...\n", 	ROOT); 			fflush(stderr); return NULL; }
static const char *WMTS_set_CachePath(		cmd_parms *cmd, void *cfg, const char *arg) { WMTSCachePath = malloc(strlen(arg)+1); strcpy(WMTSCachePath, arg); fprintf(stderr, "INFO: Setting WMTSCachePath PATH: %s ...\n", 	WMTSCachePath); fflush(stderr); return NULL; }

static const char *MWCS_set_IOThreads(		cmd_parms *cmd, void *cfg, const char *arg) { IOThreads	   = malloc(strlen(arg)+1); strcpy(IOThreads, 	  arg); fprintf(stderr, "INFO: I/O Threads: %s ...\n", 		IOThreads); 	fflush(stderr); return NULL; }
static const char *MWCS_set_ExtName(		cmd_parms *cmd, void *cfg, const char *arg) { ExtHostname  = malloc(strlen(arg)+1); strcpy(ExtHostname,   arg); fprintf(stderr, "INFO: Setting Hostname: %s ...\n", 	ExtHostname);	fflush(stderr); return NULL; }
static const char *WMTS_set_ExtName(		cmd_parms *cmd, void *cfg, const char *arg) { WMTSHostname = malloc(strlen(arg)+1); strcpy(WMTSHostname,  arg); fprintf(stderr, "INFO: [WMTS] Setting Hostname: %s ...\n",WMTSHostname);fflush(stderr); return NULL; }
static const char *API_set_DatacubeUrl(		cmd_parms *cmd, void *cfg, const char *arg) { APIDatacubeUrl = malloc(strlen(arg)+1); strcpy(APIDatacubeUrl, arg); fprintf(stderr, "INFO: [API] Datacube: %s ...\n", 	APIDatacubeUrl); fflush(stderr); return NULL; }

static const char *MWCS_set_OAuthServer(	cmd_parms *cmd, void *cfg, const char *arg) { OAuthServer   = malloc(strlen(arg)+1); strcpy(OAuthServer,  arg); fprintf(stderr, "INFO: Setting OAuth Server: %s ...\n", OAuthServer); 	fflush(stderr); return NULL; }
static const char *MWCS_set_OAuthId(		cmd_parms *cmd, void *cfg, const char *arg) { OAuthId	    = malloc(strlen(arg)+1); strcpy(OAuthId,      arg); fprintf(stderr, "INFO: Setting OAuth my Id: %s ...\n",  OAuthId); 	fflush(stderr); return NULL; }

static const char *MWCS_set_LogServerURL(	cmd_parms *cmd, void *cfg, const char *arg) { LogServerURL  = malloc(strlen(arg)+1); strcpy(LogServerURL, arg); fprintf(stderr, "INFO: Setting Log Server: %s ...\n",   LogServerURL); fflush(stderr); return NULL; }

static const char *WMTS_set_MaxTileAge(		cmd_parms *cmd, void *cfg, const char *arg) { WMTSMaxTileAge   = atol(arg); 					fprintf(stderr, "INFO: WMTS Max Tile Age: %ld secs ...\n",  WMTSMaxTileAge); fflush(stderr); return NULL; }
static const char *WMTS_set_MaxTileRatio(	cmd_parms *cmd, void *cfg, const char *arg) { WMTSMaxTileRatio = atof(arg); 					fprintf(stderr, "INFO: WMTS Max Tile Ratio: %f ...\n",    WMTSMaxTileRatio); fflush(stderr); return NULL; }

static const char *MWCS_set_GeomPOSTSize(	cmd_parms *cmd, void *cfg, const char *arg) { GeomPOSTSize   = atol(arg); 					fprintf(stderr, "INFO: POST Geom: %ld Byte...\n", 	   GeomPOSTSize);   fflush(stderr); return NULL; }


static const char *MWCS_set_WaterMark(		cmd_parms *cmd, void *cfg, const char *arg) { WaterMark    = malloc(strlen(arg)+1); strcpy(WaterMark,     arg); fprintf(stderr, "INFO: WaterMark %s ...\n",		WaterMark); 	fflush(stderr); return NULL; }
static const char *MWCS_set_OldStyleName(	cmd_parms *cmd, void *cfg ) 		    { KEEP_OLD_STYLE_NAME = TRUE; 					fprintf(stderr, "WARN: Force to use collections old style name ...\n"); fflush(stderr); return NULL; }
static const char *MWCS_set_DebugToken(		cmd_parms *cmd, void *cfg ) 		    { ENABLE_DEBUG_TOKEN  = TRUE; 					fprintf(stderr, "WARN: Debug Token is enable ...\n"); 			fflush(stderr); return NULL; }
static const char *MWCS_set_Experimental(	cmd_parms *cmd, void *cfg ) 		    { EXPERIMENTAL_MODE   = TRUE; 					fprintf(stderr, "WARN: **** EXPERIMENTAL MODE ****\n"); 		fflush(stderr); return NULL; }

static const char *MWCS_set_MGRSMaxTiles(	cmd_parms *cmd, void *cfg, const char *arg) { MaxMGRSTiles = malloc(strlen(arg)+1); strcpy(MaxMGRSTiles,  arg); fprintf(stderr, "INFO: Max MGRS tiles: %s ...\n", 	MaxMGRSTiles); 	   fflush(stderr); return NULL; }
static const char *MWCS_set_MaxTimeRange(	cmd_parms *cmd, void *cfg, const char *arg) { L_MaxTimeRange = atol(arg); 					fprintf(stderr, "INFO: Max Time Range: %ld ...\n", 	L_MaxTimeRange);   fflush(stderr); return NULL; }
static const char *MWCS_set_MemoryCalcFactor(	cmd_parms *cmd, void *cfg, const char *arg) { MemoryCalcFactor = atof(arg); 					fprintf(stderr, "INFO: Memory Factor: %f ...\n", 	MemoryCalcFactor); fflush(stderr); return NULL; }
static const char *MWCS_set_ColorTableUrl(	cmd_parms *cmd, void *cfg, const char *arg) { 
	int len = strlen(arg); if ( arg[len-1] == '/' ) len -=1; // To remove last slash if needed
	ColorTableUrl = malloc(len+1); strncpy(ColorTableUrl, arg, len); ColorTableUrl[len] = '\0'; fprintf(stderr, "INFO: Setting ColorTableUrl: %s ...\n",ColorTableUrl);fflush(stderr); return NULL; 
}
static const char *MWCS_set_MaxTasks(		cmd_parms *cmd, void *cfg, const char *arg) {

	L_MaxTasks = atoi(arg); if (L_MaxTasks <= 0 ) L_MaxTasks = 1;	

	/*	
	// Open common tasks number
	int	task_info;
	task_info = shm_open(SHTOBJ_PATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR );
	if ( ftruncate(task_info, sizeof(int) ) != 0 ) { fprintf(stderr,"ERROR: ftruncate for taks init!\n" ); fflush(stderr); sem_unlink(SEM_PATH); return NULL; }
	memcpy( mmap(NULL, sizeof(int), PROT_READ|PROT_WRITE, MAP_SHARED, task_info, 0), &L_MaxTasks, sizeof(int) );
	close(task_info);
	*/
	
	shm_unlink(SHTOBJ_PATH);	
	sem_unlink(SEM_PATH);

	fprintf(stderr, "INFO: Max Tasks: %d ...\n", L_MaxTasks); fflush(stderr); 
	return NULL; 
}

static const char *MWCS_set_MaxMemoryUse(	cmd_parms *cmd, void *cfg, const char *arg) {
	struct sysinfo myinfo; sysinfo(&myinfo); sysinfo(&myinfo); 
	unsigned long tot_mem = (unsigned long)(myinfo.mem_unit *  myinfo.totalram );


	if 	( ! strcmp(arg, "AUTO") ) 					L_MaxMemoryUse = tot_mem; 
	else if	( ( strlen(arg) > 1 ) && ( arg[ strlen(arg) - 1 ] == '%' ) )	L_MaxMemoryUse = tot_mem * ( atof(arg) / 100.0 );
	else 									L_MaxMemoryUse = ( atol(arg) <= tot_mem ) ? atol(arg) : tot_mem;

	shm_unlink(SHMOBJ_PATH);
	shm_unlink(SHTOBJ_PATH);
	sem_unlink(SEM_PATH);

	// Open common memory usage
	// int mem_info = shm_open(SHMOBJ_PATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
	// if ( ftruncate(mem_info, sizeof(unsigned long) ) != 0 ) { fprintf(stderr, "ERROR: ftruncate for MWCS_set_MaxMemoryUse!\n" ); fflush(stderr); return NULL; }
	// memcpy( mmap(NULL, sizeof(unsigned long), PROT_READ|PROT_WRITE, MAP_SHARED, mem_info, 0), &L_MaxMemoryUse, sizeof(unsigned long) ); 
	// close(mem_info);

	fprintf(stderr, "INFO: Max usable RAM set to %ld Byte ...\n",  L_MaxMemoryUse); fflush(stderr); 

	// Init if it's zero
	// sysinfo(&myinfo);
	// if ( wcs_memory_info <= 0 ) { wcs_memory_info = ( L_MaxMemoryUse > 0 ) ? (unsigned long )L_MaxMemoryUse : (unsigned long)myinfo.mem_unit * myinfo.freeram; }

	return NULL; 
}

static const char *MWCS_set_AWS(		cmd_parms *cmd, void *cfg, const char *key_id, const char *access_key, const char *region){
	AWS_ACCESS_KEY_ID	= malloc(strlen(key_id)		+1); strcpy( AWS_ACCESS_KEY_ID, 	key_id);
	AWS_SECRET_ACCESS_KEY	= malloc(strlen(access_key)	+1); strcpy( AWS_SECRET_ACCESS_KEY,	access_key);
	AWS_DEFAULT_REGION	= malloc(strlen(region)		+1); strcpy( AWS_DEFAULT_REGION,	region);
	fprintf(stderr, "INFO: AWS Configuration AWS_ACCESS_KEY_ID: %s, AWS_SECRET_ACCESS_KEY: %s, AWS_DEFAULT_REGION: %s ...\n", AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION ); fflush(stderr);
	return NULL;
}

static const char *MWCS_set_EnvVar(cmd_parms *cmd, void *cfg, const char *key, const char *value){
	int i;

	if ( ENVIRONMENT_VARIABLE[0] == NULL ) {
		ENVIRONMENT_VARIABLE[0] = (char **)malloc( MAX_D * sizeof( char *) );
		ENVIRONMENT_VARIABLE[1] = (char **)malloc( MAX_D * sizeof( char *) );
		for ( i = 0; i < MAX_D; i++) ENVIRONMENT_VARIABLE[0][i] = ENVIRONMENT_VARIABLE[1][i] = NULL;
	}

	for ( i = 0; i < MAX_D; i++) if ( ENVIRONMENT_VARIABLE[0][i] == NULL ) break;
	if ( i == MAX_D ) return NULL;


	ENVIRONMENT_VARIABLE[0][i] = malloc(strlen(key)   +1 ); strcpy( ENVIRONMENT_VARIABLE[0][i], key);
	ENVIRONMENT_VARIABLE[1][i] = malloc(strlen(value) +1 ); strcpy( ENVIRONMENT_VARIABLE[1][i], value); 
	
	fprintf(stderr, "INFO: Setting environment variable %s = %s ...\n", ENVIRONMENT_VARIABLE[0][i], ENVIRONMENT_VARIABLE[1][i] );

	return NULL;
}


static const char *MWCS_set_MEMC_Server(cmd_parms *cmd, void *cfg, const char *server_name, const char *MEMCACHED_PORT){
	memcached_return rc;

	if ( MEMC_memc == NULL ) MEMC_memc = memcached_create(NULL);

	MEMC_servers 	= memcached_server_list_append(MEMC_servers, server_name, atoi(MEMCACHED_PORT), &rc);
	rc 		= memcached_server_push(MEMC_memc, MEMC_servers);
	if ( rc == MEMCACHED_SUCCESS ) 	{ fprintf(stderr, "INFO: Added server Memcached %s:%s successfully\n", server_name, MEMCACHED_PORT					); }
	else				{ fprintf(stderr, "ERROR: Couldn't add server MEMCached %s:%s: %s\n",  server_name, MEMCACHED_PORT, memcached_strerror(MEMC_memc, rc)	); }
	return NULL;

}



static const char *MWCS_add_EPSG(cmd_parms *cmd, void *cfg, const char *epsg, const char *proj4){
	Unknown_EPSG_proj4 *cursor = NULL, *dummy = NULL;
	if ( dummy == NULL ) dummy = (Unknown_EPSG_proj4 *)malloc( sizeof( Unknown_EPSG_proj4 ) ); dummy->next = NULL; 

	dummy->epsg = atoi(epsg);
	dummy->hSRS = OSRNewSpatialReference(NULL);
	if ( OSRSetFromUserInput( dummy->hSRS, proj4 ) != OGRERR_NONE ) 	{ fprintf(stderr, "INFO: Proj4: %s is BAD!\n", proj4 ); 				fflush(stderr); return NULL; }
	else									{ fprintf(stderr, "INFO: Added Unknown EPSG:%d as '%s' ...\n", dummy->epsg, proj4 ); 	fflush(stderr); }


	if ( EPSG_proj4 == NULL ) { EPSG_proj4 = dummy; dummy = NULL;	}
	else {		  
		for ( cursor = EPSG_proj4; cursor->next != NULL; cursor = cursor->next);
		cursor->next 	= dummy;
		dummy 		= NULL; 
	}


	return NULL;
}



static const char *MWCS_set_Title( 	    	   cmd_parms *cmd, void *cfg, const char *arg ){ MWCSTitle 		    = malloc(strlen(arg)+1); strcpy(MWCSTitle, 			arg); 
											fprintf(stderr, "INFO: Setting WCS Title: %s ...\n", 			MWCSTitle ); 			fflush(stderr); return NULL; }
static const char *MWCS_set_ProviderName(   	   cmd_parms *cmd, void *cfg, const char *arg ){ MWCSProviderName   	    = malloc(strlen(arg)+1); strcpy(MWCSProviderName, 		arg);
											fprintf(stderr, "INFO: Setting WCS ProviderName: %s ...\n", 		MWCSProviderName );		fflush(stderr); return NULL; }
static const char *MWCS_set_ProviderSite(   	   cmd_parms *cmd, void *cfg, const char *arg ){ MWCSProviderSite   	    = malloc(strlen(arg)+1); strcpy(MWCSProviderSite, 		arg);
											fprintf(stderr, "INFO: Setting WCS ProviderSite: %s ...\n", 		MWCSProviderSite );		fflush(stderr); return NULL; }
static const char *MWCS_set_IndividualName( 	   cmd_parms *cmd, void *cfg, const char *arg ){ MWCSIndividualName  	    = malloc(strlen(arg)+1); strcpy(MWCSIndividualName,   	arg);
											fprintf(stderr, "INFO: Setting WCS IndividualName: %s ...\n", 		MWCSIndividualName );		fflush(stderr); return NULL; }
static const char *MWCS_set_ElectronicMailAddress( cmd_parms *cmd, void *cfg, const char *arg ){ MWCSElectronicMailAddress  = malloc(strlen(arg)+1); strcpy(MWCSElectronicMailAddress,  arg);
											fprintf(stderr, "INFO: Setting WCS ElectronicMailAddress: %s ...\n", 	MWCSElectronicMailAddress );	fflush(stderr); return NULL; }
static const char *MWCS_set_City( 		   cmd_parms *cmd, void *cfg, const char *arg ){ MWCSCity  		    = malloc(strlen(arg)+1); strcpy(MWCSCity,  			arg);
											fprintf(stderr, "INFO: Setting WCS City: %s ...\n", 	     		MWCSCity );			fflush(stderr); return NULL; }
static const char *MWCS_set_PostalCode( 	   cmd_parms *cmd, void *cfg, const char *arg ){ MWCSPostalCode  	    = malloc(strlen(arg)+1); strcpy(MWCSPostalCode,  		arg);
											fprintf(stderr, "INFO: Setting WCS PostalCode: %s ...\n", 		MWCSPostalCode );		fflush(stderr); return NULL; }
static const char *MWCS_set_Country( 		   cmd_parms *cmd, void *cfg, const char *arg ){ MWCSCountry  		    = malloc(strlen(arg)+1); strcpy(MWCSCountry,  		arg);
											fprintf(stderr, "INFO: Setting WCS Country: %s ...\n", 			MWCSCountry );			fflush(stderr); return NULL; }


static const command_rec MWCS_directives[] = {
	AP_INIT_TAKE1("MWCSRootPath", 		MWCS_set_root, 			NULL, RSRC_CONF, "The path to the root of directory structure for MWCS"),
	AP_INIT_TAKE1("MWCSIOThreads", 		MWCS_set_IOThreads, 		NULL, RSRC_CONF, "Number of thread for I/O, can be 1, 2, ... or AUTO"),
	AP_INIT_TAKE2("MWCSAddEPSG", 		MWCS_add_EPSG,			NULL, RSRC_CONF, "Add Unknown EPSG definition for MWCS"),
	AP_INIT_TAKE1("MWCSExternalHostname",  	MWCS_set_ExtName, 		NULL, RSRC_CONF, "Set external hostname for WCS ...."),
	AP_INIT_TAKE1("WMTSExternalHostname",  	WMTS_set_ExtName, 		NULL, RSRC_CONF, "Set external hostname for WMTS...."),
	AP_INIT_TAKE1("APIDatacubeUrl",  	API_set_DatacubeUrl, 		NULL, RSRC_CONF, "Set external datacube url http://...."),

	AP_INIT_TAKE1("WMTSCachePath", 		WMTS_set_CachePath,		NULL, RSRC_CONF, "The path to the root of directory structure for WMTS cache"),
	AP_INIT_TAKE1("WMTSMaxTileAge",		WMTS_set_MaxTileAge,		NULL, RSRC_CONF, "Set max age of a stored tile in cache for WMTS/WMS"),
	AP_INIT_TAKE1("WMTSMaxTileRatio",	WMTS_set_MaxTileRatio,		NULL, RSRC_CONF, "Set max ratio of original image resolution converted in tile WMTS/WMS"),

	AP_INIT_TAKE3("MWCSAWSConf",  		MWCS_set_AWS, 			NULL, RSRC_CONF, "Set AWS paramters AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION"),
	AP_INIT_TAKE1("MWCSMGRSMaxTiles", 	MWCS_set_MGRSMaxTiles, 		NULL, RSRC_CONF, "Set the max number of tiles MGRS auto discrovery"),
	AP_INIT_TAKE1("MWCSOAuthServer",  	MWCS_set_OAuthServer, 		NULL, RSRC_CONF, "OAuth Server http://...."),
	AP_INIT_TAKE1("MWCSOAuthId",  		MWCS_set_OAuthId, 		NULL, RSRC_CONF, "OAuth Server Id Name"),
	AP_INIT_TAKE1("MWCSLogServerURL",  	MWCS_set_LogServerURL, 		NULL, RSRC_CONF, "Log Server http://...."),
	AP_INIT_TAKE1("MWCSColorTableUrl",  	MWCS_set_ColorTableUrl,		NULL, RSRC_CONF, "Color Table Url http://...."),

	AP_INIT_TAKE2("MWCSEnvVar", 		MWCS_set_EnvVar,		NULL, RSRC_CONF, "Set a generic environment variable"),
	AP_INIT_TAKE2("MWCSMemcached", 		MWCS_set_MEMC_Server,		NULL, RSRC_CONF, "Add Memcached Server for WMTS/WMS"),

	AP_INIT_TAKE1("MWCSGeomPOSTSize", 	MWCS_set_GeomPOSTSize, 		NULL, RSRC_CONF, "Set the size for POST buffer for Geometry"),


	AP_INIT_TAKE1("MWCSMGRSMaxTiles", 	MWCS_set_MGRSMaxTiles, 		NULL, RSRC_CONF, "Set the max number of tiles MGRS auto discrovery"),
	AP_INIT_TAKE1("MWCSMaxTimeRange", 	MWCS_set_MaxTimeRange, 		NULL, RSRC_CONF, "Set the max time range in second for a single query"),
	AP_INIT_TAKE1("MWCSMaxMemory", 		MWCS_set_MaxMemoryUse, 		NULL, RSRC_CONF, "Set the max memory usable."),
	AP_INIT_TAKE1("MWCSMemoryCalcFactor",	MWCS_set_MemoryCalcFactor, 	NULL, RSRC_CONF, "Factor to computer memory usage."),
	AP_INIT_TAKE1("MWCSMaxTasks",		MWCS_set_MaxTasks, 		NULL, RSRC_CONF, "Set the max number of tasks accepted."),


	AP_INIT_TAKE1("MWCSWaterMark",		MWCS_set_WaterMark, 		NULL, RSRC_CONF, "Set File to use as water mark"),

	AP_INIT_NO_ARGS("MWCSDebugToken",	MWCS_set_DebugToken,		NULL, RSRC_CONF, "Enable debug token"),
	AP_INIT_NO_ARGS("MWCSExperimental",	MWCS_set_Experimental,		NULL, RSRC_CONF, "Enable Experimenal Mode"),


	AP_INIT_TAKE1("MWCSTitle", 			MWCS_set_Title, 			NULL, RSRC_CONF, "Set WCS tag ows:Title"),
	AP_INIT_TAKE1("MWCSProviderName", 		MWCS_set_ProviderName, 			NULL, RSRC_CONF, "Set WCS tag ows:ProviderName"),
	AP_INIT_TAKE1("MWCSProviderSite", 		MWCS_set_ProviderSite, 			NULL, RSRC_CONF, "Set WCS tag ows:ProviderSite"),
	AP_INIT_TAKE1("MWCSIndividualName", 		MWCS_set_IndividualName, 		NULL, RSRC_CONF, "Set WCS tag ows:IndividualName"),
	AP_INIT_TAKE1("MWCSElectronicMailAddress", 	MWCS_set_ElectronicMailAddress,		NULL, RSRC_CONF, "Set WCS tag ows:ElectronicMailAddres"),
	AP_INIT_TAKE1("MWCSCity", 			MWCS_set_City,				NULL, RSRC_CONF, "Set WCS tag ows:City"),
	AP_INIT_TAKE1("MWCSPostalCode", 		MWCS_set_PostalCode,			NULL, RSRC_CONF, "Set WCS tag ows:PostalCode"),
	AP_INIT_TAKE1("MWCSCountry", 			MWCS_set_Country,			NULL, RSRC_CONF, "Set WCS tag ows:Country"),


	{ NULL }
};




module AP_MODULE_DECLARE_DATA MWCS_module = {
	STANDARD20_MODULE_STUFF, 
	NULL, 		 	/* create per-dir    config structures */
	NULL,                  	/* merge  per-dir    config structures */
	NULL,                  	/* create per-server config structures */
	NULL,                  	/* merge  per-server config structures */
	MWCS_directives,       	/* table of config file commands       */
	MWCS_register_hooks  	/* register hooks                      */
};






//------------------------------------------------------------------------------------------------
// This part is devoted to parsing math input string ... it works? ... I don't know.

int init_math_parser(void){
        int i;
        pat_t *p;

        for (i = 0, p = pat_ops; p[i].str; i++) if (regcomp(&(p[i].re), p[i].str, REG_NEWLINE|REG_EXTENDED)) fail("comp", p[i].str);
        for (i = 0, p = pat_arg; p[i].str; i++) if (regcomp(&(p[i].re), p[i].str, REG_NEWLINE|REG_EXTENDED)) fail("comp", p[i].str);

        return 1;
}

pat_t* match(char *s, pat_t *p, str_tok_t * t, char **e){
        int i;
        regmatch_t m;

        while (*s == ' ') s++;
        *e = s;

        if (!*s) return &pat_eos;

        for (i = 0; p[i].str; i++) {
                if (regexec(&(p[i].re), s, 1, &m, REG_NOTEOL)) continue;
                t->s    = s;
                *e      = s + (t->len = m.rm_eo - m.rm_so);
                return p + i;
        }
        return 0;
}

int parse(struct mathUnit *unit){
        pat_t           *p      = NULL;
        str_tok_t       *t      = NULL;
        str_tok_t       tok;

	if (unit 		== NULL ) return 0;
	if (unit->coveragemath 	== NULL ) return 0;

	char *s	= unit->coveragemath;
        unit->prec_booster 	= 0;
	unit->l_queue 		= 0;
	unit->l_stack		= 0;
	unit->l_stack_max	= 0;

        while (*s) {
                p = match(s, pat_arg, &tok, &s);
                if (!p || p == &pat_eos) fail("parse arg", s);
                if (p->prec == -1) { unit->prec_booster += 100; continue; }

                qpush(tok);

re_op:          p = match(s, pat_ops, &tok, &s);
                if (!p) fail("parse op", s);

                tok.assoc       = p->assoc;
                tok.prec        = p->prec;


                if      (p->prec > 0) tok.prec = p->prec + unit->prec_booster;
                else if (p->prec == -1) {
                        if (unit->prec_booster < 100) fail("unmatched )", s);
                        tok.prec = unit->prec_booster;
                }

                while (unit->l_stack) {
                        t = unit->stack + unit->l_stack - 1;
                        if (!(t->prec == tok.prec && t->assoc == A_L) && t->prec <= tok.prec) break;
			if ( unit->l_stack > unit->l_stack_max ) unit->l_stack_max = unit->l_stack; 
                        qpush(spop());
                }

                if (p->prec == -1) {
                        unit->prec_booster -= 100;
                        goto re_op;
                }
                if (!p->prec) {
                        if (unit->prec_booster) fail("unmatched (", s);
                        return 1;
                }
                spush(tok);
        }



        return 1;

}

//------------------------------------------------------------------------------------------------



int printTupleList(request_rec *r, block head, int pxAOISizeX, int pxAOISizeY ){
	int i, x, y;
	block cursor = NULL;

	for ( x = 0; 		x < pxAOISizeX - 1; 	x++)
	for ( y = 0; 		y < pxAOISizeY; 	y++)
	for ( cursor = head; 	cursor != NULL; 	cursor = cursor->next)	{
	for ( i = 0;		i < cursor->nband - 1;	i++) 			ap_rprintf(r, "%g ", cursor->raster[i][  x + y * pxAOISizeX  ]);
										ap_rprintf(r, "%g,", cursor->raster[i][  x + y * pxAOISizeX  ]);
										}                                                            
                                                                                                                                             
	for ( y = 0; 		y < pxAOISizeY - 1; 	y++)                                                                                 
	for ( cursor = head; 	cursor != NULL; 	cursor = cursor->next)	{                                                            
	for ( i = 0;		i < cursor->nband - 1;	i++) 			ap_rprintf(r, "%g ", cursor->raster[i][  x + y * pxAOISizeX  ]);
										ap_rprintf(r, "%g,", cursor->raster[i][  x + y * pxAOISizeX  ]);
										}                                                            
                                                                                                                                             
                                                                                                                                             
	for ( cursor = head; 	cursor->next != NULL; 	cursor = cursor->next) 	{                                                            
	for ( i = 0;		i < cursor->nband - 1;	i++) 			ap_rprintf(r, "%g ", cursor->raster[i][  x + y * pxAOISizeX  ]);
										ap_rprintf(r, "%g,", cursor->raster[i][  x + y * pxAOISizeX  ]);
										}                                                            
                                                                                                                                             
	for ( i = 0;		i < cursor->nband - 1;	i++) 			ap_rprintf(r, "%g ", cursor->raster[i][  x + y * pxAOISizeX  ]);
										ap_rprintf(r, "%g",  cursor->raster[i][  x + y * pxAOISizeX  ]);

	return 0;


}


//------------------------------------------------------------------------------------------------

// Y = Year
// M = Month
// D = Day
// T = time
// E = EPSG
// G = GeoTransform

int numcompare(const struct dirent** file1, const struct dirent** file2) {

	double diff = atof( (*file1)->d_name ) - atof( (*file2)->d_name );

	//	if ( sscanf(entry->d_name, "%*[^0-9]%lf", &grid) == 0 ) 

	if (diff <  0) { return -1; }
	if (diff >  0) { return  1; }
	if (diff == 0) { return  0; }

	return 0;
}

int namecompare(const struct dirent** file1, const struct dirent** file2){

    const char *a = (*file1)->d_name;
    const char *b = (*file2)->d_name;
    return strcasecmp(a,b);
}
	

 
int  walkthrough(const char *name, char *prod_array, char *subdataset, block *head, block *cursor, block tmp,  struct tm *t_time,
				time_t 			t_start,
				time_t 			t_finish,
				int			s_ref_year,
				int			s_ref_month,
				int			s_ref_day,
				int			s_ref_time,
				OGRSpatialReferenceH 	hSourceSRS,
				double 			x_input_ul,
				double 			y_input_ul,
				double 			x_input_ur,
				double 			y_input_ur,
				double 			x_input_lr,
				double 			y_input_lr,
				double 			x_input_ll,
				double 			y_input_ll,
				double			h_input_low,
				double			h_input_high,
				double			x_min_res,
				double			y_min_res,
				int 			X_RANGE_DEFINED,
				int 			Y_RANGE_DEFINED,
				int			X_RANGE_DEFINED_PX, 
				int			Y_RANGE_DEFINED_PX,
				int 			H_RANGE_DEFINED,
			       	int			PROD_FOUND,
				int			VIRTUAL_COLLECTION,	
				char			*input_name,
				double			*grid_starts,
				double			*grid_stops,
				int			grid_index,
				char			*mgrs_tile,

				int *hit_number, request_rec *r ){

	struct 		dirent **namelist;
	struct 		dirent *entry;

	int n = -1; 
	char *base = basename( (char *)name );
	if 	( base[0] == 'D' )	n = scandir(name, &namelist, 0, namecompare);
	else				n = scandir(name, &namelist, 0, numcompare ); //alphasort versionsort

	if (n < 0) return 1;
	while (n--) {
		time_t 	t_target;
	       	char 	path[MAX_PATH_LEN];
		int 	len 		= 0;
		int 	t_ref_time 	= 0;
		int 	pxSizeX 	= 0;
		int 	pxSizeY 	= 0;
		int	nband		= 0;
		int 	type		= 0;
		int	pxAOISizeX	= 0;
		int 	pxAOISizeY	= 0;
		double	grid		= 0;
		double	Xp_ul		= 0;
		double 	Yp_ul		= 0;
		double	Xp_ur		= 0;
		double 	Yp_ur		= 0;
		double 	Xp_lr		= 0;
		double 	Yp_lr		= 0;
		double 	Xp_ll		= 0;
		double 	Yp_ll		= 0;

		int	GOOD_WAY	= TRUE;
		int	CONTINUE	= TRUE;
		int	grid_index_next	= grid_index;
		char	*value		= NULL;
		char	label		= '\0';

	        // double  latitude        = 0.0;
		// double  longitude       = 0.0;
		// char	mgrUTM_z1[255];
		// char	mgrUTM_z2[255];

		OGRSpatialReferenceH hTargetSRS;
		char 	*wktTargetSRS;
		double 	adfGeoTransform[6], adfInvTransform[6];


		entry = namelist[n];

	        if ( entry->d_name[0]  == '.' ) continue;

		len = snprintf(path, sizeof(path)-1, "%s/%s", name, entry->d_name); path[len] = 0;

		if ( entry->d_type == DT_UNKNOWN ){
			struct stat fs;
			lstat(path, &fs); if( S_ISDIR(fs.st_mode) ) entry->d_type = DT_DIR;
		}

		if (entry->d_type == DT_DIR) {
			value 	= &entry->d_name[1];
			label	=  entry->d_name[0];

			switch (label){					
				case 't': // MGRS tiling tXX where XX is the zone number


					if ( mgrs_tile == NULL ) break;
					if ( ( strlen(value) == 2 ) && ( value[0]  >= 48 ) && ( value[0]  <= 57  ) && ( value[1]  >= 48 ) && ( value[1]  <= 57  )){ len = snprintf(path, sizeof(path)-1, "%s/%s", name, mgrs_tile ); path[len] = 0; CONTINUE = FALSE; }
					break;	

					

				case 'Y': // Year 
					if ( s_ref_year  >= 0 ) { len = snprintf(path, sizeof(path)-1, "%s/%c%04d", name, label, s_ref_year);  path[len] = 0; t_time->tm_year = s_ref_year - 1900; 	CONTINUE = FALSE; }
					else t_time->tm_year	 = atoi(value) - 1900;

					tmp->infoflag[0] = TRUE;
					break;
				case 'M': // Month
					if ( s_ref_month >= 0 ) { len = snprintf(path, sizeof(path)-1, "%s/%c%02d", name, label, s_ref_month); path[len] = 0; t_time->tm_mon  = s_ref_month - 1;	CONTINUE = FALSE; }
					else t_time->tm_mon	 = atoi(value) - 1;
					
					tmp->infoflag[1] = TRUE;
					break;
				case 'D': // Day
					if ( s_ref_day   >= 0 ) { len = snprintf(path, sizeof(path)-1, "%s/%c%02d", name, label, s_ref_day  ); path[len] = 0; t_time->tm_mday = s_ref_day;		CONTINUE = FALSE; }
					else  t_time->tm_mday	 = atoi(value);

					tmp->infoflag[2] = TRUE;
					break;
				case 'T': // Time HHMMSS
					if ( s_ref_time  >= 0 ) { len = snprintf(path, sizeof(path)-1, "%s/%c%06d", name, label, s_ref_time ); path[len] = 0; t_ref_time      = s_ref_time;		CONTINUE = FALSE; }
					else  t_ref_time = atoi(value);

					t_time->tm_hour = ( t_ref_time / 10000 );
					t_time->tm_min  = ( t_ref_time - t_time->tm_hour * 10000 ) / 100;
					t_time->tm_sec  = ( t_ref_time - t_time->tm_hour * 10000 - t_time->tm_min * 100 );

					t_target 	 = timegm(t_time);
					if ( 	( t_start <= t_target ) && ( t_target <= t_finish ) ) 	{ tmp->time = t_target; tmp->infoflag[3] = TRUE; }
					else if ( t_start >  t_target )					{ GOOD_WAY = FALSE; CONTINUE = FALSE; }
					else								{ GOOD_WAY = FALSE; }


					break;
				case 'E': // EPSG
					hTargetSRS = OSRNewSpatialReference(NULL);

					#if GDAL_VERSION >= 304
					OSRSetAxisMappingStrategy(hTargetSRS, OAMS_TRADITIONAL_GIS_ORDER); // GDAL 3.0 Fix
					#endif

					tmp->epsg  = atoi(value);

					if ( ImportFromEPSG(	&hTargetSRS, 	tmp->epsg) != OGRERR_NONE ) { GOOD_WAY = FALSE; break; }
					OSRExportToWkt(		hTargetSRS, 	&wktTargetSRS);
					tmp->wkt   = malloc(strlen(wktTargetSRS)+1); strcpy(tmp->wkt, wktTargetSRS);

					// fprintf(stderr, "IN : %s UL: %fx%f / UR: %fx%f / LR: %fx%f / LL: %fx%f \n",  
					//		OSRGetAttrValue (hSourceSRS , "AUTHORITY", 1), x_input_ul, y_input_ul, x_input_ur, y_input_ur, x_input_lr, y_input_lr, x_input_ll, y_input_ll ); fflush(stderr);

					if ( ( hSourceSRS != NULL ) && ( OSRGetAttrValue (hSourceSRS, "AUTHORITY", 1) != NULL ) && ( tmp->epsg != atoi( OSRGetAttrValue (hSourceSRS, "AUTHORITY", 1) )) ) {
	                                        OGRCoordinateTransformationH hCT = OCTNewCoordinateTransformation( hSourceSRS, hTargetSRS );
												
											
						if ( OCTTransform(hCT, 1, &x_input_ul, &y_input_ul, NULL) == FALSE ) { GOOD_WAY = FALSE; break; }
						if ( OCTTransform(hCT, 1, &x_input_ur, &y_input_ur, NULL) == FALSE ) { GOOD_WAY = FALSE; break; }
						if ( OCTTransform(hCT, 1, &x_input_lr, &y_input_lr, NULL) == FALSE ) { GOOD_WAY = FALSE; break; }
						if ( OCTTransform(hCT, 1, &x_input_ll, &y_input_ll, NULL) == FALSE ) { GOOD_WAY = FALSE; break; }

						OCTDestroyCoordinateTransformation(hCT);
					}

					// fprintf(stderr, "OUT: %d UL: %fx%f / UR: %fx%f / LR: %fx%f / LL: %fx%f \n", tmp->epsg,  x_input_ul, y_input_ul, x_input_ur, y_input_ur, x_input_lr, y_input_lr, x_input_ll, y_input_ll ); fflush(stderr);

					hSourceSRS = OSRClone(hTargetSRS);
					OSRDestroySpatialReference(hTargetSRS);
					tmp->infoflag[4] = TRUE;
					break;
				case 'G': // GeoTransform and image information
					sscanf(value, "%lfx%lf_%lfx%lf_%dx%dx%d_%d",   &adfGeoTransform[0], &adfGeoTransform[3], &adfGeoTransform[1], &adfGeoTransform[5], &pxSizeX, &pxSizeY, &nband, &type );
					if ( GDALInvGeoTransform(adfGeoTransform, adfInvTransform ) == FALSE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: GDALInvGeoTransform %s\n", value); GOOD_WAY = FALSE; break; }
					if ( ( PROD_FOUND == TRUE ) && ( VIRTUAL_COLLECTION == FALSE ) && ( x_min_res != adfGeoTransform[1] ) && ( y_min_res != adfGeoTransform[5] ) ) { GOOD_WAY = FALSE; break; }

					// Create square coorner coordinates
					Xp_ul = floor(	adfInvTransform[0] + x_input_ul * adfInvTransform[1] + y_input_ul * adfInvTransform[2] ) + 0.001;
					Yp_ul = floor(	adfInvTransform[3] + x_input_ul * adfInvTransform[4] + y_input_ul * adfInvTransform[5] ) + 0.001;
					Xp_ur = floor(	adfInvTransform[0] + x_input_ur * adfInvTransform[1] + y_input_ur * adfInvTransform[2] ) + 0.001;
					Yp_ur = floor(	adfInvTransform[3] + x_input_ur * adfInvTransform[4] + y_input_ur * adfInvTransform[5] ) + 0.001;
					Xp_lr = floor(	adfInvTransform[0] + x_input_lr * adfInvTransform[1] + y_input_lr * adfInvTransform[2] ) + 0.001;
					Yp_lr = floor(	adfInvTransform[3] + x_input_lr * adfInvTransform[4] + y_input_lr * adfInvTransform[5] ) + 0.001;
					Xp_ll = floor(	adfInvTransform[0] + x_input_ll * adfInvTransform[1] + y_input_ll * adfInvTransform[2] ) + 0.001;
					Yp_ll = floor(	adfInvTransform[3] + x_input_ll * adfInvTransform[4] + y_input_ll * adfInvTransform[5] ) + 0.001;

					if ( Xp_ll < Xp_ul ) Xp_ul = Xp_ll;
					if ( Xp_ur > Xp_lr ) Xp_lr = Xp_ur;

					if ( Yp_ur < Yp_ul ) Yp_ul = Yp_ur;
					if ( Yp_ll > Yp_lr ) Yp_lr = Yp_ll;
					
					// fprintf(stderr, "%fx%f %fx%f %fx%f %fx%f %dx%d\n", Xp_ul, Yp_ul, Xp_ur, Yp_ur, Xp_lr, Yp_lr, Xp_ll, Yp_ll, pxSizeX, pxSizeY ); fflush(stderr);

					if ( X_RANGE_DEFINED_PX == TRUE ) { Xp_ul = x_input_ul; Xp_lr = x_input_lr; }
					if ( Y_RANGE_DEFINED_PX == TRUE ) { Yp_ul = y_input_ul; Yp_lr = y_input_lr; }

					if (	( x_input_ul == x_input_lr )		&&
						( y_input_ul == y_input_lr )		&&
						( X_RANGE_DEFINED == TRUE  )		&&
						( Y_RANGE_DEFINED == TRUE  )		)
						if (	( Xp_ul < 0 )			||
							( Yp_ul < 0 )			||
							( Xp_ul > pxSizeX - 1 ) 	||
							( Yp_ul > pxSizeY - 1 )		) { GOOD_WAY = FALSE; break; } 
						else					{ Xp_lr = Xp_ul + 1; Yp_lr = Yp_ul + 1; pxAOISizeX = 1; pxAOISizeY = 1;}
					else {
	
						if ( X_RANGE_DEFINED == FALSE ) { Xp_ul = 0; Xp_lr = (pxSizeX - 1); pxAOISizeX = pxSizeX;} 
						else {
							if ( Xp_ul < 0 ) 		Xp_ul = 0;
							if ( Xp_ul == Xp_lr   )		Xp_lr = Xp_ul   + 1;
							if ( Xp_lr >= pxSizeX ) 	Xp_lr = pxSizeX - 1;
							pxAOISizeX = (int)ceil( Xp_lr - Xp_ul + 0.5 ); if ( pxAOISizeX <= 0 ) { GOOD_WAY = FALSE; break; }
						}
	
	
						if ( Y_RANGE_DEFINED == FALSE ) { Yp_ul = 0; Yp_lr = (pxSizeY - 1); pxAOISizeY = pxSizeY;} 
						else {
							if ( Yp_ul < 0 ) 		Yp_ul = 0;
							if ( Yp_ul == Yp_lr   )		Yp_lr = Yp_ul	+ 1;
							if ( Yp_lr >= pxSizeY ) 	Yp_lr = pxSizeY - 1;
							pxAOISizeY = (int)ceil( Yp_lr - Yp_ul + 0.5 ); if ( pxAOISizeY <= 0 ) { GOOD_WAY = FALSE; break; }
						}
					}

					tmp->sizeX	= pxAOISizeX;
					tmp->sizeY	= pxAOISizeY;
					tmp->offsetX	= pxAOISizeX;
					tmp->offsetY	= pxAOISizeY;
					tmp->srcSizeX	= pxSizeX;
					tmp->srcSizeY	= pxSizeY;
					tmp->upX	= (int)Xp_ul;
					tmp->upY	= (int)Yp_ul;
					tmp->nband	= nband;
					tmp->tband	= (int *)malloc( tmp->nband * sizeof(int)); for (int i = 0; i < tmp->nband; i++) tmp->tband[i] = i;
					tmp->type	= type;

					tmp->GeoTransform[1] = adfGeoTransform[1];
					tmp->GeoTransform[2] = adfGeoTransform[2];
					tmp->GeoTransform[4] = adfGeoTransform[4];
					tmp->GeoTransform[5] = adfGeoTransform[5];

					tmp->GeoTransform[0] = adfGeoTransform[0] + tmp->upX * adfGeoTransform[1] + tmp->upY * adfGeoTransform[2];
					tmp->GeoTransform[3] = adfGeoTransform[3] + tmp->upX * adfGeoTransform[4] + tmp->upY * adfGeoTransform[5];
					
					// this is fucking wrong!
					// tmp->GeoTransform[0] = adfGeoTransform[0] + Xp_ul * adfGeoTransform[1] + Yp_ul * adfGeoTransform[2];
					// tmp->GeoTransform[3] = adfGeoTransform[3] + Xp_ul * adfGeoTransform[4] + Yp_ul * adfGeoTransform[5];

					tmp->infoflag[5] = TRUE;
					break;
				case 'H': // High
					tmp->high = atof(value); if ( H_RANGE_DEFINED == FALSE ) break;
					if ( ( h_input_low <= tmp->high ) && ( tmp->high  <= h_input_high ) )  	{ GOOD_WAY = TRUE;  	   }
					else									{ GOOD_WAY = FALSE; break; }
					break;

				case 'F':
					sscanf(value, "%dx%dx%d_%d", &pxSizeX, &pxSizeY, &nband, &type );
					adfGeoTransform[0] 	= 0.0; 	adfGeoTransform[1] 	= 1.0; 	adfGeoTransform[2] 	=  0.0;
					adfGeoTransform[3] 	= 0.0;	adfGeoTransform[4] 	= 0.0;	adfGeoTransform[5] 	= -1.0;

					
					Xp_ul = 0;	 Yp_ul = 0; 
					Xp_lr = pxSizeX; Yp_lr = pxSizeY;
					if ( X_RANGE_DEFINED_PX == TRUE ) { Xp_ul = x_input_ul; Xp_lr = x_input_lr; }
					if ( Y_RANGE_DEFINED_PX == TRUE ) { Yp_ul = y_input_ul; Yp_lr = y_input_lr; }

					if ( Xp_ul > pxSizeX ) 	Xp_ul = pxSizeX;
					if ( Xp_lr > pxSizeX ) 	Xp_lr = pxSizeX;
					if ( Yp_ul > pxSizeY ) 	Yp_ul = pxSizeY;
					if ( Yp_lr > pxSizeY ) 	Yp_lr = pxSizeY;

					pxAOISizeX 	= (int)( Xp_lr - Xp_ul + 0.5 );
					pxAOISizeY 	= (int)( Yp_lr - Yp_ul + 0.5 );
					tmp->sizeX	= pxAOISizeX;
					tmp->sizeY	= pxAOISizeY;
					tmp->offsetX	= pxAOISizeX;
					tmp->offsetY	= pxAOISizeY;
					tmp->srcSizeX	= pxSizeX;
					tmp->srcSizeY	= pxSizeY;
					tmp->upX	= (int)Xp_ul;
					tmp->upY	= (int)Yp_ul;
					tmp->nband	= nband;
					tmp->tband	= (int *)malloc( tmp->nband * sizeof(int)); for (int i = 0; i < tmp->nband; i++) tmp->tband[i] = i;
					tmp->type	= type;
					tmp->wkt	= NULL;

					tmp->GeoTransform[1] = adfGeoTransform[1];
					tmp->GeoTransform[2] = adfGeoTransform[2];
					tmp->GeoTransform[4] = adfGeoTransform[4];
					tmp->GeoTransform[5] = adfGeoTransform[5];

					tmp->GeoTransform[0] = adfGeoTransform[0] + tmp->upX * adfGeoTransform[1] + tmp->upY * adfGeoTransform[2];
					tmp->GeoTransform[3] = adfGeoTransform[3] + tmp->upX * adfGeoTransform[4] + tmp->upY * adfGeoTransform[5];

					memcpy(tmp->srcGeoTransform, tmp->GeoTransform, sizeof(double) * 6 );
					tmp->infoflag[5] = TRUE;
					break;
				case 'L':
					if ( subdataset != NULL ) {
						if ( ( value[0] == '_' ) && ( ! strcmp(subdataset, value + 1 ) ) ) 	{ GOOD_WAY = TRUE;    		}
						else								 	{ GOOD_WAY = FALSE; break; 	}
					} GOOD_WAY = TRUE;
					
					break;

				default:
					if ( grid_starts == NULL ) break;
					if ( grid_stops	 == NULL ) break;
					if ( grid_index	 <  0	 ) break;

					//if ( sscanf(entry->d_name, "%*[^0-9]%lf", &grid) == 0 ) grid = atof(entry->d_name);
					//fprintf(stderr, "%f %f %f\n", grid_starts[grid_index], grid, grid_stops[grid_index]); fflush(stderr);

					grid = atof(entry->d_name);
					if 	( ( grid_starts[grid_index] == grid ) && ( grid == grid_stops[grid_index] ) ) 	{ GOOD_WAY = TRUE;  grid_index_next = grid_index-1; CONTINUE = FALSE; }
					else if ( ( grid_starts[grid_index] <= grid ) && ( grid <= grid_stops[grid_index] ) ) 	{ GOOD_WAY = TRUE;  grid_index_next = grid_index-1; CONTINUE =  TRUE; }
					else if ( ( grid_starts[grid_index] >  grid ) 					    ) 	{ GOOD_WAY = FALSE; CONTINUE = FALSE;  }
					else											{ GOOD_WAY = FALSE; }
					break;

			}
			if ( GOOD_WAY == TRUE ) {
				walkthrough(path, prod_array, subdataset, head, cursor, tmp, t_time,
							t_start,	t_finish,	s_ref_year,      s_ref_month,   s_ref_day,	s_ref_time,
							hSourceSRS,
							x_input_ul, 	y_input_ul, 	x_input_ur,     y_input_ur,	x_input_lr, 	 y_input_lr, 	x_input_ll,      y_input_ll, h_input_low, h_input_high,
							x_min_res,      y_min_res,	X_RANGE_DEFINED, Y_RANGE_DEFINED, X_RANGE_DEFINED_PX, Y_RANGE_DEFINED_PX, H_RANGE_DEFINED, PROD_FOUND, VIRTUAL_COLLECTION,
							input_name,																		
							grid_starts,    grid_stops,     grid_index_next, mgrs_tile,
							hit_number, r ); 
			}
			if ( CONTINUE == FALSE ) break;
		} else {

	        	if ( entry->d_name[0]  == '.' 			) continue;
			if ( strstr(entry->d_name, "DescribeCoverage" )	) continue;
			if ( strstr(entry->d_name, ".xml") 		) continue;
			if ( strstr(entry->d_name, ".py") 		) continue;
			if ( strstr(entry->d_name, ".txt") 		) continue;
			if ( strstr(entry->d_name, ".hdr") 		) continue;

			// Filter for file name
			if ( input_name != NULL ) if ( strstr(entry->d_name, input_name) == NULL ) continue;

			
			if ( *head == NULL )	{ *head = *cursor = (block)malloc(sizeof(struct sblock)); tmp->prev = NULL;				 	}
			else			{ (*cursor)->next = (block)malloc(sizeof(struct sblock)); tmp->prev = (*cursor); (*cursor) = (*cursor)->next; 	}


			memcpy((*cursor), tmp, sizeof(struct sblock));
			

			memcpy((*cursor)->srcGeoTransform, tmp->GeoTransform, sizeof(double) * 6 );
			// (*cursor)->wkt   	 = malloc(strlen(tmp->wkt)+1); strcpy((*cursor)->wkt, tmp->wkt);
			(*cursor)->next 	 = NULL;
	              	bzero((*cursor)->file,    MAX_PATH_LEN - 1); sprintf((*cursor)->file, "%s/%s", name, entry->d_name);
			(*cursor)->psWarpOptions = NULL; 
			(*cursor)->dataset	 = NULL;
			(*cursor)->vrt		 = NULL;
			(*cursor)->datasetId	 = NULL; 
			(*cursor)->raster	 = NULL;
			(*cursor)->Metadata	 = NULL;
			(*cursor)->MetadataCount = 0;
			(*cursor)->gcps		 = NULL;
			(*cursor)->gcps_cnt	 = 0;
			(*cursor)->shp_head	 = NULL;
			(*cursor)->shp_cursor	 = NULL;
			(*hit_number)++;
		}
		free(namelist[n]);
	}
	free(namelist);

	return 0;
}


int ColorizeDataset(struct info *info, GDALDatasetH hDstDS, GDALDatasetH *hMEMSrcDS, char *colortable, char *prod_path_array, double *scale, double *offset, double *nodata, int autoscale, double minscale, double maxscale, int NODATA_AUTO, char *equaliz_sigma ){
	int i,j;

	GDALRasterBandH	hBandSrc;

	request_rec *r = info->r;
	char colortable_path[MAX_PATH_LEN];

	GByte   *Red    = NULL;
	GByte   *Green  = NULL;
	GByte   *Blue   = NULL;
	GByte   *Alpha  = NULL;
	FILE    *CTfp   = NULL;
	double	*raster = NULL;
	char 	*buff	= NULL;
	size_t 	len 	= 0;
	ssize_t read;

	int 	pxAOISizeX 	= 0;
	int 	pxAOISizeY 	= 0;
	int	nband		= 0;

	double	dfMin		= 0.0;
	double	dfMax		= 0.0;
	double	pdfMean		= 0.0;
	double	padfStdDev	= 0.0;
        int     i_rgb   	= 0;
	int     ncolors 	= 0;
	int	CLASSFIED	= FALSE;
        double  R,G,B;
	char	*value		= NULL;
	char	*tok	 	= NULL;
	int 	*range		= NULL;
	unsigned int v		= 0;

	// I want use an unsigned int range of color/values, bat 0 is no value

	//GByte   rel[256];

	pxAOISizeX 	= GDALGetRasterXSize(hDstDS);
	pxAOISizeY 	= GDALGetRasterYSize(hDstDS);
	nband		= GDALGetRasterCount(hDstDS);
	*hMEMSrcDS       = GDALCreate( GDALGetDriverByName("MEM"), "", pxAOISizeX, pxAOISizeY, 4, GDT_Byte, NULL );
	if ( hMEMSrcDS == NULL )  { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: GDALCreate corolorize Dataset"); return 1; }

	Red             = (GByte *)malloc(sizeof(GByte) * pxAOISizeX * pxAOISizeY ); if ( Red   == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc Red");   return 1; }
	Green           = (GByte *)malloc(sizeof(GByte) * pxAOISizeX * pxAOISizeY ); if ( Green == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc Green"); return 1; }
	Blue            = (GByte *)malloc(sizeof(GByte) * pxAOISizeX * pxAOISizeY ); if ( Blue  == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc Blue");  return 1; }
	Alpha           = (GByte *)malloc(sizeof(GByte) * pxAOISizeX * pxAOISizeY ); if ( Alpha == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc Alpha"); return 1; }
	range 		= (int *)malloc(sizeof(int) * USHRT_MAX + 1 ); 		     if ( range  == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc range");    return 1; } 
	CPLErr		err;


	if ( colortable != NULL ) { i = strlen(colortable) - 6; if ( i < 0 ) i = 0; if ( ! strcmp(colortable + i, ".class" ) ) CLASSFIED = TRUE; } 
	if ( CLASSFIED  == TRUE ) { autoscale = FALSE; 	for(j = 0; j < ( USHRT_MAX + 1 ); j++ ) range[j] = -1;	}

	// First the case where the result dataset is and rgb ...
	 if ( nband >= 3 ) {
	        double *rasterRGB[3] = { NULL, NULL, NULL } ;
	        double scaleMin[3] = { 0.0, 0.0, 0.0 };
	       	double scaleMax[3] = { 0.0, 0.0, 0.0 };
		double Mean[3]	   = { 0.0, 0.0, 0.0 };
		double StdDev[3]   = { 0.0, 0.0, 0.0 };
	        rasterRGB[0] = (double *)malloc(sizeof(double) * pxAOISizeX * pxAOISizeY); if ( rasterRGB[0] == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc rasterRGB[0]");   return 1; }
	        rasterRGB[1] = (double *)malloc(sizeof(double) * pxAOISizeX * pxAOISizeY); if ( rasterRGB[1] == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc rasterRGB[1]");   return 1; }
	        rasterRGB[2] = (double *)malloc(sizeof(double) * pxAOISizeX * pxAOISizeY); if ( rasterRGB[2] == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc rasterRGB[2]");   return 1; }

	        hBandSrc = GDALGetRasterBand( hDstDS,    1); err = GDALRasterIO( hBandSrc, GF_Read,   0, 0,  pxAOISizeX, pxAOISizeY, rasterRGB[0], pxAOISizeX, pxAOISizeY, GDT_Float64,       0, 0);
	        if ( autoscale == FALSE ) GDALGetRasterStatistics( hBandSrc, FALSE, TRUE, &scaleMin[0], &scaleMax[0], &Mean[0], &StdDev[0] ); 
	        hBandSrc = GDALGetRasterBand( hDstDS,    2); err = GDALRasterIO( hBandSrc, GF_Read,   0, 0,  pxAOISizeX, pxAOISizeY, rasterRGB[1], pxAOISizeX, pxAOISizeY, GDT_Float64,       0, 0);
	        if ( autoscale == FALSE ) GDALGetRasterStatistics( hBandSrc, FALSE, TRUE, &scaleMin[1], &scaleMax[1], &Mean[1], &StdDev[1] ); 
	        hBandSrc = GDALGetRasterBand( hDstDS,    3); err = GDALRasterIO( hBandSrc, GF_Read,   0, 0,  pxAOISizeX, pxAOISizeY, rasterRGB[2], pxAOISizeX, pxAOISizeY, GDT_Float64,       0, 0);
	        if ( autoscale == FALSE ) GDALGetRasterStatistics( hBandSrc, FALSE, TRUE, &scaleMin[2], &scaleMax[2], &Mean[2], &StdDev[2] );  

		if ( autoscale == TRUE  ) { scaleMin[0] = scaleMin[1] = scaleMin[2] = minscale; scaleMax[0] = scaleMax[1] = scaleMax[2] = maxscale; }

		if ( ( autoscale == FALSE ) && (  GDALGetRasterDataType(hBandSrc) == GDT_Byte ) && ( nband == 3 ) ) { scaleMin[0] = scaleMin[1] = scaleMin[2] = 0; scaleMax[0] = scaleMax[1] = scaleMax[2] = 255; } // It sould be a RGB image, like a photo i think

		if ( ( scaleMin[0] == scaleMax[0] ) && 	( scaleMin[1] == scaleMax[1] ) && ( scaleMin[2] == scaleMax[2] ) )		
			{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "WARN: Min and Max are the same, skip colorize ..."); free(rasterRGB[0]); free(rasterRGB[1]); free(rasterRGB[2]); return 0; }


//		fprintf(stderr, "%f %f %f %f %f %f\n", scaleMin[0],  scaleMin[1],  scaleMin[2], scaleMax[0], scaleMax[1], scaleMax[2]  ); fflush(stderr);

		if (equaliz_sigma != NULL ) for ( i = 0; i < 3; i++){
			scaleMin[i] = Mean[i] - atoi(equaliz_sigma) *  StdDev[i];
			scaleMax[i] = Mean[i] + atoi(equaliz_sigma) *  StdDev[i];
		
		}

		if ( ( isnan( nodata[0] ) != 0 ) && ( isnan( nodata[1] ) != 0 ) && ( isnan( nodata[2] ) != 0 ) ){ // Check if nodata is Nan
		        for (i = 0 ; i < ( pxAOISizeX * pxAOISizeY ); i++){
				rasterRGB[0][i] = ( rasterRGB[0][i] == rasterRGB[0][i] ) ? ((( rasterRGB[0][i] - offset[0] ) * scale[0]) - scaleMin[0]) * 255 / (scaleMax[0] - scaleMin[0]): 0 ;
		                rasterRGB[1][i] = ( rasterRGB[1][i] == rasterRGB[1][i] ) ? ((( rasterRGB[1][i] - offset[1] ) * scale[1]) - scaleMin[1]) * 255 / (scaleMax[1] - scaleMin[1]): 0 ;
		                rasterRGB[2][i] = ( rasterRGB[2][i] == rasterRGB[2][i] ) ? ((( rasterRGB[2][i] - offset[2] ) * scale[2]) - scaleMin[2]) * 255 / (scaleMax[2] - scaleMin[2]): 0 ;
				Red[i]          = (GByte)( rasterRGB[0][i] > 255 ? 255 : rasterRGB[0][i] < 0 ? 0 : rasterRGB[0][i] );
				Green[i]        = (GByte)( rasterRGB[1][i] > 255 ? 255 : rasterRGB[1][i] < 0 ? 0 : rasterRGB[1][i] );
				Blue[i]         = (GByte)( rasterRGB[2][i] > 255 ? 255 : rasterRGB[2][i] < 0 ? 0 : rasterRGB[2][i] );
		                Alpha[i]        = ( ( Red[i] == 0 ) && ( Green[i] == 0 ) && ( Blue[i] == 0 ) ) ? 0 : 255 ;

		
		        }
		} else {
		        for (i = 0 ; i < ( pxAOISizeX * pxAOISizeY ); i++){
				rasterRGB[0][i] = ( rasterRGB[0][i] != nodata[0] ) ? ((( rasterRGB[0][i] - offset[0] ) * scale[0]) - scaleMin[0]) * 255 / (scaleMax[0] - scaleMin[0]): 0 ;
		                rasterRGB[1][i] = ( rasterRGB[1][i] != nodata[1] ) ? ((( rasterRGB[1][i] - offset[1] ) * scale[1]) - scaleMin[1]) * 255 / (scaleMax[1] - scaleMin[1]): 0 ;
		                rasterRGB[2][i] = ( rasterRGB[2][i] != nodata[2] ) ? ((( rasterRGB[2][i] - offset[2] ) * scale[2]) - scaleMin[2]) * 255 / (scaleMax[2] - scaleMin[2]): 0 ;
				Red[i]          = (GByte)( rasterRGB[0][i] > 255 ? 255 : rasterRGB[0][i] < 0 ? 0 : rasterRGB[0][i] );
				Green[i]        = (GByte)( rasterRGB[1][i] > 255 ? 255 : rasterRGB[1][i] < 0 ? 0 : rasterRGB[1][i] );
				Blue[i]         = (GByte)( rasterRGB[2][i] > 255 ? 255 : rasterRGB[2][i] < 0 ? 0 : rasterRGB[2][i] );
		                Alpha[i]        = ( ( Red[i] == 0 ) && ( Green[i] == 0 ) && ( Blue[i] == 0 ) ) ? 0 : 255 ;

		
		        }
		}

	        hBandSrc = GDALGetRasterBand( *hMEMSrcDS, 1); err = GDALRasterIO( hBandSrc, GF_Write,  0, 0,  pxAOISizeX, pxAOISizeY, Red,     pxAOISizeX, pxAOISizeY, GDT_Byte, 0, 0); GDALSetRasterColorInterpretation(hBandSrc, GCI_RedBand);
	        hBandSrc = GDALGetRasterBand( *hMEMSrcDS, 2); err = GDALRasterIO( hBandSrc, GF_Write,  0, 0,  pxAOISizeX, pxAOISizeY, Green,   pxAOISizeX, pxAOISizeY, GDT_Byte, 0, 0); GDALSetRasterColorInterpretation(hBandSrc, GCI_GreenBand);
	        hBandSrc = GDALGetRasterBand( *hMEMSrcDS, 3); err = GDALRasterIO( hBandSrc, GF_Write,  0, 0,  pxAOISizeX, pxAOISizeY, Blue,    pxAOISizeX, pxAOISizeY, GDT_Byte, 0, 0); GDALSetRasterColorInterpretation(hBandSrc, GCI_BlueBand);
	        hBandSrc = GDALGetRasterBand( *hMEMSrcDS, 4); err = GDALRasterIO( hBandSrc, GF_Write,  0, 0,  pxAOISizeX, pxAOISizeY, Alpha,   pxAOISizeX, pxAOISizeY, GDT_Byte, 0, 0); GDALSetRasterColorInterpretation(hBandSrc, GCI_AlphaBand);
	
	        free(rasterRGB[0]);     free(rasterRGB[1]);     free(rasterRGB[2]);
	        free(Red);              free(Green);            free(Blue);             free(Alpha);

	 } else  if ( nband == 1 ){
		GByte *rgb[3] = { NULL, NULL, NULL };
		raster	= (double *)malloc(sizeof(double) * pxAOISizeX * pxAOISizeY); 	if ( raster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc raster");   return 1; }
		value 	= malloc(100);	

		hBandSrc = GDALGetRasterBand( hDstDS,    1); 
		if ( NODATA_AUTO != FALSE ) GDALDeleteRasterNoDataValue(hBandSrc);	
		err = GDALRasterIO( hBandSrc, GF_Read,   0, 0,  pxAOISizeX, pxAOISizeY, raster, pxAOISizeX, pxAOISizeY, GDT_Float64,     0, 0);
		
		if ( GDALGetRasterStatistics( hBandSrc, FALSE, TRUE, &dfMin, &dfMax, &pdfMean, &padfStdDev ) != CE_None ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "WARN: Empty image, skip colorize ..."); free(raster); return 0; }

		if ( NODATA_AUTO != FALSE ){ if( NODATA_AUTO == NODATA_MIN ) nodata[0] = dfMin; if( NODATA_AUTO == NODATA_MAX ) nodata[0] = dfMax; }

		// http://www.ncl.ucar.edu/Document/Graphics/color_table_gallery.shtml
		
		if ( autoscale == FALSE ) { dfMin = ( dfMin - offset[0] ) * scale[0];   dfMax = ( dfMax - offset[0] ) * scale[0]; 	}
		else			  { dfMin = minscale;				dfMax = maxscale;				}


		if ( ColorTableUrl != NULL ) {
			CURLcode        res;
		        struct          MemoryStruct chunk;
		        long            response_code;
			CURL *curl	= NULL;
			char *token     = NULL;
		        chunk.memory    = malloc(1);
			chunk.size      = 0;
			size_t value_length; uint32_t flags; memcached_return rc;


		        curl = curl_easy_init();
		        if(curl) {
				bzero(colortable_path, MAX_PATH_LEN - 1);
				if 	( colortable != NULL ) 	snprintf(colortable_path, MAX_PATH_LEN, "%s/%s.rgb/", ColorTableUrl, colortable);
				else				snprintf(colortable_path, MAX_PATH_LEN, "%s/%s.rgb/", ColorTableUrl, DefaultColorTable);


				if ( MEMC_memc != NULL ) { 
					chunk.memory = (GByte *)memcached_get(MEMC_memc, colortable_path, strlen(colortable_path), &value_length, &flags, &rc);
					if (rc == MEMCACHED_SUCCESS) {	chunk.size   = value_length; 		  }
					else			     {	chunk.memory = malloc(1); chunk.size = 0; }
				}
					

				if ( chunk.size == 0 ) {
			                curl_easy_setopt(curl, CURLOPT_URL,             colortable_path);
			                curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,   WriteMemoryCallback);
			                curl_easy_setopt(curl, CURLOPT_WRITEDATA,       (void *)&chunk);
			                curl_easy_setopt(curl, CURLOPT_USERAGENT,       "MWCS/1.0");
	//		                curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
			                curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER,  FALSE);
			                curl_easy_setopt(curl, CURLOPT_SHARE,           info->share);
			                res = curl_easy_perform(curl);
			                if(res != CURLE_OK) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: ColorizeDataset curl_easy_perform() %s failed: %s", colortable_path, curl_easy_strerror(res) ); return FALSE; }
			                curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code); if ( response_code != 200 ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: ColorizeDataset get HTTP code %ld", response_code ); return FALSE; }
	
			                char            *ct           = NULL;
			                size_t          ct_len        = 0;
			                ssize_t         ct_n          = 0;


					if ( MEMC_memc != NULL ) {
						rc = memcached_set(MEMC_memc, colortable_path, strlen(colortable_path), chunk.memory, chunk.size, (time_t)0, (uint32_t)0);	
						if (rc == MEMCACHED_SUCCESS) 	ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Memcache enable, storing color table %s", 	    colortable_path); 	
						else				ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Couldn't store in Memcache color table %s, %s", colortable_path, memcached_strerror(MEMC_memc, rc) );
					}
				}

		       	        for( token = strtok(chunk.memory, "\n"); token != NULL;  token = strtok(NULL, "\n") ) {
			                if ( ! strstr(token, "ncolors") ) continue;  for (j = 0; j < strlen(token); j++) if (token[j] == '=') { j++; break; }  ncolors = atoi(token+j);
			                break;
				}

			        // Malloc array of colors
			        rgb[0]  = (GByte *)malloc(sizeof(GByte) * ncolors );
			        rgb[1]  = (GByte *)malloc(sizeof(GByte) * ncolors );
			        rgb[2]  = (GByte *)malloc(sizeof(GByte) * ncolors );

			        // Read the color table
		       	        for( ; token != NULL;  token = strtok(NULL, "\n") ) {
					tok = strchr(token, '#' ); if ( tok != NULL ) tok[0] = '\0';
					bzero(value, 99); j = sscanf(token, "%lf %lf %lf %s", &R, &G, &B, value); if ( j < 3 ) continue;

					if ( j == 4 ) {
						if ( dfMax == dfMin ) {
							if ( ( value[0] == '>' ) || ( value[0] == '<' ) ) value++;
							if ( dfMax != atof(value) ) continue;
							range[0] = i_rgb;
						} else {
							if	  ( value[0] == '>' ) {
								v = (unsigned int)( (double)USHRT_MAX / ( dfMax - dfMin ) * ( atof(value+1) - dfMin ) ); 
								if ( v < 0 ) v = 0; 
								if ( v > USHRT_MAX ) 	range[USHRT_MAX] = i_rgb; 
								else 			for ( j = v; j < ( USHRT_MAX + 1 ); j++) range[j] = i_rgb; 
							} else if ( value[0] == '<' ) { 
								v = (unsigned int)( (double)USHRT_MAX / ( dfMax - dfMin ) * ( atof(value+1) - dfMin ) ); 
								if ( v > USHRT_MAX ) v = USHRT_MAX; 
								if ( v < 0 ) 		range[0] = i_rgb; 
								else			for ( j = v; j > -1; j--) range[j] = i_rgb;
							} else { 
								v = (unsigned int)( (double)USHRT_MAX / ( dfMax - dfMin ) * ( atof(value)   - dfMin ) );
								if ( v < 0 ) continue; if ( v > USHRT_MAX ) continue;
								range[v] = i_rgb; 
							}
						}
					}

	                		if (    ( strchr(token, '.') == NULL )  &&
						( (double)(GByte)R == R )       &&
			                        ( (double)(GByte)G == G )       &&
	        		                ( (double)(GByte)B == B )       ){
	
	                		        rgb[0][i_rgb] = (GByte)R;
	                        		rgb[1][i_rgb] = (GByte)G;
			                        rgb[2][i_rgb] = (GByte)B;
			                } else {
			                        rgb[0][i_rgb] = (GByte)(R * 255);
	        		                rgb[1][i_rgb] = (GByte)(G * 255);
			                        rgb[2][i_rgb] = (GByte)(B * 255);
	                		}
	                		i_rgb++;	
	        		}

       		         	if(chunk.memory) free(chunk.memory);
       		         	curl_easy_cleanup(curl);
			}

		} else { 
			if ( colortable != NULL ) { bzero(colortable_path, MAX_PATH_LEN - 1); sprintf(colortable_path, "%s/%s.rgb",             ColorTablesPath, colortable);           CTfp = fopen(colortable_path,   "r"); }
			if ( CTfp == NULL )       { bzero(colortable_path, MAX_PATH_LEN - 1); sprintf(colortable_path, "%s/%s.rgb",             ColorTablesPath, DefaultColorTable);    CTfp = fopen(colortable_path,   "r"); }	
			if ( CTfp != NULL ){

			        // Get number of colors 
				while ( ( read = getline(&buff, &len, CTfp ) ) != -1) {
			                if ( ! strstr(buff, "ncolors") ) continue;
			                for (j = 0; j < strlen(buff); j++) if (buff[j] == '=') { j++; break; }
			                ncolors = atoi(buff+j);
			                break;
	        		}

			        // Malloc array of colors
			        rgb[0]  = (GByte *)malloc(sizeof(GByte) * ncolors );
			        rgb[1]  = (GByte *)malloc(sizeof(GByte) * ncolors );
			        rgb[2]  = (GByte *)malloc(sizeof(GByte) * ncolors );


				// CLASSFIED = TRUE
			        // Read the color table
				while ( ( read = getline(&buff, &len, CTfp ) ) != -1) { 
					tok = strchr(buff, '#' ); if ( tok != NULL ) tok[0] = '\0';
					bzero(value, 99); j = sscanf(buff, "%lf %lf %lf %s", &R, &G, &B, value); if ( j < 3 ) continue;

					if ( j == 4 ){
						if ( dfMax == dfMin ) {
							if ( ( value[0] == '>' ) || ( value[0] == '<' ) ) value++;
							if ( dfMax != atof(value) ) continue;
							range[0] = i_rgb;
						} else {
							if	  ( value[0] == '>' ) {
								v = (unsigned int)( (double)USHRT_MAX / ( dfMax - dfMin ) * ( atof(value+1) - dfMin ) ); 
								if ( v < 0 ) v = 0; 
								if ( v > USHRT_MAX ) 	range[USHRT_MAX] = i_rgb; 
								else 			for ( j = v; j < ( USHRT_MAX + 1 ); j++) range[j] = i_rgb; 
							} else if ( value[0] == '<' ) { 
								v = (unsigned int)( (double)USHRT_MAX / ( dfMax - dfMin ) * ( atof(value+1) - dfMin ) ); 
								if ( v > USHRT_MAX ) v = USHRT_MAX; 
								if ( v < 0 ) 		range[0] = i_rgb; 
								else			for ( j = v; j > -1; j--) range[j] = i_rgb;
							} else { 
								v = (unsigned int)( (double)USHRT_MAX / ( dfMax - dfMin ) * ( atof(value)   - dfMin ) );
								if ( v < 0 ) continue; if ( v > USHRT_MAX ) continue;
								range[v] = i_rgb; 
							}
						}
					}	
					
					if (    ( strchr(buff, '.') == NULL )   &&
	                	                ( (double)(GByte)R == R )       &&
			                        ( (double)(GByte)G == G )       &&
	        		                ( (double)(GByte)B == B )       ){
	
	                		        rgb[0][i_rgb] = (GByte)R;
	                        		rgb[1][i_rgb] = (GByte)G;
			                        rgb[2][i_rgb] = (GByte)B;
			                } else {
			                        rgb[0][i_rgb] = (GByte)(R * 255);
	        		                rgb[1][i_rgb] = (GByte)(G * 255);
			                        rgb[2][i_rgb] = (GByte)(B * 255);
	                		}
	
        	        		i_rgb++;
	        		}	
			        fclose(CTfp);
			}
		}

		
	        if ( ncolors <= 0 	) 				{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to load Color Table");   return 1; }
	        if ( i_rgb   <= 0 	) 				{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to load Color Table");   return 1; }
	        if ( ( CLASSFIED == FALSE ) && ( ncolors != i_rgb ) ) 	{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to load Color Table");   return 1; }
	        if (   CLASSFIED == FALSE ) for(j = 0; j < ( USHRT_MAX + 1 ); j++ ) range[j] = (unsigned int) ( (double)j * ( (double)ncolors / ( USHRT_MAX + 1 ) ) ); 

		if ( isnan( nodata[0] ) != 0 ) { // I have to check if nodata is defined as Nan   
			for ( i = 0; i < ( pxAOISizeX * pxAOISizeY ); i++ ){
	                	if ( raster[i] == raster[i] ) {
					raster[i] = ( (double)USHRT_MAX / ( dfMax - dfMin ) * ( (( raster[i] - offset[0] ) * scale[0]) - dfMin ) );
					if 	( raster[i] < 0 	) { Alpha[i]  = 0; continue; }
					else if ( raster[i] > USHRT_MAX ) { raster[i] = USHRT_MAX;   }
					j 	= range[ (unsigned int)( raster[i] > USHRT_MAX ? USHRT_MAX : raster[i] ) ]; if ( j < 0 ) { Alpha[i]  = 0; continue; }
        	                        Red[i] 	= rgb[0][j]; Green[i] = rgb[1][j]; Blue[i] = rgb[2][j]; Alpha[i] = 255;

        	                } else Alpha[i] = 0;
			}
        	} else {
			for ( i = 0; i < ( pxAOISizeX * pxAOISizeY ); i++ ){
		                if ( raster[i] != nodata[0] ) {
					raster[i] = ( (double)USHRT_MAX / ( dfMax - dfMin ) * ( (( raster[i] - offset[0] ) * scale[0]) - dfMin ) );
					if 	( raster[i] < 0 	) { Alpha[i]  = 0; continue; }
					else if ( raster[i] > USHRT_MAX ) { raster[i] = USHRT_MAX;   }
					j 	= range[ (unsigned int)( raster[i] > USHRT_MAX ? USHRT_MAX : raster[i] ) ]; if ( j < 0 ) { Alpha[i]  = 0; continue; }
        	                        Red[i] 	= rgb[0][j]; Green[i] = rgb[1][j]; Blue[i] = rgb[2][j]; Alpha[i] = 255;

        	                } else Alpha[i] = 0;
			}
        	}



	        free(raster); free(range); free(value);

        	hBandSrc        = GDALGetRasterBand( *hMEMSrcDS, 1 );   GDALSetRasterColorInterpretation(hBandSrc, GCI_RedBand);
        	                                                        err = GDALRasterIO( hBandSrc, GF_Write, 0, 0, pxAOISizeX, pxAOISizeY, Red,    pxAOISizeX, pxAOISizeY, GDT_Byte,  0, 0);

        	hBandSrc        = GDALGetRasterBand( *hMEMSrcDS, 2 );   GDALSetRasterColorInterpretation(hBandSrc, GCI_GreenBand);
        	                                                        err = GDALRasterIO( hBandSrc, GF_Write, 0, 0, pxAOISizeX, pxAOISizeY, Green,  pxAOISizeX, pxAOISizeY, GDT_Byte,  0, 0);

        	hBandSrc        = GDALGetRasterBand( *hMEMSrcDS, 3 );   GDALSetRasterColorInterpretation(hBandSrc, GCI_BlueBand);
        	                                                        err = GDALRasterIO( hBandSrc, GF_Write, 0, 0, pxAOISizeX, pxAOISizeY, Blue,   pxAOISizeX, pxAOISizeY, GDT_Byte,  0, 0);

        	hBandSrc        = GDALGetRasterBand( *hMEMSrcDS, 4 );   GDALSetRasterColorInterpretation(hBandSrc, GCI_AlphaBand);
        	                                                        err = GDALRasterIO( hBandSrc, GF_Write, 0, 0, pxAOISizeX, pxAOISizeY, Alpha,  pxAOISizeX, pxAOISizeY, GDT_Byte,  0, 0);

               	free(Red);              free(Green);            free(Blue);             free(Alpha);

	} else { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to corolorize Dataset"); return 1; }


	return 0;
}



//------------------------------------------------------------------------------------------------


int gridPointIdToSeqnum(int gridPointId) { return gridPointId < 1000000 ? gridPointId 	: gridPointId - 737856 * ((gridPointId - 1) / 1000000) + 1; }
int seqnumToGridPointId(int seqnum)      { return seqnum <= 262145 	? seqnum 	: seqnum == 2621442 ? 9262145 	:  seqnum - 1 + ((seqnum - 2) / 262144) * 737856; }
int seqnumToZoneId(int seqnum)           { return seqnum <= 262145 	? 1 		: seqnum == 2621442 ? 10 	: (seqnum - 2) / 262144 + 1; }


int renderShapeFile( GDALDatasetH  *hSrcDS, struct loadmule *packmule ){ // Fuck fuck fuck fuck ... was not enough of raster ... the shapefile WTF!
	int i, j, k, iField;
	GDALDatasetH 	hDS;
	OGRLayerH 	hLayer;
	OGRFeatureH 	hFeature;
	OGRFeatureDefnH hFDefn;
	OGRFieldDefnH 	hFieldDefn;
	OGRGeometryH	*hGeom	= NULL;
	double		*value 	= NULL;
	block   	cursor          = packmule->cursor;
        struct vsblock 	*shp_head	= cursor->shp_head;
        struct vsblock 	*shp_cursor	= NULL;
	const char 	*auth 		= NULL;
	struct  info 	*info      	= packmule->info; request_rec *r = info->r;

	GDALRasterBandH hBandSrc;
	char   **papszRasterizeOptions  = NULL;
       	int     nTargetBand             = 1;
	struct vsblock **shp_head_sum	= NULL;
	struct vsblock  *shp_cursor_sum	= NULL; 
	struct vsblock  *shp_tmp_sum	= NULL; 
	OGREnvelope sEnvelope;


	double	adfGeoTransform[6], adfInvTransform[6], x, y, z, x1, y1;
	int Xp, Yp; int sum = 0; int sizeX = cursor->sizeX; int sizeY = cursor->sizeY;

	GDALDriverH hMEMDriver = GDALGetDriverByName("MEM");
	papszRasterizeOptions  = CSLSetNameValue( papszRasterizeOptions, "ALL_TOUCHED", "TRUE" );

//	pthread_mutex_lock(&gate);

	hDS = GDALOpenEx( cursor->file, GDAL_OF_VECTOR, NULL, NULL, NULL );	if( hDS    == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: GDALOpenEx error on %s", cursor->file ); 			return FALSE; }
	hLayer = GDALDatasetGetLayerByName( hDS, shp_head->layer );		if( hLayer == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: GDALDatasetGetLayerByName error on %s", shp_head->layer ); 	return FALSE; }
	OGR_L_ResetReading(hLayer);


	memcpy(adfGeoTransform, cursor->GeoTransform, sizeof(double) * 6); // adfGeoTransform[1] *= 20;  adfGeoTransform[5] *= 20;
	if ( GDALInvGeoTransform(adfGeoTransform, adfInvTransform ) == FALSE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: renderShapeFile GDALInvGeoTransform"); return FALSE; }

	if ( ( packmule->x_input_ul == packmule->x_input_lr ) && ( packmule->y_input_ul == packmule->y_input_lr ) ) { sizeX = 1; sizeY = 1; }
	shp_head_sum   = (struct vsblock **)malloc(sizeof(struct vsblock *) * sizeX * sizeY ); 	if ( shp_head_sum == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: renderShapeFile malloc error shp_head_sum"); return FALSE; }
	for ( i =  0; i < (sizeX * sizeY); i++) shp_head_sum[i] = NULL;

	j = 0; i = -1; for ( shp_cursor = shp_head; shp_cursor != NULL; shp_cursor = shp_cursor->next ) { 

		if (i != shp_cursor->feature ) OGR_L_SetNextByIndex(hLayer, shp_cursor->feature); hFeature = OGR_L_GetNextFeature(hLayer);

		if ( hFeature == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: renderShapeFile feature %d not exists", 	shp_cursor->feature ); return FALSE; }
		shp_cursor->hGeom = OGR_G_Clone( OGR_F_GetGeometryRef(hFeature ) );
		auth 		  = OSRGetAttrValue (OGR_G_GetSpatialReference(shp_cursor->hGeom) , "AUTHORITY", 1);

		if ( ( ( auth != NULL ) && ( 4326 != atoi(auth) ) ) || ( auth == NULL ) ) {
		        OGRSpatialReferenceH geoSRS = OSRNewSpatialReference(NULL); 
			#if GDAL_VERSION >= 304
			OSRSetAxisMappingStrategy(geoSRS, OAMS_TRADITIONAL_GIS_ORDER); // GDAL 3.0 Fix
			#endif
		        if ( ImportFromEPSG(&geoSRS, 4326 ) 		  != OGRERR_NONE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: renderShapeFile OSRImportFromEPSG geometry proj incorrect EPSG:4326"); 	return FALSE; }
		        if ( OGR_G_TransformTo(shp_cursor->hGeom, geoSRS) != OGRERR_NONE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: renderShapeFile OGR_G_TransformTo to EPSG:4326"); 				return FALSE; }
			OSRDestroySpatialReference(geoSRS);
		}



		hFDefn = OGR_L_GetLayerDefn(hLayer); iField = OGR_FD_GetFieldIndex( hFDefn, shp_cursor->field );
		if ( iField < 0  ) 	{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: renderShapeFile field %s not exists",	shp_cursor->field   ); return FALSE; }

		hFieldDefn = OGR_FD_GetFieldDefn( hFDefn, iField );

		switch( OGR_Fld_GetType(hFieldDefn) ) {
			case OFTInteger: 	shp_cursor->value = (double)OGR_F_GetFieldAsInteger(   hFeature, iField ); 		break;
	        	case OFTInteger64:	shp_cursor->value = (double)OGR_F_GetFieldAsInteger64( hFeature, iField ); 		break;
	        	case OFTReal:		shp_cursor->value = (double)OGR_F_GetFieldAsDouble(    hFeature, iField ); 		break;
			case OFTString:		shp_cursor->value = (double)atof( OGR_F_GetFieldAsString(  hFeature, iField ) ); 	break;
	        	default:		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: renderShapeFile not supported field %s", shp_cursor->field ); return FALSE;
	    	}


		// OGR_G_GetPoint(shp_cursor->hGeom, 0, &x, &y, &z);
		OGR_G_GetEnvelope(shp_cursor->hGeom, &sEnvelope);

		x = ( sEnvelope.MinX + sEnvelope.MaxX ) / 2.0;
		y = ( sEnvelope.MinY + sEnvelope.MaxY ) / 2.0;

		Xp = (int)floor(  adfInvTransform[0] + x * adfInvTransform[1] + y * adfInvTransform[2] ) + 0.001; if ( Xp >= sizeX ) Xp = sizeX - 1; if ( Xp < 0 ) Xp = 0;
                Yp = (int)floor(  adfInvTransform[3] + x * adfInvTransform[4] + y * adfInvTransform[5] ) + 0.001; if ( Yp >= sizeY ) Yp = sizeY - 1; if ( Yp < 0 ) Yp = 0;
		k  = Xp + sizeX * Yp; 
		if ( shp_head_sum[k] == NULL ) {
			shp_head_sum[k] 	= (struct vsblock *)malloc(sizeof(struct vsblock));  
			shp_head_sum[k]->value	= shp_cursor->value; 
			shp_head_sum[k]->time	= shp_cursor->time; 
			shp_head_sum[k]->cnt 	= 1; 
			shp_head_sum[k]->next	= NULL; 

			if 	( ( OGR_G_GetGeometryType(shp_cursor->hGeom) != wkbPoint ) && 
				  ( OGR_G_Area(shp_cursor->hGeom) <= adfGeoTransform[1]  ) ) {  shp_head_sum[k]->hGeom = OGR_G_CreateGeometry(wkbPoint); OGR_G_SetPoint_2D(shp_head_sum[k]->hGeom, 0, x, y); }
			else									shp_head_sum[k]->hGeom = OGR_G_Clone(shp_cursor->hGeom);

			sum++;
		} else {
			// Move point to balance
			if ( OGR_G_GetGeometryType(shp_head_sum[k]->hGeom) == wkbPoint ) { 
				x1 = OGR_G_GetX(shp_head_sum[k]->hGeom,0); y1 = OGR_G_GetY(shp_head_sum[k]->hGeom,0);
				OGR_G_SetPoint(shp_head_sum[k]->hGeom, 0, ( x1 + x ) / 2.0, ( y1 + y ) / 2.0, 0);
			}
						

			for ( shp_cursor_sum = shp_tmp_sum = shp_head_sum[k]; shp_cursor_sum != NULL; shp_tmp_sum = shp_cursor_sum, shp_cursor_sum = shp_cursor_sum->next ) if ( shp_cursor_sum->time == shp_cursor->time ) break;
			if ( shp_cursor_sum == NULL ) { 
				shp_tmp_sum->next 	= (struct vsblock *)malloc(sizeof(struct vsblock));  	
				shp_cursor_sum 		= shp_tmp_sum->next; 
				shp_cursor_sum->value  	= shp_cursor->value;
				shp_cursor_sum->time   	= shp_cursor->time;
				shp_cursor_sum->cnt 	= 1;
				shp_cursor_sum->next 	= NULL;			
			} else {
				shp_cursor_sum->value += shp_cursor->value; shp_cursor_sum->cnt++;
			
			}

		
		}
		j++; i = shp_cursor->feature;
	}

	GDALClose(hDS);
//	pthread_mutex_unlock(&gate);

	if ( j !=  cursor->shp_num ) return FALSE;

	hGeom = (OGRGeometryH *)malloc(sizeof(OGRGeometryH) * sum);
	value = (double       *)malloc(sizeof(double) 	    * sum);

	while (( cursor->shp_cursor = cursor->shp_head ) != NULL) { cursor->shp_head = cursor->shp_head->next; OGR_G_DestroyGeometry(cursor->shp_cursor->hGeom); free(cursor->shp_cursor); } cursor->shp_head = NULL;

	for ( i =  0, j = 0; i < (sizeX * sizeY); i++){
		if ( shp_head_sum[i] == NULL ) continue;

		if ( OGR_G_GetGeometryType(shp_head_sum[i]->hGeom) != wkbPoint ) 	hGeom[j] = OGR_G_Simplify(	shp_head_sum[i]->hGeom, adfGeoTransform[1] );
		else									hGeom[j] = OGR_G_Buffer(	shp_head_sum[i]->hGeom, adfGeoTransform[1], 4);

		for ( shp_cursor_sum = shp_head_sum[i], value[j] = 0.0, k = 0; shp_cursor_sum != NULL; shp_cursor_sum = shp_cursor_sum->next ){ 
	
			
                        if ( cursor->shp_head == NULL ) { cursor->shp_head              = cursor->shp_cursor =  (struct vsblock *)malloc(sizeof(struct vsblock)); 							}
                        else                            { cursor->shp_cursor->next      = 			(struct vsblock *)malloc(sizeof(struct vsblock)); cursor->shp_cursor = cursor->shp_cursor->next;	}

			cursor->shp_cursor->hGeom       = OGR_G_Clone(shp_head_sum[i]->hGeom); 
			cursor->shp_cursor->feature	= j;
        		cursor->shp_cursor->time    	= shp_cursor_sum->time;
			cursor->shp_cursor->cnt 	= shp_cursor_sum->cnt;
			cursor->shp_cursor->value 	= shp_cursor_sum->value / (double)shp_cursor_sum->cnt;
                        cursor->shp_cursor->next   	= NULL;


			value[j] += cursor->shp_cursor->value; k++;
			if ( shp_cursor_sum->next == NULL ) break;
		
		
		}

		value[j] /= (double)k; j++;
		OGR_G_DestroyGeometry(shp_head_sum[i]->hGeom); while (( shp_cursor_sum = shp_head_sum[i] ) != NULL) { shp_head_sum[i] = shp_head_sum[i]->next;  free(shp_cursor_sum); } shp_head_sum[i] = NULL;
	}
	
//	if ( packmule->outType != IMAGE ) 	return TRUE; 
		
	

	if ( ( (*hSrcDS)  = GDALCreate( hMEMDriver, "", sizeX, sizeY, 1, cursor->type, NULL ) ) == NULL ) return FALSE;
        GDALSetGeoTransform( 	(*hSrcDS), cursor->GeoTransform);
	GDALSetProjection(	(*hSrcDS), cursor->wkt);

	hBandSrc = GDALGetRasterBand((*hSrcDS), 1 ); GDALSetRasterNoDataValue(hBandSrc, -9999 ); GDALFillRaster(hBandSrc, -9999, 0);

	if ( GDALRasterizeGeometries( (*hSrcDS), 1, &nTargetBand, sum, hGeom, NULL, NULL, value, papszRasterizeOptions, NULL, NULL ) != CE_None ) 
			{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: renderShapeFile RasterizeGeometries Fail!"); return FALSE; }

	
	return TRUE;

}

/*
int readSMOSzip( GDALDatasetH  *hSrcDS, struct loadmule *packmule ){ // Ok man ... here we can try to touch the heaven
	
	char	tmp[120];

	int 	err;
	int 	i,j,k,z;
	int	len		= 0;
	long long sum		= 0;
	int	offset		= 0;
	int     DSR_Size        = 0;
	double	Chi_2_Scale	= 0.0;
	struct 	zip_stat 	sb;
	struct 	zip 		*za;
	struct 	zip_file 	*zf;
	int 	subDataSet	= packmule->subDataSet;
	struct  info *info      = packmule->info; request_rec *r = info->r;
	block   cursor          = packmule->cursor;
	char	*archive	= cursor->file;
	zip_error_t 		error;
	zip_source_t 		*src;

	char 	*ptr;
	int	*raster	= NULL;
	float	*smos	= NULL;
	char 	*buf	= NULL;
	unsigned char *subZip	= NULL;
	int	HDR_i	= 0;
	int	HDR_s	= 0;
	int	DBL_i	= 0;
	int	DBL_s	= 0;
	int	ZIP_i	= 0;
	int	ZIP_s	= 0;



	if ((za = zip_open(archive, 0, &err)) == NULL) { zip_error_to_str(buf, 120, err, errno); ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Can't open zip archive %s: %s!", archive, buf); return FALSE; }

	GDALDatasetH  		hDataset;
	GDALRasterBandH         hBandSrc;
	CPLErr          	errcazzo;
	GDALDriverH             hMEMDriver         = GDALGetDriverByName("MEM");
	double			adfGeoTransform[6] = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
	double			adfInvTransform[6] = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
	double			Xp_ul, Yp_ul;
	int			sizex, sizey;

	hDataset = GDALOpen(earth4h9, GA_ReadOnly);
	sizex	 = GDALGetRasterXSize( hDataset ); 
	sizey	 = GDALGetRasterYSize( hDataset );
	GDALGetGeoTransform( hDataset, adfGeoTransform );
	if( hDataset == NULL ) 									 { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to open 4H9 %s!", earth4h9); 			return FALSE; }
	if ( ( raster = (int *)malloc( sizeof(int) * cursor->sizeX * cursor->sizeY ) ) == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to malloc readSMOSzip!"); GDALClose(hDataset);	return FALSE; }
	if ( GDALInvGeoTransform(adfGeoTransform,    adfInvTransform ) == FALSE )		 { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: GDALInvGeoTransform readSMOSzip"); GDALClose(hDataset);  return FALSE; }

	Xp_ul = adfInvTransform[0] + cursor->GeoTransform[0] * adfInvTransform[1] + cursor->GeoTransform[3] * adfInvTransform[2];
	Yp_ul = adfInvTransform[3] + cursor->GeoTransform[0] * adfInvTransform[4] + cursor->GeoTransform[3] * adfInvTransform[5];

	hBandSrc = GDALGetRasterBand( hDataset, 1 );
	errcazzo = GDALRasterIO( hBandSrc, GF_Read,  (int)Xp_ul, (int)Yp_ul, cursor->offsetX, cursor->offsetY, raster, cursor->sizeX, cursor->sizeY, GDT_Int32, 0, 0);
	GDALClose(hDataset);

	for (i = 0; i < zip_get_num_entries(za, 0); i++) if ( (zip_stat_index(za, i, 0, &sb) == 0) ) {
		if 	( ! strcmp( get_filename_ext(sb.name), "HDR" )) { HDR_i = i; HDR_s = sb.size; } // fprintf(stderr, "Name: [%s]\n", sb.name); fflush(stderr); }
		else if ( ! strcmp( get_filename_ext(sb.name), "DBL" )) { DBL_i = i; DBL_s = sb.size; } // fprintf(stderr, "Name: [%s]\n", sb.name); fflush(stderr); }	
		else if ( ! strcmp( get_filename_ext(sb.name), "zip" )) { ZIP_i = i; ZIP_s = sb.size; } // fprintf(stderr, "Name: [%s]\n", sb.name); fflush(stderr); }	
	}

	if ( ZIP_s != 0 ) { // Son of a dirty bitch! A ZIP IN A ZIP! WTF!?!?

		zf 	= zip_fopen_index(za, ZIP_i, 0); 		if (!zf) 		return FALSE;
		subZip 	= (unsigned char *)malloc(ZIP_s);
		len 	= zip_fread(zf, (void *)subZip, ZIP_s); 	if ( len != ZIP_s ) 	return FALSE;
		zip_fclose(zf);
		if (( src = zip_source_buffer_create(subZip, ZIP_s, 1, &error)) 	== NULL ) return FALSE;
		if (( za  = zip_open_from_source(src, 0, &error))			== NULL ) return FALSE;

		for (i = 0; i < zip_get_num_entries(za, 0); i++) if ( (zip_stat_index(za, i, 0, &sb) == 0) ) {
			if 	( ! strcmp( get_filename_ext(sb.name), "HDR" )) { HDR_i = i; HDR_s = sb.size; } 
			else if ( ! strcmp( get_filename_ext(sb.name), "DBL" )) { DBL_i = i; DBL_s = sb.size; } 	
		}

	} 

	// Read XML file 
	zf 	 = zip_fopen_index(za, HDR_i, 0);  if (!zf) return FALSE; 
	sum 	 = 0;
	DSR_Size = 0;
	buf 	 = malloc(HDR_s);
	len 	 = zip_fread(zf, buf, HDR_s); if ( len != HDR_s ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Can't read SMOS header of zip archive %s!", archive ); return FALSE; }
	zip_fclose(zf);

	char * line = strtok(strdup(buf), "\n"); while(line) {
                if ( ( ptr = strstr(line, "<File_Type>") ) && ( offset == 0 ) ) {
			if 	( ptr[15]== 'S' ) offset = 28;
			else			  offset = 20;
		}

                if ( ( ptr = strstr(line, "<DSR_Size>") ) && ( DSR_Size == 0 ) ){
			sprintf(tmp, "%.*s", 8, ptr + 10 );
			DSR_Size = atoi(tmp);
		}
		
                if ( ( ptr = strstr(line, "<Chi_2_Scale>") ) && ( Chi_2_Scale == 0 ) ){
			sprintf(tmp, "%.*s", 12, ptr + 13 );
			Chi_2_Scale = atof(tmp);
		}

		if ( ( DSR_Size != 0 ) && ( offset != 0 ) ) break;
		line  = strtok(NULL, "\n");
	}

	if ( offset   == 0 ) return FALSE;
	if ( DSR_Size == 0 ) return FALSE;


	unsigned int 	N_Grid_Points 	= 0;
	DSMOS      	*DSlist		= NULL;
	unsigned char	*buffer		= NULL;
	unsigned char   *DSR_ptr	= NULL;
	int		size		= 0;
	int		extra_offset	= 0;
	int		Grid_Point_ID_lis[10];
	int	       *Grid_Point_ID_lut[10];
	int		Grid_Point_ID_min[10];
	int		Grid_Point_ID_max[10]; for (i = 0; i < 10 ; i++) { Grid_Point_ID_min[i] = INT_MAX; Grid_Point_ID_max[i] = 0; Grid_Point_ID_lis[i] = FALSE; Grid_Point_ID_lut[i] = NULL; }

	zf 	= zip_fopen_index(za, DBL_i, 0);	if (!zf) return FALSE;
	len 	= zip_fread(zf, &N_Grid_Points, 4); 	if ( ( DSlist = (DSMOS *)malloc(sizeof(DSMOS) * N_Grid_Points) ) == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Malloc error DSlist\n"); return FALSE; }
	buffer 	= (unsigned char *)malloc( DSR_Size 	* N_Grid_Points );
	len 	= zip_fread(zf, buffer, DSR_Size 	* N_Grid_Points );


	for ( j = 0, DSR_ptr = buffer; j < N_Grid_Points ; j++, DSR_ptr += DSR_Size ){
		memcpy(&DSlist[j].Grid_Point_ID, 	DSR_ptr  + 0, 			4); DSlist[j].Seqnum = gridPointIdToSeqnum( (int)DSlist[j].Grid_Point_ID); if ( DSlist[j].Seqnum < 1 ) { DSlist[j].data = -999.0; continue; }
		memcpy(&DSlist[j].data,          	DSR_ptr  + offset,     		4);
		memcpy(&DSlist[j].ScienceFlags,         DSR_ptr  + offset + 169,        4);
		memcpy(&DSlist[j].Chi_2,         	DSR_ptr  + offset + 131,        1);

		// TB_TOA_Theta_B_V_DQX DSR_ptr  + offset + 124 
		// Confidence_Flags 	DSR_ptr  + offset + 124 + 4
		// GQX			DSR_ptr  + offset + 124 + 4 + 2
		// Chi_2		DSR_ptr  + offset + 124 + 4 + 2 + 1

		z = seqnumToZoneId(DSlist[j].Seqnum) - 1; 		if ( z >= 10 ) { DSlist[j].Seqnum = 0; DSlist[j].data = -999.0; DSlist[j].ScienceFlags = 0; DSlist[j].Chi_2 = 0; continue; }
									Grid_Point_ID_lis[z] = TRUE;
		if ( DSlist[j].Grid_Point_ID > Grid_Point_ID_max[z] ) 	Grid_Point_ID_max[z] = DSlist[j].Grid_Point_ID;
		if ( DSlist[j].Grid_Point_ID < Grid_Point_ID_min[z] ) 	Grid_Point_ID_min[z] = DSlist[j].Grid_Point_ID;
	}

	zip_fclose(zf); free(buffer);

	if ( zip_close(za) == -1 ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Can't close zip archive %s!", archive); return FALSE; }

	if ( ( smos = (float *)malloc( sizeof(float) * cursor->sizeX * cursor->sizeY ) ) == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to malloc readSMOSzip!"); return FALSE; }
	if ( ( (*hSrcDS)  = GDALCreate( hMEMDriver, "", cursor->sizeX, cursor->sizeY, cursor->nband, GDT_Float32, NULL ) ) == NULL ) return FALSE;

	for (i = 0; i < 10 ; i++){
		if ( Grid_Point_ID_lis[i] != TRUE ) continue;
		size = Grid_Point_ID_max[i] - Grid_Point_ID_min[i] + 1;
		Grid_Point_ID_lut[i] = (int *)malloc(sizeof(int) * size);
	}

	for ( i = 0; i < N_Grid_Points ; i++){
		if ( DSlist[i].Seqnum < 1 ) continue;
		z 			= seqnumToZoneId(DSlist[i].Seqnum) - 1;
		j 			= DSlist[i].Grid_Point_ID - Grid_Point_ID_min[z];
		Grid_Point_ID_lut[z][j] = i;
	}
	

	if ( subDataSet == 3 ) 		for ( j = 0; j < ( cursor->sizeX * cursor->sizeY ); j++ ) {
		smos[j] = -999;
		z = seqnumToZoneId(raster[j]) - 1; 	if ( Grid_Point_ID_lis[z] != TRUE ) 	continue;
		i = seqnumToGridPointId(raster[j]); 	if ( i < Grid_Point_ID_min[z] )		continue;
							if ( i > Grid_Point_ID_max[z] )         continue;
		k = Grid_Point_ID_lut[z][ i - Grid_Point_ID_min[z] ]; 
		if ( k 			>= N_Grid_Points ) continue;
		if ( DSlist[k].Seqnum	!= raster[j]	 ) continue;
		if ( DSlist[k].Chi_2	== 0 		 ) continue;
		smos[j] = ( (float)DSlist[k].Chi_2 * Chi_2_Scale ) / 255.0;

	} else if ( subDataSet == 2 ) 	for ( j = 0; j < ( cursor->sizeX * cursor->sizeY ); j++ ) {
		smos[j] = -999;
		z = seqnumToZoneId(raster[j]) - 1; 	if ( Grid_Point_ID_lis[z] != TRUE ) 	continue;
		i = seqnumToGridPointId(raster[j]); 	if ( i < Grid_Point_ID_min[z] )		continue;
							if ( i > Grid_Point_ID_max[z] )         continue;
		k = Grid_Point_ID_lut[z][ i - Grid_Point_ID_min[z] ]; 
		if ( k 			>= N_Grid_Points ) continue;
		if ( DSlist[k].Seqnum	!= raster[j]	 ) continue;
		smos[j] = (float)DSlist[k].ScienceFlags;

	} else  			for ( j = 0; j < ( cursor->sizeX * cursor->sizeY ); j++ ) {
		smos[j] = -999;
		z = seqnumToZoneId(raster[j]) - 1; 	if ( Grid_Point_ID_lis[z] != TRUE ) 	continue;
		i = seqnumToGridPointId(raster[j]); 	if ( i < Grid_Point_ID_min[z] )		continue;
							if ( i > Grid_Point_ID_max[z] )         continue;
		k = Grid_Point_ID_lut[z][ i - Grid_Point_ID_min[z] ]; 
		if ( k 			>= N_Grid_Points ) continue;
		if ( DSlist[k].Seqnum	!= raster[j]	 ) continue;
		smos[j] = (float)DSlist[k].data;
	}	



        hBandSrc = GDALGetRasterBand( (*hSrcDS), 1 );
	GDALSetRasterNoDataValue(hBandSrc, -999.0 );
        errcazzo = GDALRasterIO( hBandSrc, GF_Write,  0, 0, cursor->sizeX, cursor->sizeY, smos, cursor->sizeX, cursor->sizeY, GDT_Float32, 0, 0);
	cursor->upX = cursor->upY = 0;
	cursor->offsetX	= cursor->sizeX;
	cursor->offsetY = cursor->sizeY;

	for (i = 0; i < 10 ; i++) if ( Grid_Point_ID_lut[i] != NULL ) free(Grid_Point_ID_lut[i]);
	if (smos   != NULL) free(smos);
	if (raster != NULL) free(raster);
	if (DSlist != NULL) free(DSlist); 
	return TRUE;
}
*/

//------------------------------------------------------------------------------------------------




void *imagepackmule(void *data){
	// Support varibles
	GDALDatasetH    	hSrcDS, 	hDstDS;	
	GDALRasterBandH         hBandSrc;
	GDALDatasetH            hMEMDstDS;
	int			i			= 0;
	int			j			= 0;
	double			nodata			= 0.0; // DEFAULT_NODATA;
	double			scale			= 1.0;
	double			offset			= 0.0;
        double  		Xp_ul                   = 0.0;
        double  		Yp_ul                   = 0.0;
	double          	*raster         	= NULL;
	GDALDriverH     	hMEMDriver		= GDALGetDriverByName("MEM");
	int     		GCPCount        	= 0;
	GDAL_GCP 		*GCPList        	= NULL;
	GDAL_GCP 		*GCPList_filter        	= NULL;
	char            	*pszGCPProjection       = NULL;
	double  		adfInvTransform[6]      = { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 };
	int     		pbSuccess               = 0;
	GDALWarpOptions         *psWarpOptions          = NULL;
	GDALWarpOperationH      *oOperation;

	// Needs variable from outsite
	struct loadmule *packmule 		= (struct loadmule *)data;
	int		id			= packmule->id;
	block 		cursor 			= packmule->cursor;
	struct 		info *info		= packmule->info; request_rec *r = info->r;
        double  	x_input_ul              = packmule->x_input_ul;
	double  	y_input_ul              = packmule->y_input_ul;
        double  	x_input_lr              = packmule->x_input_lr;
	double  	y_input_lr              = packmule->y_input_lr;
	int             epgs_out        	= packmule->epgs_out;
        double  	GeoX_ul                 = packmule->GeoX_ul;
        double  	GeoY_ul                 = packmule->GeoY_ul;
	int 		pxAOISizeX		= packmule->pxAOISizeX;
	int		pxAOISizeY 		= packmule->pxAOISizeY;
	char           	*wktTargetSRS		= packmule->wktTargetSRS;
	int		FILTER			= packmule->FILTER;
	int		WMTS_MODE		= packmule->WMTS_MODE;

	int		readScale		= TRUE;
	int		readOffset		= TRUE;
	char 		*name			= NULL;
	char		*colon 			= NULL;
	CPLErr 		err;
	char 		pszFilename[50];

	int (*mod)( GDALDatasetH  *, struct loadmule *); // Proto func for module


	if ( ( info->ENVIRONMENT_VARIABLE[0] != NULL ) && ( info->ENVIRONMENT_VARIABLE[1] != NULL ) )
		for ( i = 0; i < MAX_D; i++) if ( ( info->ENVIRONMENT_VARIABLE[0][i] != NULL ) && ( info->ENVIRONMENT_VARIABLE[1][i] != NULL ) ) setenv(info->ENVIRONMENT_VARIABLE[0][i], info->ENVIRONMENT_VARIABLE[1][i],     1);

	// First open dataset or subdataset or whatever ...
	// for remote dataset i try to not use mutex ... mah ... i hope that it works
	if ( cursor->vrt != NULL ) { 
		bzero(pszFilename, 49); snprintf(pszFilename, 50, "/vsimem/live_long_and_prosper-%d-%d.vrt", r->connection->client_addr->port, id);
		pthread_mutex_lock(&gate);


		VSILFILE *vrt = NULL;
	       	vrt = VSIFileFromMemBuffer( pszFilename, cursor->vrt, strlen( cursor->vrt ), FALSE ); 

		if ( vrt == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: VSIFileFromMemBuffer error write VRT in  memory %s", pszFilename ); cursor->file[0] = '\0'; pthread_exit( (void *)1 ); } 
		else 		   VSIFCloseL(vrt);

		if ( (hSrcDS = GDALOpen( pszFilename, GA_ReadOnly )) == NULL ) {
		       	ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: GDALOpen error on VRT from memory %s %s", pszFilename, CPLGetLastErrorMsg()  );
			cursor->file[0] = '\0';
			free(cursor->vrt); VSIUnlink(pszFilename);
			pthread_mutex_unlock(&gate);
			pthread_exit( (void *)1 );
		}

		pthread_mutex_unlock(&gate);

	} else if ( ( info->module != NULL) && ( cursor->file     != NULL) && ( ( mod = use_module_by_name(info->module,  NULL) ) != NULL ) ) { if ( (*mod)(  &hSrcDS, packmule)          == FALSE ) { cursor->file[0] = '\0';  pthread_exit( (void *)1 ); } // module by name
	} else if ( ( cursor->file     != NULL ) && 			      ( ( mod = use_module_by_regex(cursor->file, NULL) ) != NULL ) ) { if ( (*mod)(  &hSrcDS, packmule)          == FALSE ) { cursor->file[0] = '\0';  pthread_exit( (void *)1 ); } // module by regex
	} else if (   cursor->shp_head != NULL ) 								    { if ( renderShapeFile( &hSrcDS, packmule)  == FALSE ) { cursor->file[0] = '\0';  pthread_exit( (void *)1 ); } // ShapeFile extension
	} else { // Default condition
		pthread_mutex_lock(&gate);
		if ( (hSrcDS = GDALOpen( cursor->file, GA_ReadOnly )) == NULL ) {
		       	ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: GDALOpen error on %s %s", cursor->file, CPLGetLastErrorMsg() );
			cursor->file[0] = '\0';
			pthread_mutex_unlock(&gate);
			pthread_exit( (void *)1 );
		}
		pthread_mutex_unlock(&gate);
	}

	//---- Clone metedata info 
	char            **papszMetadata = NULL;
	papszMetadata   = GDALGetMetadata(hSrcDS, NULL);
	if ( ( papszMetadata != NULL ) && ( ( cursor->MetadataCount = CSLCount(papszMetadata) ) > 0 ) ) {
		int k = 0; cursor->Metadata = (char **)malloc(sizeof( char *) * cursor->MetadataCount ); 

		for( i = 0, k = 0; i < cursor->MetadataCount ; i++ ){
			cursor->Metadata[k] = NULL; j = strlen(papszMetadata[i]); 	if ( j <= 0 ) continue; 
			cursor->Metadata[k] = (char *)malloc(j+1); 			if ( cursor->Metadata[k] == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "WARN: malloc on cursor->Metadata[%d]", i ); continue; }	
			bzero(cursor->Metadata[k], j); 	memcpy(cursor->Metadata[k], papszMetadata[i], j ); cursor->Metadata[k][j] = '\0'; k++;
		} cursor->MetadataCount = k;
		if ( k == 0 ) { free(cursor->Metadata); cursor->Metadata = NULL; cursor->MetadataCount = 0; }
		else	      { cursor->MetadataCount = k; }

	} else { cursor->Metadata = NULL; cursor->MetadataCount = 0; }
	//---

	// Prepare the memory dataset to import from disk 
	if ( ( cursor->dataset  = GDALCreate( hMEMDriver, "", cursor->sizeX, cursor->sizeY, cursor->nband, GDT_Float64, NULL ) ) == NULL ) 
			{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: GDALCreate %dx%d", cursor->sizeX, cursor->sizeY ); pthread_exit( (void *)2 ); }


	// Setting geo info to memory destination
	if ( cursor->wkt != NULL ) GDALSetProjection(	cursor->dataset, cursor->wkt );
			   	   GDALSetGeoTransform(	cursor->dataset, cursor->GeoTransform);

	// hSrcDS source image, cursor->dataset in memory image	   
	GCPCount = GDALGetGCPCount(hSrcDS);
	if ( GCPCount > 0 ) { GCPList	= ( GDAL_GCP *)GDALGetGCPs(hSrcDS); GDALSetGCPs(cursor->dataset, GCPCount, GCPList, pszGCPProjection ); 	}
	else 	            { GCPCount  = GDALGetGCPCount(cursor->dataset); if ( GCPCount > 0 ) GCPList = ( GDAL_GCP *)GDALGetGCPs(cursor->dataset); 	}


	if ( ( GCPList != NULL ) && ( GCPCount > 0 ) ){
		if ( ( cursor->upX > 0 ) || (  cursor->upY > 0 ) ) {
			for (i = 0, j = 0 ; i < GCPCount; i++ ) { // First I have to count
				Xp_ul = GCPList[i].dfGCPPixel - cursor->upX; if ( Xp_ul < 0 ) 	 continue; 
				Yp_ul = GCPList[i].dfGCPLine  - cursor->upY; if ( Yp_ul < 0 ) 	 continue;
				if ( ( cursor->upX + cursor->offsetX ) < GCPList[i].dfGCPPixel ) continue;
				if ( ( cursor->upY + cursor->offsetY ) < GCPList[i].dfGCPLine )  continue;
				j++;
			}
			if ( j > 0 ) {
				GCPList_filter = ( GDAL_GCP *)malloc( sizeof(GDAL_GCP) * j );

				for (i = 0, j = 0 ; i < GCPCount; i++ ) { // First I have to count
					Xp_ul = GCPList[i].dfGCPPixel - cursor->upX; if ( Xp_ul < 0 ) 	 continue; 
					Yp_ul = GCPList[i].dfGCPLine  - cursor->upY; if ( Yp_ul < 0 ) 	 continue;
					if ( ( cursor->upX + cursor->offsetX ) < GCPList[i].dfGCPPixel ) continue;
					if ( ( cursor->upY + cursor->offsetY ) < GCPList[i].dfGCPLine )  continue;

					GCPList_filter[j].pszId		= malloc( strlen(GCPList[i].pszId)   + 1 ); strcpy(GCPList_filter[j].pszId,   GCPList[i].pszId );
					GCPList_filter[j].pszInfo      	= malloc( strlen(GCPList[i].pszInfo) + 1 ); strcpy(GCPList_filter[j].pszInfo, GCPList[i].pszInfo);
					GCPList_filter[j].dfGCPPixel 	= Xp_ul; 	
					GCPList_filter[j].dfGCPLine	= Yp_ul;
					GCPList_filter[j].dfGCPX       	= GCPList[i].dfGCPX;
					GCPList_filter[j].dfGCPY       	= GCPList[i].dfGCPY;
					GCPList_filter[j].dfGCPZ	= GCPList[i].dfGCPZ;	
					j++;
				}

			}
			cursor->gcps_cnt = j;
			cursor->gcps	 = GDALDuplicateGCPs(j, GCPList_filter);

		} else {
			cursor->gcps_cnt = GCPCount;
			cursor->gcps	 = GDALDuplicateGCPs(GCPCount, GCPList);
		}
	} else { cursor->gcps = NULL; cursor->gcps_cnt = 0; }

	// Malloc memory for next steps 
	if ( ( raster  	= (double *)malloc( sizeof(double) * cursor->sizeX * cursor->sizeY ) )  == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc %dx%d", 	 cursor->sizeX, cursor->sizeY ); pthread_exit( (void *)2 ); }


					cursor->nodata = (double *)malloc(sizeof(double)  * cursor->nband);
	if ( cursor->scale  == NULL ) { cursor->scale  = (double *)malloc(sizeof(double)  * cursor->nband); readScale  = TRUE; } else readScale  = FALSE;
	if ( cursor->offset == NULL ) { cursor->offset = (double *)malloc(sizeof(double)  * cursor->nband); readOffset = TRUE; } else readOffset = FALSE;

	GDALRasterIOExtraArg psExtraArg; INIT_RASTERIO_EXTRA_ARG(psExtraArg); psExtraArg.eResampleAlg = GRIORA_NearestNeighbour; // GRIORA_Bilinear;  // GRIORA_CubicSpline; // GRIORA_Average; //GRIORA_NearestNeighbour; 


	double dfMinStat = 0.0, dfMaxStat = 0.0, dfMean = 0.0, dfStdDev = 0.0; CPLErr eErr; 
	int overview_cnt = 0, overview_target = -1, w; GDALRasterBandH hBandOW; int oo_xsize, oo_ysize;
	int overview_sta = OVERVIEW_NO; 


	for (i = 0, j = 0; i < cursor->nband; i++ ){
		// Read from file
		hBandSrc 	= GDALGetRasterBand( hSrcDS,   cursor->tband[i]+1 );
		nodata	 	= GDALGetRasterNoDataValue(	hBandSrc, &pbSuccess); 			if( ! pbSuccess ) nodata = 0; cursor->nodata[i] = nodata;
		if ( readScale  == TRUE ) { scale    = GDALGetRasterScale(  hBandSrc, &pbSuccess); 	if( ! pbSuccess ) scale  = 1; } else scale  = cursor->scale[i];  cursor->scale[i]  = ( scale > 1.0 ) ? 1.0 / scale : scale;
		if ( readOffset == TRUE ) { offset   = GDALGetRasterOffset( hBandSrc, &pbSuccess); 	if( ! pbSuccess ) offset = 0; } else offset = cursor->offset[i]; cursor->offset[i] = offset;

		if ( ( overview_sta == OVERVIEW_NO ) && ( WMTS_MODE = TRUE ) &&  ( ( cursor->GeoTransform[1] / cursor->srcGeoTransform[1] ) > 1.0 ) ) {
			overview_cnt = GDALGetOverviewCount(hBandSrc);
			if ( overview_cnt > 0 ) {
				overview_sta 	= OVERVIEW_YES;
				for ( w = overview_cnt - 1; w != -1; w--){ 
					hBandOW 	= GDALGetOverview(hBandSrc, w);  
					oo_xsize 	= GDALGetRasterBandXSize( hBandOW );
					oo_ysize	= GDALGetRasterBandYSize( hBandOW );
					if ( ( (double)cursor->srcSizeX / (double)oo_xsize ) <= ( cursor->GeoTransform[1] / cursor->srcGeoTransform[1] ) ) break; 
				}

				overview_target  = ( w < 0 ) ? 0 : w;
				hBandSrc	 = hBandOW;
				cursor->upX 	 = (int)( (double)oo_xsize / (double)cursor->srcSizeX * (double)cursor->upX );
				cursor->upY 	 = (int)( (double)oo_ysize / (double)cursor->srcSizeY * (double)cursor->upY );
				cursor->offsetX  = (int)( (double)oo_xsize / (double)cursor->srcSizeX * (double)cursor->offsetX );
				cursor->offsetY  = (int)( (double)oo_ysize / (double)cursor->srcSizeY * (double)cursor->offsetY );
				cursor->srcSizeX = oo_xsize;
				cursor->srcSizeY = oo_ysize;

			
			}
		} else if ( ( overview_sta == OVERVIEW_YES ) && ( overview_target >= 0 ) )  hBandSrc = GDALGetOverview(hBandSrc, overview_target );
									err = GDALRasterIOEx( hBandSrc, GF_Read,  cursor->upX,  cursor->upY, 	cursor->offsetX, cursor->offsetY, raster, cursor->sizeX, cursor->sizeY, GDT_Float64, 0, 0, &psExtraArg);
	 	hBandSrc = GDALGetRasterBand( cursor->dataset, i+1 );	err = GDALRasterIOEx( hBandSrc, GF_Write, 0, 		0, 		cursor->sizeX,   cursor->sizeY,   raster, cursor->sizeX, cursor->sizeY, GDT_Float64, 0, 0, &psExtraArg);


		if ( FILTER == TRUE ){
			// Check the content			
			// In case is a single pixel (pixel history)
			if (( cursor->sizeX == 1 ) && (  cursor->sizeY == 1 ) ) { 	if ( raster[0] == cursor->nodata[i]  ) j++; }
			else 							{ 
											
											eErr = GDALGetRasterStatistics( hBandSrc, FALSE, TRUE, &dfMinStat, &dfMaxStat, &dfMean, &dfStdDev );
											if( eErr != CE_None ) j++;
											if ( dfMinStat < cursor->MinMax[0] ) cursor->MinMax[0] = dfMinStat;
											if ( dfMaxStat > cursor->MinMax[1] ) cursor->MinMax[1] = dfMaxStat;
										}
		}  

		// fprintf(stderr, "%s\n", cursor->file ); fflush(stderr);
		// Setting nodata, scale, offset in memeory dataset
		GDALSetRasterNoDataValue(hBandSrc, cursor->nodata[i]);
		GDALSetRasterScale	(hBandSrc, cursor->scale[i] );
		GDALSetRasterOffset	(hBandSrc, cursor->offset[i]);
	}

	// Close file on disk and clean the memory
	GDALClose(hSrcDS); 	hSrcDS = NULL; 
	free(raster);		raster = NULL;

	// Clean cache
	VSICurlClearCache();

	// Delete VRT file in memory
	if ( cursor->vrt != NULL ) { free(cursor->vrt); VSIUnlink(pszFilename); }


	// In case all bands are filled of shit ... remove this image
	if ( ( FILTER == TRUE ) && ( j == cursor->nband ) ) { cursor->file[0] = '\0'; pthread_exit( (void *)1 );	}

	// Well well well ... now I have read image ... i need to warp it? 
	if ( ( x_input_ul == x_input_lr ) && ( y_input_ul == y_input_lr ) ) pthread_exit( (void *)0 );

	// If output is an AOI I must warp image in output proj	
	// But i perform reproj only if in proj is != the output proj
	if (( cursor->warp == TRUE ) || ( cursor->epsg != epgs_out )) {
		// Prepare output for warp
		if ( ( hMEMDstDS = GDALCreate( hMEMDriver, "", cursor->nPixels, cursor->nLines, cursor->nband, GDT_Float64, NULL ) ) == NULL ) 
			{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: GDALCreate %dx%dx%d", cursor->nPixels, cursor->nLines, cursor->nband ); pthread_exit( (void *)2 ); }
		GDALSetProjection(   hMEMDstDS, wktTargetSRS );
		GDALSetGeoTransform( hMEMDstDS, cursor->adfDstGeoTransform );

		// Prepare options for warping 
		psWarpOptions 				= GDALCreateWarpOptions();
		psWarpOptions->hSrcDS 			= cursor->dataset;
		psWarpOptions->hDstDS 			= hMEMDstDS;
		psWarpOptions->nBandCount 		= cursor->nband; 
		
		psWarpOptions->panSrcBands		= (int *) CPLMalloc(sizeof(int) * psWarpOptions->nBandCount );
		psWarpOptions->panDstBands		= (int *) CPLMalloc(sizeof(int) * psWarpOptions->nBandCount );

		for (i = 0; i < psWarpOptions->nBandCount; i++ ) psWarpOptions->panSrcBands[i] = psWarpOptions->panDstBands[i] = i+1; 

		psWarpOptions->pfnTransformer		= GDALGenImgProjTransform;
		psWarpOptions->eResampleAlg		= GRA_NearestNeighbour; // GRA_Bilinear GRA_Cubic GRA_CubicSpline GRA_Average GRA_Mode 

		// Set nodata value for warp action
		psWarpOptions->padfDstNoDataReal 	= (double *)CPLMalloc(psWarpOptions->nBandCount*sizeof(double));
		psWarpOptions->padfDstNoDataImag 	= (double *)CPLMalloc(psWarpOptions->nBandCount*sizeof(double));

		for (i = 0; i < cursor->nband; i++ ) psWarpOptions->padfDstNoDataReal[i] = cursor->nodata[i];

		// Multithread options activation
		const char* pszWarpThreads = CSLFetchNameValue(psWarpOptions->papszWarpOptions, "NUM_THREADS");
		
		if( pszWarpThreads != NULL ) psWarpOptions->papszWarpOptions 	= CSLSetNameValue( psWarpOptions->papszWarpOptions, "NUM_THREADS", 	pszWarpThreads);			
		psWarpOptions->papszWarpOptions 				= CSLSetNameValue( psWarpOptions->papszWarpOptions, "INIT_DEST", 	"NO_DATA" );
		if ( GCPCount > 0 ) psWarpOptions->papszWarpOptions 		= CSLSetNameValue( psWarpOptions->papszWarpOptions, "METHOD", 		"GCP_TPS" );

		// Try to use the version 2 of function
		psWarpOptions->pTransformerArg	= GDALCreateGenImgProjTransformer2(cursor->dataset, hMEMDstDS, psWarpOptions->papszWarpOptions );
						  // GDALCreateGenImgProjTransformer( hMEMSrcDS, cursor->wkt, hMEMDstDS, wktTargetSRS, FALSE, 0, 1 );
						  // GDALCreateGenImgProjTransformer3

		// Perform warp
		if ( ( oOperation = GDALCreateWarpOperation(psWarpOptions)) == NULL ) pthread_exit( (void *)2 );

		if ( pszWarpThreads != NULL ) 	GDALChunkAndWarpMulti( oOperation, 0, 0, cursor->nPixels, cursor->nLines );		// Multi thread
		else				GDALChunkAndWarpImage( oOperation, 0, 0, cursor->nPixels, cursor->nLines );		// Single thread

		// Cleaning
		GDALDestroyGenImgProjTransformer( psWarpOptions->pTransformerArg );
		GDALDestroyWarpOptions( psWarpOptions );
		GDALClose(cursor->dataset);

		// Calculate common AOI
		if ( GDALInvGeoTransform(cursor->adfDstGeoTransform, 	adfInvTransform ) == FALSE ) pthread_exit( (void *)2 );

	} else {
		hMEMDstDS	= cursor->dataset;
		cursor->nPixels = cursor->sizeX;
		cursor->nLines 	= cursor->sizeY;
		if ( GDALInvGeoTransform(cursor->GeoTransform,		adfInvTransform ) == FALSE ) pthread_exit( (void *)2 );


	}			



	Xp_ul = adfInvTransform[0] + GeoX_ul * adfInvTransform[1] + GeoY_ul * adfInvTransform[2];
	Yp_ul = adfInvTransform[3] + GeoX_ul * adfInvTransform[4] + GeoY_ul * adfInvTransform[5];
	Xp_ul *= (Xp_ul > 0.0 ) ? 0 :  -1.0; Yp_ul *= ( Yp_ul > 0.0 ) ? 0 : -1.0;

	
	double x_border_swift, y_border_swift; 
	x_border_swift = (double)pxAOISizeX - ( Xp_ul + (double)cursor->nPixels ); 	if ( ( x_border_swift > 0 ) && ( x_border_swift < 4.9 ) ) Xp_ul = Xp_ul + 1.0;
	y_border_swift = (double)pxAOISizeY - ( Yp_ul + (double)cursor->nLines ); 	if ( ( y_border_swift > 0 ) && ( y_border_swift < 4.9 ) ) Yp_ul = Yp_ul + 1.0;

	// If input size as exaictly the same of output size, no offset, no resize, no reporjetion so ... why I have to copy? bro .. I skip ...
	if ( ( Xp_ul == 0 ) && ( Yp_ul == 0 ) && ( cursor->nPixels == pxAOISizeX ) && ( cursor->nLines == pxAOISizeY ) && ( cursor->epsg == epgs_out ) && ( cursor->warp == FALSE ) ){ cursor->dataset = hMEMDstDS; pthread_exit( (void *)0 ); }

	if ( ( hDstDS = GDALCreate( hMEMDriver, "", pxAOISizeX,  pxAOISizeY, cursor->nband, GDT_Float64, NULL ) ) == NULL ) pthread_exit( (void *)2 );

	if ( ( (int)Xp_ul + cursor->nPixels ) > pxAOISizeX ) cursor->nPixels = pxAOISizeX - (int)Xp_ul;
	if ( ( (int)Yp_ul + cursor->nLines  ) > pxAOISizeY ) cursor->nLines  = pxAOISizeY - (int)Yp_ul;


	if ( cursor->nPixels > pxAOISizeX )	cursor->nPixels	= pxAOISizeX;
	if ( cursor->nLines  > pxAOISizeY ) 	cursor->nLines	= pxAOISizeY;

	raster = (double *)malloc(sizeof(double) * pxAOISizeX * pxAOISizeY ); if ( raster == NULL ) pthread_exit( (void *)2 );
	
	for (i = 0, j = 0; i < cursor->nband; i++ ){

		hBandSrc = GDALGetRasterBand( hMEMDstDS, i+1 );	err = GDALRasterIOEx( hBandSrc, GF_Read,  0, 0,	cursor->nPixels, cursor->nLines, raster, cursor->nPixels, cursor->nLines, GDT_Float64, 0, 0, &psExtraArg );
		hBandSrc = GDALGetRasterBand( hDstDS,	 i+1 ); GDALFillRaster( hBandSrc, cursor->nodata[i], 0 );
								err = GDALRasterIOEx( hBandSrc, GF_Write,
													(int)Xp_ul, 		(int)Yp_ul,
													cursor->nPixels,	cursor->nLines,	raster,
													cursor->nPixels, 	cursor->nLines,	GDT_Float64, 0, 0, &psExtraArg );

		GDALSetRasterNoDataValue(hBandSrc, cursor->nodata[i]);
		GDALSetRasterScale	(hBandSrc, cursor->scale[i] );
		GDALSetRasterOffset	(hBandSrc, cursor->offset[i]);
	}
	free(raster);
	GDALClose(hMEMDstDS);
	cursor->dataset = hDstDS;
	pthread_exit( (void *)0 );
}


//------------------------------------------------------------------------------------------------

int peepingtom(const char *name, DescCoverage *head, DescCoverage *cursor, DescCoverage tmp,  struct tm *t_time ){

	struct 		dirent **namelist;
	struct 		dirent *entry;
	int		result	= FALSE;

	int n = scandir(name, &namelist, 0, alphasort); //alphasort versionsort
	if (n < 0) return 1;
	while (n--) {
	       	char 		path[MAX_PATH_LEN];
		int 	len 		= 0;
		int 	t_ref_time 	= 0;
		int 	pxSizeX 	= 0;
		int 	pxSizeY 	= 0;
		int	nband		= 0;
		int 	type		= 0;
		char	*value		= NULL;
		char	label		= '\0';
		double 	adfGeoTransform[6];

		entry = namelist[n];
	        if ( entry->d_name[0]  == '.' )  continue;
 		len 	= snprintf(path, sizeof(path)-1, "%s/%s", name, entry->d_name); path[len] = 0;

		if ( entry->d_type == DT_UNKNOWN ){
			struct stat fs;
			lstat(path, &fs); if( S_ISDIR(fs.st_mode) ) entry->d_type = DT_DIR;
		}

		if (entry->d_type == DT_DIR) {

			value 	= &entry->d_name[1];
			label	=  entry->d_name[0];

			switch (label){
				case 'Y': // Year 
						
					t_time->tm_year	= atoi(value) - 1900;

					tmp->infoflag[0] = TRUE;
					break;
				case 'M': // Month
					t_time->tm_mon	= atoi(value) - 1;

					tmp->infoflag[1] = TRUE;
					break;
				case 'D': // Day
					t_time->tm_mday	= atoi(value);

					tmp->infoflag[2] = TRUE;
					break;
				case 'T': // Time HHMMSS
					t_ref_time	= atoi(value);
					t_time->tm_hour = ( t_ref_time / 10000 );
					t_time->tm_min  = ( t_ref_time - t_time->tm_hour * 10000 ) / 100;
					t_time->tm_sec  = ( t_ref_time - t_time->tm_hour * 10000 - t_time->tm_min * 100 );

					tmp->t_min = tmp->t_max = timegm(t_time);
					tmp->infoflag[3] = TRUE;
					break;
				case 'E': // EPSG
					tmp->epsg  = atoi(value);
					// OGRSpatialReferenceH hTargetSRS;
					// hTargetSRS = OSRNewSpatialReference(NULL);
					// OSRImportFromEPSG(hTargetSRS, atoi(&entry->d_name[1]) );
					// OSRDestroySpatialReference(hTargetSRS);
				
					tmp->infoflag[4] = TRUE;
					break;
				case 'F':
					sscanf(value, "%dx%dx%d_%d", &pxSizeX, &pxSizeY, &nband, &type );

					adfGeoTransform[0] 	= 0.0; 	adfGeoTransform[1] 	= 1.0; 	adfGeoTransform[2] 	= 0.0;
					adfGeoTransform[3] 	= 0.0;	adfGeoTransform[4] 	= 0.0;	adfGeoTransform[5] 	= 1.0;
			
					tmp->GeoX_ul 	= adfGeoTransform[0];
					tmp->GeoY_ul 	= adfGeoTransform[3];
					tmp->GeoX_lr 	= adfGeoTransform[0] + pxSizeX * adfGeoTransform[1] + pxSizeY * adfGeoTransform[2];
					tmp->GeoY_lr 	= adfGeoTransform[3] + pxSizeX * adfGeoTransform[4] + pxSizeY * adfGeoTransform[5];

					tmp->x_res      = adfGeoTransform[1];
					tmp->y_res      = adfGeoTransform[5];
					tmp->nband	= nband;
					tmp->type	= type;
					tmp->label	= NULL;
					tmp->epsg  	= 0;

					tmp->infoflag[4] = TRUE;
					tmp->infoflag[5] = TRUE;


				case 'G': // GeoTransform and image information
					sscanf(value, "%lfx%lf_%lfx%lf_%dx%dx%d_%d",   &adfGeoTransform[0], &adfGeoTransform[3], &adfGeoTransform[1], &adfGeoTransform[5], &pxSizeX, &pxSizeY, &nband, &type );

					tmp->GeoX_ul 	= adfGeoTransform[0];
					tmp->GeoY_ul 	= adfGeoTransform[3];
					tmp->GeoX_lr 	= adfGeoTransform[0] + pxSizeX * adfGeoTransform[1] + pxSizeY * adfGeoTransform[2];
					tmp->GeoY_lr 	= adfGeoTransform[3] + pxSizeX * adfGeoTransform[4] + pxSizeY * adfGeoTransform[5];

					tmp->x_res      = adfGeoTransform[1];
					tmp->y_res      = adfGeoTransform[5];
					tmp->nband	= nband;
					tmp->type	= type;
					tmp->label	= NULL;


					tmp->infoflag[5] = TRUE;
					break;
				case 'H': // High
					tmp->high_max = tmp->high_min  = atof(value);
					break;
				default:
					break;

			}
			peepingtom(path, head, cursor, tmp, t_time );
		} else {

	        	if ( entry->d_name[0]  == '.' 			) continue;
	                if ( strstr(entry->d_name, "DescribeCoverage" )	) continue;
			if ( strstr(entry->d_name, ".xml") 		) continue;
			if ( strstr(entry->d_name, ".py") 		) continue;
			if ( strstr(entry->d_name, ".txt") 		) continue;
			if ( strstr(entry->d_name, ".hdr") 		) continue;


			if ( *head == NULL ) {
				*head = *cursor 	= (DescCoverage)malloc(sizeof(struct sDescCoverage));
				memcpy((*cursor), tmp, sizeof(struct sDescCoverage));
				(*cursor)->next  	= NULL;
				(*cursor)->label	= NULL;
				(*cursor)->hit_number	= 1;
				continue;
			}

			if (	( (*cursor)->epsg  == tmp->epsg  )	&&
				( (*cursor)->x_res == tmp->x_res ) 	&&
				( (*cursor)->y_res == tmp->y_res ) 	&& 
				( (*cursor)->nband == tmp->nband )	&& 
				( (*cursor)->type  == tmp->type  ) 	){
	
				if ( tmp->GeoX_ul < (*cursor)->GeoX_ul ) 	(*cursor)->GeoX_ul	= tmp->GeoX_ul;
				if ( tmp->GeoX_lr > (*cursor)->GeoX_lr ) 	(*cursor)->GeoX_lr	= tmp->GeoX_lr;
				if ( tmp->GeoY_ul > (*cursor)->GeoY_ul ) 	(*cursor)->GeoY_ul	= tmp->GeoY_ul;
				if ( tmp->GeoY_lr < (*cursor)->GeoY_lr ) 	(*cursor)->GeoY_lr	= tmp->GeoY_lr;
				if ( (*cursor)->t_min > tmp->t_min )		(*cursor)->t_min 	= tmp->t_min;
			        if ( (*cursor)->t_max < tmp->t_max ) 		(*cursor)->t_max 	= tmp->t_max;
				if ( (*cursor)->high_min > tmp->high_min )	(*cursor)->high_min 	= tmp->high_min;
				if ( (*cursor)->high_max < tmp->high_max ) 	(*cursor)->high_max 	= tmp->high_max;

				(*cursor)->hit_number++;
				continue;
			}

			for( *cursor = *head; *cursor != NULL; *cursor = (*cursor)->next)
				if (	( (*cursor)->epsg  == tmp->epsg  )	&&
					( (*cursor)->x_res == tmp->x_res ) 	&&
					( (*cursor)->y_res == tmp->y_res )	&&
					( (*cursor)->type  == tmp->type  )	&&
					( (*cursor)->nband == tmp->nband ) 	) break;

			if ( *cursor == NULL ) {
				for( *cursor = *head; (*cursor)->next != NULL; (*cursor) = (*cursor)->next){}						
				(*cursor)->next		= malloc(sizeof(struct sDescCoverage)); (*cursor) = (*cursor)->next;
				memcpy((*cursor), tmp, sizeof(struct sDescCoverage));
				(*cursor)->next  	= NULL;
				(*cursor)->label	= NULL;
				(*cursor)->hit_number	= 1;
				continue;
			}
	

			if ( tmp->GeoX_ul   < (*cursor)->GeoX_ul ) 	(*cursor)->GeoX_ul	= tmp->GeoX_ul;
			if ( tmp->GeoX_lr   > (*cursor)->GeoX_lr ) 	(*cursor)->GeoX_lr	= tmp->GeoX_lr;
			if ( tmp->GeoY_ul   > (*cursor)->GeoY_ul ) 	(*cursor)->GeoY_ul	= tmp->GeoY_ul;
			if ( tmp->GeoY_lr   < (*cursor)->GeoY_lr ) 	(*cursor)->GeoY_lr	= tmp->GeoY_lr;

			if ( (*cursor)->high_min > tmp->high_min )	(*cursor)->high_min 	= tmp->high_min;
			if ( (*cursor)->high_max < tmp->high_max ) 	(*cursor)->high_max 	= tmp->high_max;


			if ( (*cursor)->t_min > tmp->t_min )		(*cursor)->t_min 	= tmp->t_min;
			if ( (*cursor)->t_max < tmp->t_max ) 		(*cursor)->t_max 	= tmp->t_max;

			(*cursor)->hit_number++;
		}	
		free(namelist[n]);
	}
	free(namelist);
	return 0;
}


//------------------------------------------------------------------------------------------------
int VirtualDescribeCoverage(char *root_prod_path,  DescCoverage *head, DescCoverage *cursor, DescCoverage tmp, int *killable, request_rec *r, char *token_from_user ); // Prototype to avoid warring in compiling

int generateProdInfo(char *root_prod_path, int *killable, request_rec *r, char *token_from_user ){
	int i;
	struct tm	t_time;	
	char 		imgs_path[MAX_PATH_LEN];
	char 		epsg_res_tmp[256];

	int		pxAOISizeX	= 0;
	int		pxAOISizeY	= 0;
	FILE		*virt		= NULL;
	FILE		*DCjson		= NULL;
	char		prod_path[MAX_PATH_LEN];
	DescCoverage	head		= NULL;
	DescCoverage	cursor		= NULL;
	DescCoverage	tmp		= NULL;
	json_object 	*jobj 		= NULL; 
	json_object 	*jobjDS		= NULL; 
	json_object 	*jobjBands	= NULL; 
	json_object 	*jobjBand	= NULL; 
	json_object 	*jobjnil	= NULL; 
	json_object 	*jarray 	= NULL;

	bzero(prod_path, MAX_STR_LEN - 1);
	strcpy(prod_path, root_prod_path);


	// I have to cycling for sub directory get all describecoverage and create a sort of som

	// First try to check if file exists ...
	bzero(imgs_path, MAX_PATH_LEN - 1); snprintf(imgs_path, MAX_PATH_LEN, "%s/DescribeCoverage.json", root_prod_path );	DCjson 	= fopen ( imgs_path, "r" );


	// Ok exists...
	if ( ( DCjson != NULL ) && ( fsize(DCjson) != 0 ) ) { fclose(DCjson); return 0;	}

	// Ops ... close all and 
	if ( DCjson != NULL ) { fclose(DCjson); DCjson = NULL; }


	// i have to lock this section to avoid many process do the same thing
	bzero(imgs_path, MAX_PATH_LEN - 1); sprintf(imgs_path, "%s/.lock", root_prod_path ); 
	if ( fexists(imgs_path) ) return 1; close(creat(imgs_path, 0666));


	// Reopen in write mode
	bzero(imgs_path, MAX_PATH_LEN - 1); sprintf(imgs_path, "%s/DescribeCoverage.json", root_prod_path ); DCjson = fopen ( imgs_path, "w" );
	if ( DCjson == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable write DescribeCoverage.json for %s", root_prod_path);  return 1; }


	jobj 	= json_object_new_object();

	ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Generation of DescribeCoverage.json for %s", root_prod_path); 
	(*killable) = FALSE;


	if ( tmp == NULL ) tmp = (DescCoverage)malloc(sizeof(struct  sDescCoverage)); for ( i = 0; i < 6; i++) tmp->infoflag[i] = FALSE; tmp->high_min = tmp->high_max = DEFAULT_NODATA;
	

	// check if i create a run virtuacollector .virtual

	bzero(imgs_path, MAX_PATH_LEN - 1); sprintf(imgs_path, "%s/.virtual", 	root_prod_path ); virt 	= fopen ( imgs_path, "r" );
	if ( virt != NULL ) 	{ fclose(virt);  	VirtualDescribeCoverage(	root_prod_path, &head, &cursor, tmp, killable, r, token_from_user ); }
	else			{			peepingtom(			root_prod_path, &head, &cursor, tmp, &t_time ); }
					
	if ( tmp != NULL ) { free(tmp); tmp = NULL; }	

	if ( head == NULL ){
		fclose(DCjson); 
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Unable to generate DescribeCoverage.json for %s, no products found!", root_prod_path);  

		bzero(imgs_path, MAX_PATH_LEN - 1); sprintf(imgs_path, "%s/DescribeCoverage.json", root_prod_path ); 
		if ( remove(imgs_path) == 0 ) 	{ 																	}	
		else				{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to remove DescribeCoverage.json for %s", root_prod_path); 	}


		bzero(imgs_path, MAX_PATH_LEN - 1); sprintf(imgs_path, "%s/.lock", root_prod_path ); 
		if ( remove(imgs_path) == 0 ) 	{ 																	}	
		else				{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to remove lock file for %s\n", root_prod_path);		}

		(*killable) = TRUE;
		return 0; 
	}

	jarray = json_object_new_array();

	for( cursor = head; cursor != NULL; cursor = cursor->next){
		cursor->GeoX_ul = cursor->GeoX_ul - fmod( cursor->GeoX_ul, cursor->x_res );
		cursor->GeoX_lr	= cursor->GeoX_lr - fmod( cursor->GeoX_lr, cursor->x_res );
		cursor->GeoY_ul	= cursor->GeoY_ul - fmod( cursor->GeoY_ul, cursor->y_res );
		cursor->GeoY_lr	= cursor->GeoY_lr - fmod( cursor->GeoY_lr, cursor->y_res );

		pxAOISizeX = (int)( ( cursor->GeoX_lr - cursor->GeoX_ul ) / cursor->x_res );	
		pxAOISizeY = (int)( ( cursor->GeoY_lr - cursor->GeoY_ul ) / cursor->y_res );		

		bzero( epsg_res_tmp, 255 );
		if ( cursor->label == NULL ) 	sprintf(epsg_res_tmp, "%d_%g", 		cursor->epsg, cursor->x_res);
		else				sprintf(epsg_res_tmp, "%s@%d_%g", 	cursor->label, cursor->epsg, cursor->x_res );

		for(i = ( cursor->label == NULL ) ? 0 : strlen(cursor->label) ; i < strlen(epsg_res_tmp); i++) 
			if (epsg_res_tmp[i] == '.') { memmove(&epsg_res_tmp[i], &epsg_res_tmp[i+1], strlen(epsg_res_tmp) - i); break; }

		jobjDS = json_object_new_object();


		jobjBands = json_object_new_array();
		for(i = 0; i < cursor->nband; i++){
			jobjBand = json_object_new_object();
			json_object_object_add(jobjBand, "color_interpretation",	json_object_new_string("Undefined")); 
			json_object_object_add(jobjBand, "data_type",			json_object_new_string(GDALGetDataTypeName(cursor->type) != NULL ?  GDALGetDataTypeName(cursor->type) : "GDT_Unknown") ) ;
			json_object_object_add(jobjBand, "definition",			json_object_new_string(""));
			json_object_object_add(jobjBand, "identifier",			json_object_new_string(""));
			json_object_object_add(jobjBand, "name",			json_object_new_string(""));

			jobjnil = json_object_new_object();
			json_object_object_add(jobjnil, "reason",			json_object_new_string("http://www.opengis.net/def/nil/OGC/0/unknown"));
			json_object_object_add(jobjnil, "value",			json_object_new_string("0"));
			json_object_object_add(jobjBand,"nil_values", jobjnil);
			

			json_object_object_add(jobjBand, "uom",				json_object_new_string(""));
			json_object_object_add(jobjBand, "value_max",			json_object_new_string(""));
			json_object_object_add(jobjBand, "value_min",			json_object_new_string(""));



			json_object_array_add(jobjBands, jobjBand);

		}


		bzero(imgs_path, MAX_PATH_LEN - 1);
		sprintf(imgs_path, "%s_%s", basename(root_prod_path), epsg_res_tmp );
		json_object_object_add(jobjDS,	"bands", 	jobjBands);
		json_object_object_add(jobjDS, 	"name",		json_object_new_string(imgs_path));
		json_object_object_add(jobjDS, 	"id", 		json_object_new_string(epsg_res_tmp));
		json_object_object_add(jobjDS, 	"EPSG", 	json_object_new_int(cursor->epsg));
		json_object_object_add(jobjDS,	"GeoX_lr", 	json_object_new_double(cursor->GeoX_lr));	
		json_object_object_add(jobjDS,	"GeoY_lr", 	json_object_new_double(cursor->GeoY_lr));	
		json_object_object_add(jobjDS,	"GeoX_ul", 	json_object_new_double(cursor->GeoX_ul));	
		json_object_object_add(jobjDS,	"GeoY_ul", 	json_object_new_double(cursor->GeoY_ul));	
		json_object_object_add(jobjDS,	"x_res", 	json_object_new_double(cursor->x_res));	
		json_object_object_add(jobjDS,	"y_res", 	json_object_new_double(cursor->y_res));	
		json_object_object_add(jobjDS, 	"pxAOISizeX", 	json_object_new_int(pxAOISizeX));
		json_object_object_add(jobjDS, 	"pxAOISizeY", 	json_object_new_int(pxAOISizeY));
		json_object_object_add(jobjDS, 	"nband", 	json_object_new_int(cursor->nband));
		json_object_object_add(jobjDS, 	"t_min", 	json_object_new_int(cursor->t_min));
		json_object_object_add(jobjDS, 	"t_max", 	json_object_new_int(cursor->t_max));
		if( cursor->high_min != DEFAULT_NODATA ) json_object_object_add(jobjDS, 	"high_min", 	json_object_new_double(cursor->high_min));
		if( cursor->high_max != DEFAULT_NODATA ) json_object_object_add(jobjDS, 	"high_max", 	json_object_new_double(cursor->high_max));
		json_object_object_add(jobjDS, 	"type", 	json_object_new_int(cursor->type));
		json_object_object_add(jobjDS, 	"hit_number", 	json_object_new_int(cursor->hit_number));

		json_object_array_add(jarray, jobjDS);
	}
	// Write JSON file
	json_object_object_add(jobj,"Collections", jarray);
	fprintf(DCjson, "%s",json_object_to_json_string(jobj)); 

        fclose(DCjson);

	// Remove locking file
	bzero(imgs_path, MAX_PATH_LEN - 1); sprintf(imgs_path, "%s/.lock", root_prod_path ); 
	if ( remove(imgs_path) == 0 ) 	{ 																(*killable) = TRUE; return 0; }
	else				{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to remove lock file for %s\n", root_prod_path);   	(*killable) = TRUE; return 1; }


}


//------------------------------------------------------------------------------------------------
int print_element_names(xmlNode * a_node){
	xmlNode *cur_node = NULL;

	for (cur_node = a_node; cur_node; cur_node = cur_node->next) {
        	if (cur_node->type == XML_ELEMENT_NODE) { fprintf(stderr, "node type: Element, name: %s\n", cur_node->name); fflush(stderr); }
		print_element_names(cur_node->children);
	}

	return 0;
}

int getEPSGfromURL( char *url){
	        int i;
		        for (i = 0; i < strlen(url); i++) if ( url[i] == '&' ) url[i] = '\0';
			        return atoi( strstr(url, "EPSG")+7 );
}

int get_element_to_DescCoverage(xmlNode * a_node, DescCoverage tmp){
	xmlNode *cur_node = NULL;
	double	a,b,c;
	for (cur_node = a_node; cur_node; cur_node = cur_node->next) {
        	if (cur_node->type == XML_ELEMENT_NODE) {
			if 	( ! xmlStrcmp(cur_node->name, (const xmlChar *)"lowerCorner") ) 	sscanf((const char *)xmlNodeGetContent(cur_node), "%lf %lf %ld", &tmp->GeoY_lr, &tmp->GeoX_ul, &tmp->t_min );
			else if ( ! xmlStrcmp(cur_node->name, (const xmlChar *)"upperCorner") ) 	sscanf((const char *)xmlNodeGetContent(cur_node), "%lf %lf %ld", &tmp->GeoY_ul, &tmp->GeoX_lr, &tmp->t_max );
			else if ( ! xmlStrcmp(cur_node->name, (const xmlChar *)"Envelope") )  { 	char *prop =  (char *)xmlGetProp(cur_node, (const xmlChar *)"srsName" ); tmp->epsg = getEPSGfromURL(prop); }
			else if ( ! xmlStrcmp(cur_node->name, (const xmlChar *)"high") )		sscanf((const char *)xmlNodeGetContent(cur_node), "%lf %lf %d",  &a, &b, &tmp->hit_number );
			else if ( ! xmlStrcmp(cur_node->name, (const xmlChar *)"offsetVector")) { 	sscanf((const char *)xmlNodeGetContent(cur_node), "%lf %lf %lf", &a, &b, &c ); 
				if ( a != 0.0 ) tmp->y_res = a; if ( b != 0.0 ) tmp->x_res = b; if ( c != 0.0 ) tmp->nband = (int)c;
			}
		
			// fprintf(stderr, "node type: Element, name: %s -> %s\n", cur_node->name,  xmlNodeGetContent(cur_node) ); fflush(stderr); 
		}
		get_element_to_DescCoverage(cur_node->children, tmp);
	}

	return 0;
}


int get_CoverageId_from_GetCapabilities(xmlNode * a_node, char *prod, urlList **URLh, urlList **URLc, char *base ){
	xmlNode *cur_node = NULL;

	for (cur_node = a_node; cur_node; cur_node = cur_node->next) {
        	if (cur_node->type == XML_ELEMENT_NODE) {
			if ( ( ! xmlStrcmp(cur_node->name, (const xmlChar *)"CoverageId") ) && ( strstr( (const char *)xmlNodeGetContent(cur_node), prod ) ) ) { 

				if ( *URLh == NULL )	{ *URLh = *URLc = (urlList *)malloc(sizeof(urlList));  							}
				else			{ (*URLc)->next = (urlList *)malloc(sizeof(urlList)); (*URLc) = (*URLc)->next; }

				(*URLc)->url = (char *)malloc(MAX_STR_LEN); bzero((*URLc)->url, MAX_STR_LEN - 1);
				sprintf((*URLc)->url, "%sCoverageId=%s", base, (const char *)xmlNodeGetContent(cur_node) );
				(*URLc)->next = NULL;
			}
		}
		get_CoverageId_from_GetCapabilities(cur_node->children, prod, URLh, URLc, base );
	}

	return 0;
}



int DescribeCoverageChekURL(char *url,  request_rec *r, urlList **URLh, urlList **URLc ){
	CURL 			*curl  		= NULL;
	char			*token		= NULL;
	struct curl_slist 	*chunk_curl 	= NULL;
	CURLcode 		res;
	char			tmp[MAX_STR_LEN];
        struct 			MemoryStruct chunk;
	long 			response_code;

        chunk.memory    = malloc(1);
        chunk.size      = 0;


	curl = curl_easy_init();
	if(curl) {
		bzero(tmp, MAX_STR_LEN - 1); sprintf(tmp, "%s&Request=DescribeCoverage", url ); 
		curl_easy_setopt(curl, CURLOPT_URL, 	tmp);


		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, 	WriteMemoryCallback);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, 	(void *)&chunk);
		curl_easy_setopt(curl, CURLOPT_USERAGENT, 	"MWCS/1.0");
	//	curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
		curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, FALSE);

		res = curl_easy_perform(curl);
		if(res != CURLE_OK) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: XMLDescribeCoverageToDescCoverage curl_easy_perform() failed: %s", curl_easy_strerror(res) ); return FALSE; }
		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code); 
		if ( response_code == 200 ) { 
			curl_easy_cleanup(curl); curl_slist_free_all(chunk_curl); curl_global_cleanup(); 
			if ( *URLh == NULL )	{ *URLh = *URLc = (urlList *)malloc(sizeof(urlList));  							}
			else			{ (*URLc)->next = (urlList *)malloc(sizeof(urlList)); (*URLc) = (*URLc)->next; }

			(*URLc)->url  = (char *)malloc(MAX_STR_LEN); bzero((*URLc)->url, MAX_STR_LEN - 1); strcpy( (*URLc)->url, url );
			(*URLc)->next = NULL;
			return TRUE; 

		}

		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "WARN: XMLDescribeCoverageToDescCoverage get HTTP code %ld, try to use GetCapabilities", response_code );

		bzero(tmp, MAX_STR_LEN - 1); sprintf(tmp, "%s&Request=GetCapabilities", url ); 
		curl_easy_setopt(curl, CURLOPT_URL, tmp);
		if(chunk.memory) free(chunk.memory);
	        chunk.memory    = malloc(1);
	        chunk.size      = 0;

		res = curl_easy_perform(curl);
		if(res != CURLE_OK) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: XMLDescribeCoverageToDescCoverage curl_easy_perform() failed: %s", curl_easy_strerror(res) ); return FALSE; }

		char 		*xml  		= NULL;
		size_t          xml_len         = 0;
		ssize_t         xml_n           = 0;

	        for( token = strtok(chunk.memory, "\n"); token != NULL;  token = strtok(NULL, "\n") ) {
			xml_n = strlen(token);
			xml   = realloc(xml, xml_len + xml_n + 1);
			memcpy(xml + xml_len, token, xml_n);
			xml_len += xml_n;
			xml[xml_len] = '\0';
		}
        	if(chunk.memory) free(chunk.memory);
		curl_easy_cleanup(curl);
		curl_slist_free_all(chunk_curl);
        	curl_global_cleanup();


		xmlDocPtr 	doc;
		xmlNode 	*root_element = NULL;

		doc 	 	= xmlReadMemory(xml, strlen(xml), "noname.xml", NULL, 0); if (doc == NULL) { fprintf(stderr,"ERROR: Failed to parse GetCapabilities\n" ); return FALSE; }
		root_element 	= xmlDocGetRootElement(doc);	

        	const char      *pszValue;
		char            *pszKey = NULL;
		char 		*p; 
		char 		*tok 	= strtok( url, DELIMS);
		char		*prod	= NULL;

		bzero(tmp, MAX_STR_LEN - 1);
		while( tok != NULL ) {
			pszValue = (const char *)CPLParseNameValue( tok, &pszKey );
			for(p = pszKey;*p;++p) *p=*p>0x40&&*p<0x5b?*p|0x60:*p;

			if ( pszValue == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to parsing '%s'", tok); return FALSE; }
			if ( ! strcmp(pszKey, "coverageid") ) { prod = malloc( strlen( pszKey ) + 2 ); sprintf(prod, "%s_", pszValue ); tok = strtok( NULL, DELIMS); continue; }
			strcat(tmp, tok);
			strcat(tmp, "&"); 

			tok = strtok( NULL, DELIMS);
		}
		if ( prod == NULL ) return FALSE;

		get_CoverageId_from_GetCapabilities(root_element, prod, URLh, URLc, tmp );

		xmlFreeDoc(doc);

		return TRUE;
	} 
	return FALSE;

}

int XMLDescribeCoverageToDescCoverage(char *url, DescCoverage dc, request_rec *r ){
	CURL 			*curl  		= NULL;
	char			*token		= NULL;
	struct curl_slist 	*chunk_curl 	= NULL;
	CURLcode 		res;
	char			tmp[MAX_STR_LEN];
        struct 			MemoryStruct chunk;
	long 			response_code;

        chunk.memory    = malloc(1);
        chunk.size      = 0;


	curl = curl_easy_init();
	if(curl) {
		bzero(tmp, MAX_STR_LEN - 1); sprintf(tmp, "%s&Request=DescribeCoverage", url ); 
		curl_easy_setopt(curl, CURLOPT_URL, 	tmp);


		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, 	WriteMemoryCallback);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, 	(void *)&chunk);
		curl_easy_setopt(curl, CURLOPT_USERAGENT, 	"MWCS/1.0");
	//	curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
		curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, FALSE);

		res = curl_easy_perform(curl);
		if(res != CURLE_OK) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: XMLDescribeCoverageToDescCoverage curl_easy_perform() failed: %s", curl_easy_strerror(res) ); 						  return FALSE; }
		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code); if ( response_code != 200 ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: XMLDescribeCoverageToDescCoverage get HTTP code %ld", response_code ); return FALSE; }

		char 		*xml  		= NULL;
		size_t          xml_len         = 0;
		ssize_t         xml_n           = 0;
		double 		dtmp 		= 0.0;

	        for( token = strtok(chunk.memory, "\n"); token != NULL;  token = strtok(NULL, "\n") ) {
			xml_n = strlen(token);
			xml   = realloc(xml, xml_len + xml_n + 1);
			memcpy(xml + xml_len, token, xml_n);
			xml_len += xml_n;
			xml[xml_len] = '\0';
		}
        	if(chunk.memory) free(chunk.memory);
		curl_easy_cleanup(curl);
		curl_slist_free_all(chunk_curl);
        	curl_global_cleanup();


		xmlDocPtr 	doc;
		xmlNode 	*root_element = NULL;

		doc 	 	= xmlReadMemory(xml, strlen(xml), "noname.xml", NULL, 0); if (doc == NULL) { fprintf(stderr,"ERROR: Failed to parse DescribeCoverage\n" ); return FALSE; }
		root_element 	= xmlDocGetRootElement(doc);	
		get_element_to_DescCoverage(root_element, dc);

		dc->hit_number++;
		if (dc->epsg != 4326 ) {	dtmp = dc->GeoX_ul; dc->GeoX_ul = dc->GeoY_lr; dc->GeoY_lr = dtmp; 
						dtmp = dc->GeoX_lr; dc->GeoX_lr = dc->GeoY_ul; dc->GeoY_ul = dtmp;	} 


		xmlFreeDoc(doc);

		return TRUE;
	} 
	return FALSE;

}


//------------------------------------------------------------------------------------------
int readFileDotURL(char *file, char **url, char *token_from_user ){
        FILE *url_fp    = NULL;
        *url            = NULL;
        url_fp          = fopen(file, "r");  if ( url_fp == NULL ) return 1;

        *url = (char *)malloc( MAX_PATH_LEN * sizeof(char) ); bzero( *url, MAX_PATH_LEN - 1);
        char *s = fgets(*url, MAX_PATH_LEN, url_fp); if ( s == NULL ) { fclose(url_fp); return 1; }
        for (int i = 0; i < strlen(*url); i++) if ( (*url)[i] == '\n' ) { (*url)[i] = '\0'; break; }

        if ( token_from_user != NULL ) { strcat(*url, "&token=" ); strcat(*url, token_from_user ); }

        fclose(url_fp);
        return 0;
}

//------------------------------------------------------------------------------------------------


int VirtualDescribeCoverage(char *root_prod_path,  DescCoverage *head, DescCoverage *cursor, DescCoverage tmp, int *killable, request_rec *r, char *token_from_user){
	int i;
	char 		imgs_path[MAX_PATH_LEN];
	char		prod_path[MAX_PATH_LEN];
	DIR 		*dp		= NULL;
	struct dirent 	*ep		= NULL;  
	FILE		*DC		= NULL;


	DIR		*(*openSource)(const char *)			= NULL;
	struct dirent 	*(*listSource)(DIR *) 				= NULL;
	int 		 (*closeSource)(DIR *)				= NULL;

	FILE 		*(*openSourceFile)(const char *, const char *) 	= NULL;
	int		 (*readSourceFile)(FILE *, const char *, ...)	= NULL;
	int 		 (*closeSourceFile)(FILE *)			= NULL;

	listSource 	= readdir;
	openSource	= opendir;
	closeSource	= closedir;
	openSourceFile	= fopen;
	readSourceFile	= fscanf;
	closeSourceFile = fclose;


	tmp->label      = malloc(256); bzero(tmp->label, 254);
	tmp->next	= NULL;
	tmp->high_min	= DEFAULT_NODATA;
	tmp->high_max	= DEFAULT_NODATA;

	dp = (*openSource)(root_prod_path);

	while ( (ep = (*listSource)(dp)) ){
        	if ( ep->d_name[0] == '.' ) 				continue; 
		if (! strcmp(ep->d_name, "DescribeCoverage.json") ) 	continue;
		

		bzero(prod_path, MAX_STR_LEN - 1);
		sprintf(prod_path, "%s/%s", root_prod_path, ep->d_name);

		//-------------------------------------------------------------------------------

		if ( ( prod_path != NULL ) && ( get_filename_ext(prod_path) != NULL ) && ( ! strcmp ( get_filename_ext(prod_path), "url" ) ) ) {
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Generation Virtual Collection from Remote using URL in %s", prod_path);
			char *url	= NULL;
			urlList	*URLh	= NULL;
			urlList	*URLc	= NULL;
			if ( readFileDotURL(prod_path, &url, token_from_user ) 	     ) 	{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to open URL file %s", prod_path); 			 continue; }
			if ( DescribeCoverageChekURL(url, r, &URLh, &URLc) == FALSE )	{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to get Information from URL file %s", prod_path);	 continue; }

			for ( URLc = URLh; URLc != NULL; URLc = URLc->next) { 
				if ( XMLDescribeCoverageToDescCoverage(URLc->url, tmp, r) == FALSE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to perform XMLDescribeCoverageToDescCoverage for %s", prod_path); continue; }
			
	                        if ( *head == NULL ) {
	                                *head = *cursor        	= (DescCoverage)malloc(sizeof(struct sDescCoverage));
	                                memcpy(*cursor, tmp, sizeof(struct sDescCoverage));
					(*cursor)->label	= malloc(256); bzero((*cursor)->label, 255); strcpy((*cursor)->label, tmp->label);
	                                (*cursor)->next        	= NULL;
	                                continue;
	                        }

				if ( (*cursor)->epsg == tmp->epsg ) {
					if ( tmp->GeoX_ul < (*cursor)->GeoX_ul )	(*cursor)->GeoX_ul      = tmp->GeoX_ul;
		                        if ( tmp->GeoX_lr > (*cursor)->GeoX_lr )	(*cursor)->GeoX_lr      = tmp->GeoX_lr;
		                        if ( tmp->GeoY_ul > (*cursor)->GeoY_ul )	(*cursor)->GeoY_ul      = tmp->GeoY_ul;
	        	                if ( tmp->GeoY_lr < (*cursor)->GeoY_lr )	(*cursor)->GeoY_lr      = tmp->GeoY_lr;
	        	                if ( tmp->nband   > (*cursor)->nband   )	(*cursor)->nband        = tmp->nband;
	        	                if ( (*cursor)->t_min > tmp->t_min )       	(*cursor)->t_min        = tmp->t_min;
	        	                if ( (*cursor)->t_max < tmp->t_max )       	(*cursor)->t_max        = tmp->t_max;
					if ( tmp->x_res   < (*cursor)->x_res   ){	(*cursor)->x_res     	= tmp->x_res; (*cursor)->y_res = tmp->x_res * -1.0; }
	             									(*cursor)->hit_number  += tmp->hit_number;
       	                	        continue;
				}
	
	                        for( *cursor = *head; *cursor != NULL; *cursor = (*cursor)->next) if ( (*cursor)->epsg == tmp->epsg ) break;
	
	                        if ( *cursor == NULL ) {
	                                for( *cursor = *head; (*cursor)->next != NULL; *cursor = (*cursor)->next){}
	                                (*cursor)->next        	= malloc(sizeof(struct sDescCoverage)); *cursor = (*cursor)->next;
	                                memcpy(*cursor, tmp, sizeof(struct sDescCoverage));
					(*cursor)->label	= malloc(256); bzero((*cursor)->label, 254); strcpy((*cursor)->label, tmp->label);
	                                (*cursor)->next         = NULL;
	                                continue;
	                        }
	
	
	                        if ( tmp->GeoX_ul < (*cursor)->GeoX_ul )	(*cursor)->GeoX_ul      = tmp->GeoX_ul;
	                        if ( tmp->GeoX_lr > (*cursor)->GeoX_lr )	(*cursor)->GeoX_lr      = tmp->GeoX_lr;
	                        if ( tmp->GeoY_ul > (*cursor)->GeoY_ul )	(*cursor)->GeoY_ul      = tmp->GeoY_ul;
	                        if ( tmp->GeoY_lr < (*cursor)->GeoY_lr )	(*cursor)->GeoY_lr      = tmp->GeoY_lr;
	        	        if ( tmp->nband   > (*cursor)->nband   )	(*cursor)->nband        = tmp->nband;
        	                if ( (*cursor)->t_min > tmp->t_min )    	(*cursor)->t_min        = tmp->t_min;
        	                if ( (*cursor)->t_max < tmp->t_max )    	(*cursor)->t_max        = tmp->t_max;
				if ( tmp->x_res   < (*cursor)->x_res   ){	(*cursor)->x_res     	= tmp->x_res; (*cursor)->y_res = tmp->x_res * -1.0; }

				(*cursor)->hit_number  += tmp->hit_number;

			}
			continue;
		}
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Generation Virtual Collection using DescribeCoverage.json of %s", prod_path);

		//-------------------------------------------------------------------------------

		if ( generateProdInfo(prod_path, killable, r, token_from_user )) continue;
		bzero(imgs_path, MAX_PATH_LEN - 1);

		//-------------------------------------------------------------------------------
		// JSON NEW PART
		snprintf(imgs_path, MAX_PATH_LEN, "%s/DescribeCoverage.json", prod_path );
		DC = (*openSourceFile)( imgs_path, "r" );
		if ( DC == NULL ) continue;
		
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
				bzero( tmp->label, 255 );
				json_object_object_foreach( json_object_array_get_idx(jobj_dc, i) , key, val){
					if 	( ! strcmp(key, "id" 		) ) strcpy(tmp->label, json_object_get_string(val) );
					else if ( ! strcmp(key, "EPSG" 		) ) tmp->epsg 		= json_object_get_int(val);
					else if ( ! strcmp(key, "GeoY_lr" 	) ) tmp->GeoY_lr	= json_object_get_double(val);
					else if ( ! strcmp(key, "GeoX_ul" 	) ) tmp->GeoX_ul 	= json_object_get_double(val);
					else if ( ! strcmp(key, "GeoY_ul" 	) ) tmp->GeoY_ul 	= json_object_get_double(val);
					else if ( ! strcmp(key, "GeoX_lr" 	) ) tmp->GeoX_lr 	= json_object_get_double(val);
					else if ( ! strcmp(key, "x_res" 	) ) tmp->x_res	 	= json_object_get_double(val);
					else if ( ! strcmp(key, "y_res" 	) ) tmp->y_res	 	= json_object_get_double(val);
					else if ( ! strcmp(key, "nband" 	) ) tmp->nband	 	= json_object_get_int(val);
					else if ( ! strcmp(key, "t_min" 	) ) tmp->t_min	 	= json_object_get_int(val);
					else if ( ! strcmp(key, "t_max" 	) ) tmp->t_max	 	= json_object_get_int(val);
					else if ( ! strcmp(key, "high_min" 	) ) tmp->high_min	= json_object_get_double(val);
					else if ( ! strcmp(key, "high_max" 	) ) tmp->high_max	= json_object_get_double(val);
					else if ( ! strcmp(key, "type" 		) ) tmp->type	 	= json_object_get_int(val);
					else if ( ! strcmp(key, "hit_number" 	) ) tmp->hit_number 	= json_object_get_int(val);
					else continue; 

				}
				if ( strchr(tmp->label, '@') ) { bzero(tmp->label, 255); strcpy(tmp->label, "virt"); }

	                        if ( *head == NULL ) {
	                                *head = *cursor        	= (DescCoverage)malloc(sizeof(struct sDescCoverage));
	                                memcpy(*cursor, tmp, sizeof(struct sDescCoverage));
					(*cursor)->label	= malloc(256); bzero((*cursor)->label, 255); strcpy((*cursor)->label, tmp->label);
	                                (*cursor)->next        	= NULL;
	                                continue;
	                        }

				if ( (*cursor)->epsg == tmp->epsg ) {
					if ( tmp->GeoX_ul < (*cursor)->GeoX_ul )	(*cursor)->GeoX_ul      = tmp->GeoX_ul;
        	                        if ( tmp->GeoX_lr > (*cursor)->GeoX_lr )	(*cursor)->GeoX_lr      = tmp->GeoX_lr;
        	                        if ( tmp->GeoY_ul > (*cursor)->GeoY_ul )	(*cursor)->GeoY_ul      = tmp->GeoY_ul;
                	                if ( tmp->GeoY_lr < (*cursor)->GeoY_lr )	(*cursor)->GeoY_lr      = tmp->GeoY_lr;
                	                if ( tmp->nband   > (*cursor)->nband   )	(*cursor)->nband        = tmp->nband;
                	                if ( (*cursor)->t_min > tmp->t_min )       	(*cursor)->t_min        = tmp->t_min;
                	                if ( (*cursor)->t_max < tmp->t_max )       	(*cursor)->t_max        = tmp->t_max;
					if ( tmp->x_res   < (*cursor)->x_res   ){	(*cursor)->x_res     	= tmp->x_res; (*cursor)->y_res = tmp->x_res * -1.0; }
	               	            							(*cursor)->hit_number  += tmp->hit_number;
	                       	        continue;
				}


	                        for( *cursor = *head; *cursor != NULL; *cursor = (*cursor)->next) if ( (*cursor)->epsg == tmp->epsg ) break;

	                        if ( *cursor == NULL ) {
	                                for( *cursor = *head; (*cursor)->next != NULL; *cursor = (*cursor)->next){}
	                                (*cursor)->next        	= malloc(sizeof(struct sDescCoverage)); *cursor = (*cursor)->next;
	                                memcpy(*cursor, tmp, sizeof(struct sDescCoverage));
					(*cursor)->label	= malloc(256); bzero((*cursor)->label, 254); strcpy((*cursor)->label, tmp->label);
	                                (*cursor)->next         = NULL;
	                                continue;
	                        }


	                        if ( tmp->GeoX_ul < (*cursor)->GeoX_ul )	(*cursor)->GeoX_ul      = tmp->GeoX_ul;
	                        if ( tmp->GeoX_lr > (*cursor)->GeoX_lr )	(*cursor)->GeoX_lr      = tmp->GeoX_lr;
	                        if ( tmp->GeoY_ul > (*cursor)->GeoY_ul )	(*cursor)->GeoY_ul      = tmp->GeoY_ul;
	                        if ( tmp->GeoY_lr < (*cursor)->GeoY_lr )	(*cursor)->GeoY_lr      = tmp->GeoY_lr;
                	        if ( tmp->nband   > (*cursor)->nband   )	(*cursor)->nband        = tmp->nband;
	                        if ( (*cursor)->t_min > tmp->t_min )    	(*cursor)->t_min        = tmp->t_min;
	                        if ( (*cursor)->t_max < tmp->t_max )    	(*cursor)->t_max        = tmp->t_max;
				if ( tmp->x_res   < (*cursor)->x_res   ){	(*cursor)->x_res     	= tmp->x_res; (*cursor)->y_res = tmp->x_res * -1.0; }

				(*cursor)->hit_number  += tmp->hit_number;


			}

		}

	}
	(*closeSource)(dp);
	for( *cursor = *head; (*cursor) != NULL; *cursor = (*cursor)->next){
		for (i = 0; i < strlen( (*cursor)->label ); i++) if ( (*cursor)->label[i] == '@' ) break; 
		if ( i == strlen( (*cursor)->label ) ) { free((*cursor)->label); (*cursor)->label = NULL; }
		else	for (; i < strlen( (*cursor)->label ); i++)  (*cursor)->label[i] = '\0';
	}




	return 0;
}



//------------------------------------------------------------------------------------------------

int printCoverageDescriptions(request_rec *r, char *prod, DescCoverage head ){
	DescCoverage cursor 	= NULL;
	int time_index 		= 1;

	int	i;
	int 	proj;
	double 	lowerCorner[2];
	double	upperCorner[2];
	double	x_min_res;
	double	y_min_res;
	int	pxAOISizeX;
	int	pxAOISizeY;
	int	nband;
	time_t	t_min;
	time_t	t_max;
//	double	high_min;
//	double	high_max;
	int	type;
	int	hit_number;

                ap_set_content_type(r, "text/xml");
		ap_rprintf(r, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		ap_rprintf(r, "<wcs:CoverageDescriptions\n");
		ap_rprintf(r, "	xsi:schemaLocation='http://www.opengis.net/wcs/2.0 http://schemas.opengis.net/wcs/2.0/wcsAll.xsd'\n");
		ap_rprintf(r, "	xmlns:wcs='http://www.opengis.net/wcs/2.0' \n");
		ap_rprintf(r, "	xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'\n");
		ap_rprintf(r, "	xmlns:crs='http://www.opengis.net/wcs/service-extension/crs/1.0'\n");
		ap_rprintf(r, "	xmlns:ows='http://www.opengis.net/ows/2.0' xmlns:gml='http://www.opengis.net/gml/3.2' \n");
		ap_rprintf(r, "	xmlns:xlink='http://www.w3.org/1999/xlink'>\n");

	for ( cursor = head ; cursor != NULL ; cursor = cursor->next ) {
              
	        proj		= cursor->epsg;     
		lowerCorner[0]	= cursor->GeoY_lr;
	       	lowerCorner[1]	= cursor->GeoX_ul;
	       	upperCorner[0]	= cursor->GeoY_ul;
	       	upperCorner[1]	= cursor->GeoX_lr;
	       	x_min_res	= cursor->x_res;
	       	y_min_res	= cursor->y_res;
	       	pxAOISizeX	= cursor->pxAOISizeX;
	       	pxAOISizeY	= cursor->pxAOISizeY;
	       	nband		= cursor->nband;
	       	t_min		= cursor->t_min;
	       	t_max		= cursor->t_max;
//	       	high_min	= cursor->high_min;
//	       	high_max	= cursor->high_max;
	       	type		= cursor->type;
	       	hit_number	= cursor->hit_number;

		if ( proj != 4326 ) 	i = 1;
		else			i = 0;


		ap_rprintf(r, " <wcs:CoverageDescription\n");
		ap_rprintf(r, "	 gml:id='%s_%d_%g'\n", prod, cursor->epsg, cursor->x_res);
		ap_rprintf(r, "	 xmlns='http://www.opengis.net/gml/3.2'\n");
		ap_rprintf(r, "	 xmlns:gmlcov='http://www.opengis.net/gmlcov/1.0'\n");
		ap_rprintf(r, "	 xmlns:swe='http://www.opengis.net/swe/2.0'>\n");
		ap_rprintf(r, "  <boundedBy>\n");
                ap_rprintf(r, "    <Envelope srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s;\" axisLabels=\"%s %s %s\" uomLabels=\"%s %s %s\" srsDimension=\"3\">\n",
                                proj, time_URL[time_index], dimenstion_Label[i][0], dimenstion_Label[i][1], time_Label[time_index], dimenstion_unit[i], dimenstion_unit[i], time_unit[time_index]);
                ap_rprintf(r, "      <lowerCorner>%f %f %ld</lowerCorner>\n",   lowerCorner[0], lowerCorner[1], t_min);
                ap_rprintf(r, "      <upperCorner>%f %f %ld</upperCorner>\n",   upperCorner[0], upperCorner[1], t_max);
		ap_rprintf(r, "    </Envelope>\n");
		ap_rprintf(r, "  </boundedBy>\n\n");
		ap_rprintf(r, "  <domainSet>\n");
		ap_rprintf(r, "    <gmlrgrid:ReferenceableGridByVectors dimension=\"3\" gml:id=\"%s_%d_%g-grid\"\n",  prod, cursor->epsg, cursor->x_res );
		ap_rprintf(r, "		xmlns:gmlrgrid=\"http://www.opengis.net/gml/3.3/rgrid\"\n");
		ap_rprintf(r, "		xsi:schemaLocation=\"http://www.opengis.net/gml/3.3/rgrid http://schemas.opengis.net/gml/3.3/referenceableGrid.xsd\">\n");
		ap_rprintf(r, "      <limits>\n");
		ap_rprintf(r, "         <GridEnvelope>\n");
		ap_rprintf(r, "           <low>0 0 0</low>\n");
		ap_rprintf(r, "           <high>%d %d %d</high>\n", pxAOISizeX - 1, pxAOISizeY - 1, hit_number - 1);
		ap_rprintf(r, "         </GridEnvelope>\n");
		ap_rprintf(r, "      </limits>\n");
		ap_rprintf(r, "      <axisLabels>%s %s %s</axisLabels>\n", dimenstion_Label[i][1], dimenstion_Label[i][0], time_Label[time_index]);
		ap_rprintf(r, "      <gmlrgrid:origin>\n");
		ap_rprintf(r, "        <Point gml:id=\"%s_%d_%g-origin\" srsName=\"http://www.opengis.net/def/crs-compound?1=/http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s\" uomLabels=\"%s %s %s\" srsDimension=\"3\">\n",
				prod, cursor->epsg, cursor->x_res, proj, time_URL[time_index], dimenstion_Label[i][0], dimenstion_Label[i][1], time_Label[time_index], dimenstion_unit[i], dimenstion_unit[i], time_unit[time_index]);
		ap_rprintf(r, "          <pos>%f %f %ld</pos>\n", upperCorner[0] - (y_min_res/2), lowerCorner[1] - (x_min_res/2), t_min);
		ap_rprintf(r, "        </Point>\n");
		ap_rprintf(r, "      </gmlrgrid:origin>\n");
		ap_rprintf(r, "      <gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "        <gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "          <gmlrgrid:offsetVector srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s\" uomLabels=\"%s %s %s\" srsDimension=\"3\">0 %f 0</gmlrgrid:offsetVector>\n", 
				proj, time_URL[time_index], dimenstion_Label[i][0], dimenstion_Label[i][1], time_Label[time_index], dimenstion_unit[i], dimenstion_unit[i], time_unit[time_index], x_min_res);
		ap_rprintf(r, "          <gmlrgrid:coefficients />\n");
		ap_rprintf(r, "          <gmlrgrid:gridAxesSpanned>%s</gmlrgrid:gridAxesSpanned>\n", dimenstion_Label[i][1]);
		ap_rprintf(r, "          <gmlrgrid:sequenceRule axisOrder=\"+1\">Linear</gmlrgrid:sequenceRule>\n");
		ap_rprintf(r, "        </gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "      </gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "      <gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "        <gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "          <gmlrgrid:offsetVector srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s\" uomLabels=\"%s %s %s\" srsDimension=\"3\">%f 0 0</gmlrgrid:offsetVector>\n", 
				proj, time_URL[time_index], dimenstion_Label[i][0], dimenstion_Label[i][1], time_Label[time_index], dimenstion_unit[i], dimenstion_unit[i], time_unit[time_index], y_min_res);
		ap_rprintf(r, "          <gmlrgrid:coefficients />\n");
		ap_rprintf(r, "          <gmlrgrid:gridAxesSpanned>%s</gmlrgrid:gridAxesSpanned>\n", dimenstion_Label[i][0]);
		ap_rprintf(r, "          <gmlrgrid:sequenceRule axisOrder=\"+1\">Linear</gmlrgrid:sequenceRule>\n");
		ap_rprintf(r, "        </gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "      </gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "      <gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "        <gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "          <gmlrgrid:offsetVector srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s\" uomLabels=\"%s %s %s\" srsDimension=\"3\">0 0 1</gmlrgrid:offsetVector>\n",
				proj, time_URL[time_index], dimenstion_Label[i][0], dimenstion_Label[i][1], time_Label[time_index], dimenstion_unit[i], dimenstion_unit[i], time_unit[time_index]);
		ap_rprintf(r, "          <gmlrgrid:coefficients>\n");
		ap_rprintf(r, "          </gmlrgrid:coefficients>\n");
		ap_rprintf(r, "          <gmlrgrid:gridAxesSpanned>t</gmlrgrid:gridAxesSpanned>\n");
		ap_rprintf(r, "        <gmlrgrid:sequenceRule axisOrder=\"+1\">Linear</gmlrgrid:sequenceRule>\n");
		ap_rprintf(r, "      </gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "    </gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "  </gmlrgrid:ReferenceableGridByVectors>\n");
		ap_rprintf(r, "</domainSet>\n");
		ap_rprintf(r, "<gmlcov:rangeType>\n");
		ap_rprintf(r, "  <swe:DataRecord>\n");


		for ( i = 0; i < nband; i++){
		ap_rprintf(r, "    <swe:field name=\"band%d\">\n", i + 1);
		ap_rprintf(r, "        <swe:Quantity definition=\"%s\">\n", OGC_dataTypes[type]);
		ap_rprintf(r, "          <swe:label>%s</swe:label>\n", basename(OGC_dataTypes[type]));
		ap_rprintf(r, "          <swe:description>primitive</swe:description>\n");
		ap_rprintf(r, "          <swe:uom code=\"10^0\" />\n");
		ap_rprintf(r, "          <swe:constraint>\n");
		ap_rprintf(r, "            <swe:AllowedValues>\n");
		ap_rprintf(r, "              <swe:interval>-3.4028234E+38 3.4028234E+38</swe:interval>\n");
		ap_rprintf(r, "            </swe:AllowedValues>\n");
		ap_rprintf(r, "          </swe:constraint>\n");
		ap_rprintf(r, "        </swe:Quantity>\n");
		ap_rprintf(r, "      </swe:field>\n");
		}



		ap_rprintf(r, "    </swe:DataRecord>\n");
		ap_rprintf(r, "  </gmlcov:rangeType>\n");
		ap_rprintf(r, "  <wcs:ServiceParameters>\n");
		ap_rprintf(r, "  <wcs:CoverageSubtype>RectifiedGridCoverage</wcs:CoverageSubtype>\n");
		ap_rprintf(r, "    <CoverageSubtypeParent xmlns=\"http://www.opengis.net/wcs/2.0\">\n");
		ap_rprintf(r, "      <CoverageSubtype>AbstractDiscreteCoverage</CoverageSubtype>\n");
		ap_rprintf(r, "        <CoverageSubtypeParent>\n");
		ap_rprintf(r, "          <CoverageSubtype>AbstractCoverage</CoverageSubtype>\n");
		ap_rprintf(r, "        </CoverageSubtypeParent>\n");
		ap_rprintf(r, "      </CoverageSubtypeParent>\n");
		ap_rprintf(r, "      <wcs:nativeFormat>application/octet-stream</wcs:nativeFormat>\n");
		ap_rprintf(r, "    </wcs:ServiceParameters>\n");
		ap_rprintf(r, "  </wcs:CoverageDescription>\n");
	}
		ap_rprintf(r, "</wcs:CoverageDescriptions>\n");


	return 0;
}
//------------------------------------------------------------------------------------------------


int printNoCoverageFound(request_rec *r, char *prod){
	ap_set_content_type(r, "text/xml");

        ap_rprintf(r, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
	ap_rprintf(r, "<ows:ExceptionReport version='2.0.0'\n");
	ap_rprintf(r, "    xsd:schemaLocation='http://www.opengis.net/ows/2.0 http://schemas.opengis.net/ows/2.0/owsExceptionReport.xsd'\n");
	ap_rprintf(r, "    xmlns:ows='http://www.opengis.net/ows/2.0' xmlns:xsd='http://www.w3.org/2001/XMLSchema-instance' xmlns:xlink='http://www.w3.org/1999/xlink'>\n");
	ap_rprintf(r, "    <ows:Exception locator='%s' exceptionCode='NoSuchCoverage'>\n", prod);
	ap_rprintf(r, "        <ows:ExceptionText>One of the identifiers passed does not match with any of the coverages offered by this server</ows:ExceptionText>\n");
	ap_rprintf(r, "    </ows:Exception>\n");
	ap_rprintf(r, "</ows:ExceptionReport>\n");

	return 0;
}


//------------------------------------------------------------------------------------------------

int MergingFunc(request_rec *r, int MERGING_IMAGE, int size, int band, block cursor, double *raster, double *avg, int *hit_for_avg){
	int i, j;
	int k = band;
	if ( MERGING_IMAGE == AVERAGE ) {
		for (i = 0; i < size; i++ ) {
			j = i + ( size * k);
			if ( raster[i] 	== cursor->nodata[k] )	{ continue; }
			if ( avg[j] 	== cursor->nodata[k] )	{ avg[j] = raster[i]; continue; }
			avg[j] += raster[i];
			hit_for_avg[j]++;
		}					
	} else if ( MERGING_IMAGE == OVERLAP ) {
		for (i = 0; i < size; i++ ) {
			j = i + ( size * k);
			if ( avg[j] == cursor->nodata[k] ) avg[j] = raster[i]; 
		}					
	} else if ( MERGING_IMAGE == MOSTRECENT ) {
		for (i = 0; i < size; i++ ) {
			j = i + ( size * k);
			if ( raster[i]  == cursor->nodata[k] )  { 			continue; }
			if ( avg[j]     == cursor->nodata[k] )  { avg[j] = raster[i]; 	continue; }
			if ( raster[i]  != cursor->nodata[k] )  { avg[j] = raster[i];   continue; }
		}					
	} else if ( MERGING_IMAGE == MINVALUE ) {
		for (i = 0; i < size; i++ ) {
			j = i + ( size * k);
			if ( raster[i]  == cursor->nodata[k] )  { 			continue; }
			if ( avg[j]     == cursor->nodata[k] )  { avg[j] = raster[i]; 	continue; }
			if ( ( raster[i]  != cursor->nodata[k] ) && ( raster[i] < avg[j] ) ) { avg[j] = raster[i]; continue; }
		}					
	} else if ( MERGING_IMAGE == MAXVALUE ) {
		for (i = 0; i < size; i++ ) {
			j = i + ( size * k);
			if ( raster[i]  == cursor->nodata[k] )  { 			continue; }
			if ( avg[j]     == cursor->nodata[k] )  { avg[j] = raster[i]; 	continue; }
			if ( ( raster[i]  != cursor->nodata[k] ) && ( raster[i] > avg[j] ) ) { avg[j] = raster[i]; continue; }
		}					
	} else { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Merging image method!"); return 500; }
	return 0;
}


//------------------------------------------------------------------------------------------------

void *MathComputationUnit(void *data){
	int i, j, k, err;
	char	*s		= NULL;
	int 	depth		= 0;
	double	x		= 0;
        double  *a  		= NULL;
        double  *b  		= NULL;
        int 	asz  		= 0;
        int	bsz  		= 0;
	int 	targetBand 	= 0;
	int	act		= AVG;
	int	*hit_for_avg	= NULL;
	GByte	*mathmask	= NULL;
	double	*raster		= NULL;

	double	**stackdb	= NULL;
	int	*stacksz	= NULL;
	char 	*e		= NULL;
	block 	cursor		= NULL;
	block	tmp		= NULL;
	char imgs_path[MAX_PATH_LEN];

	struct mathParamters *params 		= (struct mathParamters *)data;
	struct info 	 *info                  = params->info; request_rec *r = info->r;
	int		 pxAOISizeX		= params->pxAOISizeX;
	int		 pxAOISizeY		= params->pxAOISizeY;
	struct 	mathUnit *mathcur		= params->mathcur;
	time_t           *time_serie		= params->time_serie;
	block		 head			= params->head;
	block            vrt_cursor      	= params->vrt_cursor;

	int		 time_serie_index	= params->time_serie_index;
	int     	 *layers                = params->layers;
	int		 layer_index		= params->layer_index;
	int		 nband			= params->nband;
	int		 MERGING_IMAGE		= params->MERGING_IMAGE;

	GDALRasterBandH  hBandSrc;


	s		= (char    *)malloc(sizeof(char)     * MAX_STR_LEN);	
	stackdb 	= (double **)malloc(sizeof(double *) * MAX_D); 			 for ( i = 1 ; i < MAX_D; i++ ) stackdb[i] = NULL;
	stacksz 	= (int     *)malloc(sizeof(int)      * MAX_D); 			
	hit_for_avg	= (int 	   *)malloc(sizeof(int)      * pxAOISizeX * pxAOISizeY); 
	mathmask 	= (GByte   *)malloc(sizeof(GByte)    * pxAOISizeX * pxAOISizeY); 

	if ( s		  == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc s"); 		pthread_exit( (void *)1 ); }
	if ( stackdb 	  == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc stackdb"); 	pthread_exit( (void *)1 ); }
	if ( stacksz 	  == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc stacksz"); 	pthread_exit( (void *)1 ); }
	if ( hit_for_avg  == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc hit_for_avg"); 	pthread_exit( (void *)1 ); }
	if ( mathmask 	  == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc mathmask"); 	pthread_exit( (void *)1 ); }

	// Initi math mask
	for ( i = 0 ; i < (pxAOISizeX * pxAOISizeY ); i++ ) mathmask[i] = TRUE;

	// fprintf(stderr, "INFO START MATH FOR %ld\n", time_serie[time_serie_index] ); fflush(stderr);

	for ( j = 0; j < mathcur->l_queue; j++){
		bzero(s, MAX_STR_LEN - 1); strncpy(s, mathcur->queue[j].s, mathcur->queue[j].len);
                x = strtod(s, &e);

//		fprintf(stderr, "INFO: %s %d\n", s, depth); fflush(stderr);

                if 	(e > s)		{
						if (depth >= MAX_D) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack overflow e > s"); pthread_exit( (void *)1 ); }
						stackdb[depth] 		= (double *) malloc(sizeof(double)); 
						if ( stackdb[depth] == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc stackdb[%d]",  depth); pthread_exit( (void *)1 ); }
						stackdb[depth][0] 	= x;
						stacksz[depth] 		= 1;
						depth++;
					}
	        else if (*s == '+')    	
					{
						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow +"); pthread_exit( (void *)1 ); } depth--; b = stackdb[depth]; bsz = stacksz[depth];
						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow +"); pthread_exit( (void *)1 ); } depth--; a = stackdb[depth]; asz = stacksz[depth];
						raster = (double *)malloc( sizeof(double) * ( bsz > asz ? bsz : asz ) ); 
						if ( raster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc raster +"); pthread_exit( (void *)1 ); }

						if 	( bsz == asz ) for (i = 0; i < bsz ; i++ ) raster[i] = a[i] + b[i]; 
						else if ( bsz >  asz ) for (i = 0; i < bsz ; i++ ) raster[i] = a[0] + b[i];
						else if ( bsz <  asz ) for (i = 0; i < asz ; i++ ) raster[i] = a[i] + b[0];
						free(stackdb[depth]); free(stackdb[depth+1]); stackdb[depth] = NULL; stackdb[depth+1] = NULL;
						stackdb[depth] = raster;
						stacksz[depth] = ( bsz > asz ? bsz : asz );
						depth++;


					}
	        else if (*s == '-')    	
					{ 

						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow -"); pthread_exit( (void *)1 ); } depth--; b = stackdb[depth]; bsz = stacksz[depth];
						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow -"); pthread_exit( (void *)1 ); } depth--; a = stackdb[depth]; asz = stacksz[depth];
						raster = (double *)malloc( sizeof(double) * ( bsz > asz ? bsz : asz ) );
						if ( raster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc raster -"); pthread_exit( (void *)1 ); }

						if 	( bsz == asz ) for (i = 0; i < bsz ; i++ ) raster[i] = a[i] - b[i];
						else if ( bsz >  asz ) for (i = 0; i < bsz ; i++ ) raster[i] = a[0] - b[i];
						else if ( bsz <  asz ) for (i = 0; i < asz ; i++ ) raster[i] = a[i] - b[0];
						free(stackdb[depth]); free(stackdb[depth+1]); stackdb[depth] = NULL; stackdb[depth+1] = NULL;
						stackdb[depth] = raster;
						stacksz[depth] = ( bsz > asz ? bsz : asz );
						depth++;


					}
	        else if (*s == '/')   	
					{ 

						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow /"); pthread_exit( (void *)1 ); } depth--; b = stackdb[depth]; bsz = stacksz[depth];
						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow /"); pthread_exit( (void *)1 ); } depth--; a = stackdb[depth]; asz = stacksz[depth];
						raster = (double *)malloc( sizeof(double) * ( bsz > asz ? bsz : asz ) );
						if ( raster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc raster /"); pthread_exit( (void *)1 ); }

						if 	( bsz == asz ) for (i = 0; i < bsz ; i++ ) raster[i] = a[i] / b[i];
						else if ( bsz >  asz ) for (i = 0; i < bsz ; i++ ) raster[i] = a[0] / b[i];
						else if ( bsz <  asz ) for (i = 0; i < asz ; i++ ) raster[i] = a[i] / b[0];
						free(stackdb[depth]); free(stackdb[depth+1]); stackdb[depth] = NULL; stackdb[depth+1] = NULL;
						stackdb[depth] = raster;
						stacksz[depth] = ( bsz > asz ? bsz : asz );
						depth++;
					}
	        else if ( (*s == '^') || (( *s == '*' ) && ( *(s+1) == '*' )) )
				 	{ 

						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow ^ **"); pthread_exit( (void *)1 ); } depth--; b = stackdb[depth]; bsz = stacksz[depth];
						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow ^ **"); pthread_exit( (void *)1 ); } depth--; a = stackdb[depth]; asz = stacksz[depth];
						raster = (double *)malloc( sizeof(double) * ( bsz > asz ? bsz : asz ) );
						if ( raster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc raster ^ *"); pthread_exit( (void *)1 ); }

						if 	( bsz == asz ) for (i = 0; i < bsz ; i++ ) raster[i] = pow( a[i] , b[i] );
						else if ( bsz >  asz ) for (i = 0; i < bsz ; i++ ) raster[i] = pow( a[0] , b[i] );
						else if ( bsz <  asz ) for (i = 0; i < asz ; i++ ) raster[i] = pow( a[i] , b[0] );
						free(stackdb[depth]); free(stackdb[depth+1]); stackdb[depth] = NULL; stackdb[depth+1] = NULL;
						stackdb[depth] = raster;
						stacksz[depth] = ( bsz > asz ? bsz : asz );
						depth++;


					}
 	        else if (*s == '*')    	
					{

						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow *"); pthread_exit( (void *)1 ); } depth--; b = stackdb[depth]; bsz = stacksz[depth];
						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow *"); pthread_exit( (void *)1 ); } depth--; a = stackdb[depth]; asz = stacksz[depth];
						raster = (double *)malloc( sizeof(double) * ( bsz > asz ? bsz : asz ) );
						if ( raster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc raster *"); pthread_exit( (void *)1 ); }

						if 	( bsz == asz ) for (i = 0; i < bsz ; i++ ) raster[i] = a[i] * b[i];
						else if ( bsz >  asz ) for (i = 0; i < bsz ; i++ ) raster[i] = a[0] * b[i];
						else if ( bsz <  asz ) for (i = 0; i < asz ; i++ ) raster[i] = a[i] * b[0];
						free(stackdb[depth]); free(stackdb[depth+1]); stackdb[depth] = NULL; stackdb[depth+1] = NULL;
						stackdb[depth] = raster;
						stacksz[depth] = ( bsz > asz ? bsz : asz );
						depth++;


					}
	        else if (*s == '%')    	
					{ 

						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow %%"); pthread_exit( (void *)1 ); } depth--; b = stackdb[depth]; bsz = stacksz[depth];
						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow %%"); pthread_exit( (void *)1 ); } depth--; a = stackdb[depth]; asz = stacksz[depth];
						raster = (double *)malloc( sizeof(double) * ( bsz > asz ? bsz : asz ) );
						if ( raster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc raster %%"); pthread_exit( (void *)1 ); }

						if 	( bsz == asz ) for (i = 0; i < bsz ; i++ ) raster[i] = fmod( a[i] , b[i] );
						else if ( bsz >  asz ) for (i = 0; i < bsz ; i++ ) raster[i] = fmod( a[0] , b[i] );
						else if ( bsz <  asz ) for (i = 0; i < asz ; i++ ) raster[i] = fmod( a[i] , b[0] );
						free(stackdb[depth]); free(stackdb[depth+1]); stackdb[depth] = NULL; stackdb[depth+1] = NULL;
						stackdb[depth] = raster;
						stacksz[depth] = ( bsz > asz ? bsz : asz );
						depth++;
					}

	        else if ( (*s == '>') && ( *(s+1) == '>' ) )
					{ 

						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow >>"); pthread_exit( (void *)1 ); } depth--; b = stackdb[depth]; bsz = stacksz[depth];
						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow >>"); pthread_exit( (void *)1 ); } depth--; a = stackdb[depth]; asz = stacksz[depth];
						raster = (double *)malloc( sizeof(double) * ( bsz > asz ? bsz : asz ) );
						if ( raster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc raster >>"); pthread_exit( (void *)1 ); }

						if 	( bsz == asz ) for (i = 0; i < bsz ; i++ ) raster[i] = (long int)a[i] >> (long int)b[i];
						else if ( bsz >  asz ) for (i = 0; i < bsz ; i++ ) raster[i] = (long int)a[0] >> (long int)b[i];
						else if ( bsz <  asz ) for (i = 0; i < asz ; i++ ) raster[i] = (long int)a[i] >> (long int)b[0];

						free(stackdb[depth]); free(stackdb[depth+1]); stackdb[depth] = NULL; stackdb[depth+1] = NULL;
						stackdb[depth] = raster;
						stacksz[depth] = ( bsz > asz ? bsz : asz );
						depth++;
					}

	        else if ( (*s == '<') && ( *(s+1) == '<' ) )
					{ 

						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow <<"); pthread_exit( (void *)1 ); } depth--; b = stackdb[depth]; bsz = stacksz[depth];
						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow <<"); pthread_exit( (void *)1 ); } depth--; a = stackdb[depth]; asz = stacksz[depth];
						raster = (double *)malloc( sizeof(double) * ( bsz > asz ? bsz : asz ) );
						if ( raster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc raster <<"); pthread_exit( (void *)1 ); }

						if 	( bsz == asz ) for (i = 0; i < bsz ; i++ ) raster[i] = (long int)a[i] << (long int)b[i];
						else if ( bsz >  asz ) for (i = 0; i < bsz ; i++ ) raster[i] = (long int)a[0] << (long int)b[i];
						else if ( bsz <  asz ) for (i = 0; i < asz ; i++ ) raster[i] = (long int)a[i] << (long int)b[0];

						free(stackdb[depth]); free(stackdb[depth+1]); stackdb[depth] = NULL; stackdb[depth+1] = NULL;
						stackdb[depth] = raster;
						stacksz[depth] = ( bsz > asz ? bsz : asz );
						depth++;
					}

	        else if (*s == '&')
					{ 

						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow &"); pthread_exit( (void *)1 ); } depth--; b = stackdb[depth]; bsz = stacksz[depth];
						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow &"); pthread_exit( (void *)1 ); } depth--; a = stackdb[depth]; asz = stacksz[depth];
						raster = (double *)malloc( sizeof(double) * ( bsz > asz ? bsz : asz ) );
						if ( raster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc raster &"); pthread_exit( (void *)1 ); }

						if 	( bsz == asz ) for (i = 0; i < bsz ; i++ ) raster[i] = (long int)a[i] & (long int)b[i];
						else if ( bsz >  asz ) for (i = 0; i < bsz ; i++ ) raster[i] = (long int)a[0] & (long int)b[i];
						else if ( bsz <  asz ) for (i = 0; i < asz ; i++ ) raster[i] = (long int)a[i] & (long int)b[0];

						free(stackdb[depth]); free(stackdb[depth+1]); stackdb[depth] = NULL; stackdb[depth+1] = NULL;
						stackdb[depth] = raster;
						stacksz[depth] = ( bsz > asz ? bsz : asz );
						depth++;
					}

	        else if (*s == '|')
					{ 

						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow |"); pthread_exit( (void *)1 ); } depth--; b = stackdb[depth]; bsz = stacksz[depth];
						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow |"); pthread_exit( (void *)1 ); } depth--; a = stackdb[depth]; asz = stacksz[depth];
						raster = (double *)malloc( sizeof(double) * ( bsz > asz ? bsz : asz ) );
						if ( raster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc raster |"); pthread_exit( (void *)1 ); }

						if 	( bsz == asz ) for (i = 0; i < bsz ; i++ ) raster[i] = (long int)a[i] | (long int)b[i];
						else if ( bsz >  asz ) for (i = 0; i < bsz ; i++ ) raster[i] = (long int)a[0] | (long int)b[i];
						else if ( bsz <  asz ) for (i = 0; i < asz ; i++ ) raster[i] = (long int)a[i] | (long int)b[0];

						free(stackdb[depth]); free(stackdb[depth+1]); stackdb[depth] = NULL; stackdb[depth+1] = NULL;
						stackdb[depth] = raster;
						stacksz[depth] = ( bsz > asz ? bsz : asz );
						depth++;
					}

		else if (*s == '>')    	
					{ 
						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow >"); pthread_exit( (void *)1 ); } depth--; b = stackdb[depth]; bsz = stacksz[depth];
						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow >"); pthread_exit( (void *)1 ); } depth--; a = stackdb[depth]; asz = stacksz[depth];
						raster = (double *)malloc( sizeof(double) * ( bsz > asz ? bsz : asz ) );
						if ( raster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc raster >"); pthread_exit( (void *)1 ); }

						if ( *(s+1) == '=' ){
						if 	( bsz == asz ) for (i = 0; i < bsz ; i++ ) raster[i] = a[i] >  b[i] ? 1 : 0;
						else if ( bsz >  asz ) for (i = 0; i < bsz ; i++ ) raster[i] = a[0] >  b[i] ? 1 : 0;
						else if ( bsz <  asz ) for (i = 0; i < asz ; i++ ) raster[i] = a[i] >  b[0] ? 1 : 0;
						}else{
						if 	( bsz == asz ) for (i = 0; i < bsz ; i++ ) raster[i] = a[i] >= b[i] ? 1 : 0;
						else if ( bsz >  asz ) for (i = 0; i < bsz ; i++ ) raster[i] = a[0] >= b[i] ? 1 : 0;
						else if ( bsz <  asz ) for (i = 0; i < asz ; i++ ) raster[i] = a[i] >= b[0] ? 1 : 0;
						}
						free(stackdb[depth]); free(stackdb[depth+1]); stackdb[depth] = NULL; stackdb[depth+1] = NULL;
						stackdb[depth] = raster;
						stacksz[depth] = ( bsz > asz ? bsz : asz );
						depth++;
					}

	        else if (*s == '<')    	
					{ 

						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow <"); pthread_exit( (void *)1 ); } depth--; b = stackdb[depth]; bsz = stacksz[depth];
						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow <"); pthread_exit( (void *)1 ); } depth--; a = stackdb[depth]; asz = stacksz[depth];
						raster = (double *)malloc( sizeof(double) * ( bsz > asz ? bsz : asz ) );
						if ( raster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc raster <"); pthread_exit( (void *)1 ); }

						if ( *(s+1) == '=' ){
						if 	( bsz == asz ) for (i = 0; i < bsz ; i++ ) raster[i] = a[i] <  b[i] ? 1 : 0;
						else if ( bsz >  asz ) for (i = 0; i < bsz ; i++ ) raster[i] = a[0] <  b[i] ? 1 : 0;
						else if ( bsz <  asz ) for (i = 0; i < asz ; i++ ) raster[i] = a[i] <  b[0] ? 1 : 0;
						}else{
						if 	( bsz == asz ) for (i = 0; i < bsz ; i++ ) raster[i] = a[i] <= b[i] ? 1 : 0;
						else if ( bsz >  asz ) for (i = 0; i < bsz ; i++ ) raster[i] = a[0] <= b[i] ? 1 : 0;
						else if ( bsz <  asz ) for (i = 0; i < asz ; i++ ) raster[i] = a[i] <= b[0] ? 1 : 0;
						}
						free(stackdb[depth]); free(stackdb[depth+1]); stackdb[depth] = NULL; stackdb[depth+1] = NULL;
						stackdb[depth] = raster;
						stacksz[depth] = ( bsz > asz ? bsz : asz );
						depth++;
					}

	        else if ( (*s == '!') && ( *(s+1) == '=' ) )
					{ 

						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow !="); pthread_exit( (void *)1 ); } depth--; b = stackdb[depth]; bsz = stacksz[depth];
						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow !="); pthread_exit( (void *)1 ); } depth--; a = stackdb[depth]; asz = stacksz[depth];
						raster = (double *)malloc( sizeof(double) * ( bsz > asz ? bsz : asz ) );
						if ( raster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc raster !="); pthread_exit( (void *)1 ); }

						if 	( bsz == asz ) for (i = 0; i < bsz ; i++ ) raster[i] = a[i] != b[i] ? 1 : 0;
						else if ( bsz >  asz ) for (i = 0; i < bsz ; i++ ) raster[i] = a[0] != b[i] ? 1 : 0;
						else if ( bsz <  asz ) for (i = 0; i < asz ; i++ ) raster[i] = a[i] != b[0] ? 1 : 0;

						free(stackdb[depth]); free(stackdb[depth+1]); stackdb[depth] = NULL; stackdb[depth+1] = NULL;
						stackdb[depth] = raster;
						stacksz[depth] = ( bsz > asz ? bsz : asz );
						depth++;
					}

	        else if ( (*s == '=') && ( *(s+1) == '=' ) )
					{ 

						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow =="); pthread_exit( (void *)1 ); } depth--; b = stackdb[depth]; bsz = stacksz[depth];
						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow =="); pthread_exit( (void *)1 ); } depth--; a = stackdb[depth]; asz = stacksz[depth];
						raster = (double *)malloc( sizeof(double) * ( bsz > asz ? bsz : asz ) );
						if ( raster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc raster =="); pthread_exit( (void *)1 ); }

						if 	( bsz == asz ) for (i = 0; i < bsz ; i++ ) raster[i] = a[i] == b[i] ? 1 : 0;
						else if ( bsz >  asz ) for (i = 0; i < bsz ; i++ ) raster[i] = a[0] == b[i] ? 1 : 0;
						else if ( bsz <  asz ) for (i = 0; i < asz ; i++ ) raster[i] = a[i] == b[0] ? 1 : 0;

						free(stackdb[depth]); free(stackdb[depth+1]); stackdb[depth] = NULL; stackdb[depth+1] = NULL;
						stackdb[depth] = raster;
						stacksz[depth] = ( bsz > asz ? bsz : asz );
						depth++;
					}
	        else if ( ! strcmp(s, "AND") )
					{ 
						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow AND"); pthread_exit( (void *)1 ); } depth--; b = stackdb[depth]; bsz = stacksz[depth];
						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow AND"); pthread_exit( (void *)1 ); } depth--; a = stackdb[depth]; asz = stacksz[depth];
						raster = (double *)malloc( sizeof(double) * ( bsz > asz ? bsz : asz ) );
						if ( raster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc raster AND"); pthread_exit( (void *)1 ); }

						if 	( bsz == asz ) for (i = 0; i < bsz ; i++ ) raster[i] = ( a[i] == 1 ) && ( b[i] == 1 ) ? 1 : 0;
						else if ( bsz >  asz ) for (i = 0; i < bsz ; i++ ) raster[i] = ( a[0] == 1 ) && ( b[i] == 1 ) ? 1 : 0;
						else if ( bsz <  asz ) for (i = 0; i < asz ; i++ ) raster[i] = ( a[i] == 1 ) && ( b[0] == 1 ) ? 1 : 0;

						free(stackdb[depth]); free(stackdb[depth+1]); stackdb[depth] = NULL; stackdb[depth+1] = NULL;
						stackdb[depth] = raster;
						stacksz[depth] = ( bsz > asz ? bsz : asz );
						depth++;
					}
	        else if ( ! strcmp(s, "OR") )
					{ 

						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow OR"); pthread_exit( (void *)1 ); } depth--; b = stackdb[depth]; bsz = stacksz[depth];
						if (!depth) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow OR"); pthread_exit( (void *)1 ); } depth--; a = stackdb[depth]; asz = stacksz[depth];
						raster = (double *)malloc( sizeof(double) * ( bsz > asz ? bsz : asz ) );
						if ( raster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc raster OR"); pthread_exit( (void *)1 ); }

						if 	( bsz == asz ) for (i = 0; i < bsz ; i++ ) raster[i] = ( a[i] == 1 ) || ( b[i] == 1 ) ? 1 : 0;
						else if ( bsz >  asz ) for (i = 0; i < bsz ; i++ ) raster[i] = ( a[0] == 1 ) || ( b[i] == 1 ) ? 1 : 0;
						else if ( bsz <  asz ) for (i = 0; i < asz ; i++ ) raster[i] = ( a[i] == 1 ) || ( b[0] == 1 ) ? 1 : 0;

						free(stackdb[depth]); free(stackdb[depth+1]); stackdb[depth] = NULL; stackdb[depth+1] = NULL;
						stackdb[depth] = raster;
						stacksz[depth] = ( bsz > asz ? bsz : asz );
						depth++;
					}

 	        else {

			if (depth >= MAX_D) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack overflow value"); pthread_exit( (void *)1 ); }
			for( cursor = head; cursor != NULL; cursor = cursor->next) if ( ( ! strncmp(cursor->math_id, mathcur->queue[j].s, mathcur->queue[j].len) ) && ( cursor->time == time_serie[time_serie_index] ) ) { tmp = cursor; break; }
			if ( cursor == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math collection %s not found", s); GoodbyeMessage(info, "Math collection %s not found", s); pthread_exit( (void *)2 ); }

			// I need to find a method to get the band
			targetBand = 0;

			stacksz[depth] 	= pxAOISizeX * pxAOISizeY;
			stackdb[depth] 	= (double *) malloc(sizeof(double) * stacksz[depth]); if ( stackdb[depth] == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc stackdb[%d]",  depth); pthread_exit( (void *)1 ); }
			hBandSrc 	= GDALGetRasterBand(tmp->dataset, targetBand + 1 );
			err = GDALRasterIO( hBandSrc, GF_Read,  0, 0,  pxAOISizeX, pxAOISizeY, stackdb[depth], pxAOISizeX, pxAOISizeY, GDT_Float64, 	0, 0);
			for ( i = 0 ; i < stacksz[depth]; i++ ) { if ( stackdb[depth][i] == tmp->nodata[targetBand] ) mathmask[i] = FALSE; hit_for_avg[i] = 1; }
			if ( raster == NULL) free(raster); raster = (double *) malloc(sizeof(double) * stacksz[depth]); if ( raster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math malloc raster"); pthread_exit( (void *)1 ); }


			for( cursor = tmp->next; cursor != NULL; cursor = cursor->next) {
				if ( strncmp(cursor->math_id, mathcur->queue[j].s, mathcur->queue[j].len) 	) continue;
				if ( cursor->time != time_serie[time_serie_index] 				) continue;
							
				hBandSrc = GDALGetRasterBand(cursor->dataset, targetBand + 1 );
				err = GDALRasterIO( hBandSrc, GF_Read,  0, 0,  pxAOISizeX, pxAOISizeY, raster, pxAOISizeX, pxAOISizeY, GDT_Float64, 	0, 0);

				if ( MERGING_IMAGE == AVERAGE ) {
					for (i = 0; i < stacksz[depth]; i++ ) {
						if ( raster[i] 		== cursor->nodata[targetBand] )	{ continue; }
						if ( stackdb[depth][i] 	== cursor->nodata[targetBand] )	{ stackdb[depth][i] = raster[i]; mathmask[i] = TRUE; continue; }
						stackdb[depth][i] += raster[i];
						hit_for_avg[i]++;
					}
				} else if ( MERGING_IMAGE == OVERLAP ) {
					for (i = 0; i < stacksz[depth]; i++ ) {
						if ( stackdb[depth][i]  != cursor->nodata[targetBand] ) { continue; }
						if ( raster[i]          == cursor->nodata[targetBand] ) { continue; }
						if ( stackdb[depth][i] 	== cursor->nodata[targetBand] ) { stackdb[depth][i] = raster[i]; mathmask[i] = TRUE; }
					}

				} else if ( MERGING_IMAGE == MOSTRECENT ) {
					for (i = 0; i < stacksz[depth]; i++ ) {
						if ( raster[i] 		== cursor->nodata[targetBand] )	{ continue; }
						if ( stackdb[depth][i] 	== cursor->nodata[targetBand] )	{ stackdb[depth][i] = raster[i]; mathmask[i] = TRUE; continue; }
						stackdb[depth][i] = raster[i];
					}
				} else if ( MERGING_IMAGE == MINVALUE ) {
					for (i = 0; i < stacksz[depth]; i++ ) {
						if ( raster[i] 		== cursor->nodata[targetBand] )	{ continue; }
						if ( stackdb[depth][i] 	== cursor->nodata[targetBand] )	{ stackdb[depth][i] = raster[i]; mathmask[i] = TRUE; continue; }
						if ( raster[i]		<  stackdb[depth][i]	      ) { stackdb[depth][i] = raster[i]; }
					}
				} else if ( MERGING_IMAGE == MAXVALUE ) {
					for (i = 0; i < stacksz[depth]; i++ ) {
						if ( raster[i] 		== cursor->nodata[targetBand] )	{ continue; }
						if ( stackdb[depth][i] 	== cursor->nodata[targetBand] )	{ stackdb[depth][i] = raster[i]; mathmask[i] = TRUE; continue; }
						if ( raster[i]		>  stackdb[depth][i]	      ) { stackdb[depth][i] = raster[i]; }
					}
				} else { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Merging image method!"); pthread_exit( (void *)1 ); }
			}	
			for (i = 0; i < stacksz[depth]; i++ ) if ( stackdb[depth][i] != tmp->nodata[targetBand] ) stackdb[depth][i] = stackdb[depth][i] / (double)hit_for_avg[i];
			if ( raster != NULL ) { free(raster); raster = NULL; }

			depth++;

	        }
	}

//	fprintf(stderr, "INFO STOP: ---------------------------------\n"); fflush(stderr);
	

	if (depth != 1) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Math stack underflow"); pthread_exit( (void *)1 ); }

	vrt_cursor->time = time_serie[time_serie_index];
	for ( i = 0 ; i < (pxAOISizeX * pxAOISizeY ); i++ ) if ( mathmask[i] == FALSE ) stackdb[0][i] = vrt_cursor->nodata[nband];

	if  (( layers != NULL) && ( layer_index > 0 ) ) {
		for(i = 0; i < layer_index; i++ ) {
			if ( ( layers[i] - 1 ) != mathcur->band ) continue;
			hBandSrc = GDALGetRasterBand(vrt_cursor->dataset, i + 1);
			err = GDALRasterIO( hBandSrc, GF_Write,  0, 0,  pxAOISizeX, pxAOISizeY, stackdb[0], pxAOISizeX, pxAOISizeY, GDT_Float64, 0, 0);
			bzero(imgs_path, MAX_PATH_LEN - 1); sprintf(imgs_path, "%s", mathcur->coveragemath);		
			GDALSetMetadataItem 	(hBandSrc, "collection", imgs_path, NULL);
			GDALSetRasterNoDataValue(hBandSrc, vrt_cursor->nodata[nband]);	 
			GDALSetRasterScale      (hBandSrc, vrt_cursor->scale[nband] );
			GDALSetRasterOffset     (hBandSrc, vrt_cursor->offset[nband]);

		}
		
	} else {
		hBandSrc = GDALGetRasterBand(vrt_cursor->dataset, nband + 1);
		err = GDALRasterIO( hBandSrc, GF_Write,  0, 0,  pxAOISizeX, pxAOISizeY, stackdb[0], pxAOISizeX, pxAOISizeY, GDT_Float64, 0, 0);
		bzero(imgs_path, MAX_PATH_LEN - 1); sprintf(imgs_path, "%s", mathcur->coveragemath);		

		GDALSetMetadataItem 	(hBandSrc, "collection", imgs_path, NULL);
		GDALSetRasterNoDataValue(hBandSrc, vrt_cursor->nodata[nband]);	 
		GDALSetRasterScale      (hBandSrc, vrt_cursor->scale[nband] );
		GDALSetRasterOffset     (hBandSrc, vrt_cursor->offset[nband]);
	}

	
	for ( i = 0 ; i < MAX_D; i++ )	if ( stackdb[i] != NULL ) free(stackdb[i]);
	free(hit_for_avg); free(mathmask);  


	pthread_exit( (void *)0 );
} 


//------------------------------------------------------------------------------------------------


block sortList(block list){

	// 
	if(list == NULL || list->next == NULL) return list; // the list is sorted.

	//replace target node with the first : 

	//1- find target node : 
	block curr, target, targetPrev, prev;
	curr 		= list;
	target 	= list;
	prev 		= list;
	targetPrev 	= list;

	while(curr != NULL) {
		if(curr->time < target->time) {
			targetPrev	= prev;
			target		= curr;
        	}
		prev = curr;
	        curr = curr->next;

	}

	//target node is in target. 

	//2- switching firt node and target node : 
	block tmp;
	if(target != list) {
		targetPrev->next 	= list;
		tmp 			= list->next;
		list->next 		= target->next;
		target->next 		= tmp;
	}

	// now target is the first node of the list.

	// calling the function again with the sub list :
	//            list minus its first node :
	target->next = sortList(target->next);


	return target;
}



//------------------------------------------------------------------------------------------------
// This part is for read directory using curl ... Yes, using curl .. What is meaning? ftp and http is
// a new way ... :-O


DIR *openURL(const char *name){
	int i;
	CURL 		*curl_handle;
	CURLcode	res;

	char		path[MAX_PATH_LEN];	
	char		*token			= NULL;
	struct		ex_dirent *head		= NULL;
	struct 		ex_dirent *cursor 	= NULL;

	struct MemoryStruct chunk;


	i = strlen(name); bzero(path, MAX_PATH_LEN - 1 );
	if ( name[i-1] == '/' )		strcpy( path, 		name);
	else				sprintf(path, "%s/", 	name);

	chunk.memory 	= malloc(1);  	
	chunk.size 	= 0; 

	curl_global_init(CURL_GLOBAL_ALL);
	curl_handle = curl_easy_init();
	curl_easy_setopt(curl_handle, CURLOPT_URL, path);
	curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
	curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)&chunk);
	curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "MWCS/1.0");
	curl_easy_setopt(curl_handle, CURLOPT_DIRLISTONLY, 1);
	res = curl_easy_perform(curl_handle);

	if(res != CURLE_OK) { fprintf(stderr, "ERROR: curl_easy_perform() failed: %s, %s\n", curl_easy_strerror(res), path); return NULL; }
	
	head = cursor = (struct ex_dirent *)malloc(sizeof(struct ex_dirent)); cursor->next = NULL;

	for( token = strtok(chunk.memory, "\n"); token != NULL;  token = strtok(NULL, "\n") ){
		cursor->next  	= (struct ex_dirent *)malloc(sizeof(struct ex_dirent));
		cursor 		= cursor->next;
		cursor->next 	= NULL; 
		strcpy(cursor->d_name, token);
	}  	
	// for(cursor = head->next; cursor != NULL; cursor = cursor->next) fprintf(stderr, "+ %s\n", cursor->d_name); fflush(stderr);

	curl_easy_cleanup(curl_handle);
	if(chunk.memory) free(chunk.memory);
	curl_global_cleanup();

	return (DIR *)head;
}


FILE *openURLfile(const char *path, const char *mode){
        CURL            *curl_handle;
        CURLcode        res;

        char            *token                  = NULL;
        static struct   ex_dirent *head         = NULL;
        struct          ex_dirent *cursor       = NULL;

        struct MemoryStruct chunk;

        chunk.memory    = malloc(1);
        chunk.size      = 0;

        curl_global_init(CURL_GLOBAL_ALL);
        curl_handle = curl_easy_init();
        curl_easy_setopt(curl_handle, CURLOPT_URL, path);
        curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
        curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)&chunk);
        curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "MWCS/1.0");
        res = curl_easy_perform(curl_handle);

        if(res != CURLE_OK) { fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res)); return NULL; }

        head = cursor = (struct ex_dirent *)malloc(sizeof(struct ex_dirent));

        for( token = strtok(chunk.memory, "\n"); token != NULL;  token = strtok(NULL, "\n") ){
                cursor->next    = (struct ex_dirent *)malloc(sizeof(struct ex_dirent));
                cursor          = cursor->next;
                cursor->next    = NULL;
                cursor->line    = malloc(strlen(token) + 1);
                strcpy(cursor->line, token);
        }

        
        curl_easy_cleanup(curl_handle);
        if(chunk.memory) free(chunk.memory);
        curl_global_cleanup();
        return (FILE *)head;
}

int readURLfile(FILE *stream, const char *format, ...){
        va_list args;
        struct ex_dirent *cursor        = (struct ex_dirent *)stream;
        struct ex_dirent *tmp           = NULL;
        tmp = cursor->next;

        va_start( args, format );
        vsscanf( tmp->line, format, args);
        va_end( args );

        if ( tmp->next == NULL )        return EOF;
        else                            cursor->next = cursor->next->next;

        return 0;
}


struct dirent *readURL(DIR *dirp){
	struct ex_dirent *cursor 	= (struct ex_dirent *)dirp;
	struct ex_dirent *tmp		= NULL;
	struct dirent *fake		= NULL;

	if ( cursor->next == NULL ) 	return NULL;

	fake = (struct dirent *)malloc(sizeof(struct dirent));
	tmp  = cursor->next; 
	strcpy(fake->d_name, tmp->d_name);
	cursor->next = cursor->next->next;	
	free(tmp);

	return fake;
}


int closeURLfile(FILE *fp){
	struct ex_dirent *cursor = (struct ex_dirent *)fp;
	free(cursor);
	return 0;
}


int closeURL(DIR *dirp){
	struct ex_dirent *cursor = (struct ex_dirent *)dirp;
	free(cursor);

	return 0;
}


//------------------------------------------------------------------------------------------------


struct tar_header{
        char name[100];
        char mode[8];
        char owner[8];
        char group[8];
        char size[12];
        char modified[12];
        char checksum[8];
        char type[1];
        char link[100];
        char padding[255];
};


void tar_add(request_rec *r, GByte *realRaster, vsi_l_offset pnOutDataLength, char *imgs_path){
	GByte 	buf[513];
	size_t 	offset = 0;
        struct tar_header header;
	bzero(buf, 512 );

        memset( &header, 0, sizeof( struct tar_header ) );
        snprintf( header.name,          100,    "%s",           basename(imgs_path)  );
        snprintf( header.mode,          8,      "%06o ",        0644 ); //You should probably query the input file for this info
        snprintf( header.owner,         8,      "%06o ",        0 ); //^
        snprintf( header.group,         8,      "%06o ",        0 ); //^
        snprintf( header.size,          12,     "%011o",        (unsigned int)(pnOutDataLength) );
        snprintf( header.modified,      12,     "%011o",        (unsigned int)time(0) ); //Again, get this from the filesystem
        memset( header.checksum,        ' ',    8);
        header.type[0] = '0';


        //Calculate the checksum
        size_t checksum = 0;
        int i;
        const unsigned char* bytes = (unsigned char *)&header;
        for( i = 0; i < sizeof( struct tar_header ); ++i )  checksum += bytes[i];
        snprintf( header.checksum, 	8, 	"%06o ", 	(unsigned int)checksum );

	ap_rwrite( (const void *)bytes, 	sizeof( struct tar_header ), 	r );
	ap_rwrite( (const void *)realRaster, 	pnOutDataLength, 		r );

	offset = ( pnOutDataLength % 512 );
	if ( offset != 0 ) ap_rwrite( (const void *)buf, 512 - offset,		r );
}


//------------------------------------------------------------------------------------------------

int getData( struct info *info ){ // MAIN
	int i = 0, j = 0, k = 0, i_prod = 0;

	char 		*query_string 	= info->query_string;
	char 		*uri		= info->uri;
	request_rec 	*r		= info->r;
	int 		*killable	= &(info->killable);

	char imgs_path[MAX_PATH_LEN];

	char prod[MAX_STR_LEN];		char **prod_array	= NULL;
	char prod_path[MAX_STR_LEN];	char **prod_path_array 	= NULL;
	char log_tmp_str[MAX_STR_LEN];
	char time_path[MAX_STR_LEN];

	char final_path[MAX_PATH_LEN];
	char buff[100];
	char epsg_res_ref[256];
	char epsg_res_tmp[256];
	char datasetId[256]; 
	char *colortable		= NULL;
	char *subdataset		= NULL;
	char *token_from_user		= NULL;

	char *p;
	int MAX_THREAD 			= 1;
	int hit_number			= 0;
	int s_ref_year			= 0;
	int s_ref_month			= 0;
	int s_ref_day			= 0;
	int s_ref_time			= 0;
	int e_ref_year			= 0;
	int e_ref_month			= 0;
	int e_ref_day			= 0;
	int e_ref_time			= 0;
	int proj			= 0;
	DIR 		*dp		= NULL;
	struct dirent 	*ep		= NULL;  
	double		*raster		= NULL;
	double		*avg		= NULL;
	GByte		*mathmask	= NULL;
	int		*hit_for_avg	= NULL;
	FILE		*DC		= NULL;
	char		*equaliz_sigma 	= NULL;

	struct tm	t_time;	
	struct tm	s_time;	
	struct tm	e_time;	

	time_t t_start;
	time_t t_finish;
	time_t t_tmp;
	time_t t_min;
	time_t t_max;

	time_t 	chronometer;
	char   	chronometer_str[256];
	struct 	timespec time_before,time_after;

	double	time_total  = 0.0;	
	double	time_search = 0.0;	
	double	time_read   = 0.0;	

	int	collection_need_for_math = 0;

	double 	adfGeoTransform[6] 	= {0.0, 0.0, 0.0, 0.0, 0.0, 0.0 };
	double 	adfExtent[4]		= {0.0, 0.0, 0.0, 0.0};
	int	pxAOISizeX		= 0;
	int	pxAOISizeY		= 0;
	double	x_input_ul		= 0.0;
	double	y_input_ul		= 0.0;
	double	x_input_ur		= 0.0;
	double	y_input_ur		= 0.0;
	double	x_input_lr		= 0.0;
	double	y_input_lr		= 0.0;
	double	x_input_ll		= 0.0;
	double	y_input_ll		= 0.0;
	double	h_input_low		= 0.0;
	double	h_input_high		= 0.0;
	double	sol_input_start		= 0.0;
	double	sol_input_stop		= 0.0;
	double	sl_input_start		= 0.0;
	double	sl_input_stop		= 0.0;
	double	tmp_d			= 0.0;
	double	*grid_starts 		= NULL;
	double	*grid_stops		= NULL;
	double	high_min		= DEFAULT_NODATA;
	double	high_max		= DEFAULT_NODATA;
	double	color_range_min		= 0.0;
	double	color_range_max		= 0.0;
	double	nodata_user_defined	= 0.0;
	double	scale_user_defined	= 1.0;
	double	offset_user_defined	= 0.0;
	int	math_memory_factor	= 0;
	int	*layers			= NULL;
	int	grid_index		= 0;
	int	layer_index		= 0;
	int 	gotcha 			= FALSE;
	int	PROD_FOUND		= FALSE;
	int 	VIRTUAL_COLLECTION	= FALSE;
	int	X_RANGE_DEFINED		= FALSE;
	int	Y_RANGE_DEFINED		= FALSE;
	int	H_RANGE_DEFINED		= FALSE;
	int	START_TIME_DEFINED	= FALSE;
	int	END_TIME_DEFINED	= FALSE;
	int	X_INPUT_COORD_IS	= DEFAULT_NODATA;
	int	Y_INPUT_COORD_IS	= DEFAULT_NODATA;
	int	MAX_MGRS_TILES		= DEF_MGRS_TILES;
	int	COLOR_RANGE_DEFINED	= FALSE;
	int	NO_DATA_DEFINED		= FALSE;
	int	SCALE_DEFINED		= FALSE;
	int	OFFSET_DEFINED		= FALSE;
	int	INPUT_GEOM		= FALSE;
	int	FILTER			= TRUE;
	int	CROP			= TRUE;
	int	COMPRESSION		= TRUE;
	int	AUTO_MGRS_TILE		= FALSE;
	int	AUTO_PATHROW_TILE	= FALSE;
	int	NODATA_AUTO		= FALSE;
	int	gml_version		= 1;
	int	X_RANGE_DEFINED_PX	= FALSE;
	int	Y_RANGE_DEFINED_PX	= FALSE;
	int	SOL_RANGE_DEFINED	= FALSE;
	int	SL_RANGE_DEFINED	= FALSE;	
	int	OLD_STYLE_NAME		= FALSE;
	int	math_method		= SQUEEZE;
	int	WMTS_MODE		= FALSE;
	int	MERGING_IMAGE		= AVERAGE;

	double	GeoX_ul			= 0.0;
	double	GeoY_ul			= 0.0;
	double	GeoX_lr			= 0.0;
	double	GeoY_lr			= 0.0;
	double	Xp_ul			= 0.0;
	double	Yp_ul			= 0.0;
	double	Xp_lr			= 0.0;
	double	Yp_lr			= 0.0;

	int	nband			= 0;
	int	type			= 0;
	int	subDataSet		= 1;
	double	x_min_res		= 0.0;
	double	y_min_res		= 0.0;
	double	x_min_res_ori		= 0.0;
	double	y_min_res_ori		= 0.0;

        struct vsblock  *shp_head       = NULL;
        struct vsblock  *shp_cursor     = NULL;
        struct vsblock  *shp_tmp     	= NULL;
	
	double	dfMinX			= 0.0;
	double	dfMaxX			= 0.0;
	double	dfMaxY			= 0.0;
	double	dfMinY			= 0.0;
	double	dfXRes			= 0.0;
	double	dfYRes			= 0.0;

	char		*query		= NULL;
	char		*tok		= NULL;
	long int	len		= 0;
	const char      *pszValue;
	int		Request		= GetCapabilities; // Default 
	char            *pszKey         = NULL;
	char		*input_proj	= NULL;
	char		*force_out_proj	= NULL;
	int		epgs_in		= DEFAULT_NODATA;
	int		epgs_out	= DEFAULT_NODATA;
	int 		outputType 	= GDT_Byte;
	double		outscale	= 1.0;
	double		outresolution	= 0.0;
	int		outsize_x	= 0;
	int		outsize_y	= 0;
	int 		zoom		= -1;
	char		*input_name	= NULL;
	char		*output_name	= NULL;
	char 		subsetKey[256];
	char		p1[256];
	char		p2[256];
	char		*gmin_str	= NULL;
	char		*gmax_str	= NULL;
	char		*layer_str	= NULL;
	struct 		ProjAndRes projRes[MAX_D];
	int		projs[MAX_D];

	int		LC8_PathRow	= 0;
	int		LC8_path	= 0;
	int		LC8_row		= 0;
	char		**mgrs_tile_arr = NULL;
	char		*granuleId[MAX_D];

	char 		*magic_filter_params = NULL;
	char 		*magic_filter_func   = NULL;
	       

	block		head		= NULL;
	block		cursor		= NULL;
	block		tmp		= NULL;
	block		vrt_head	= NULL;
	block		vrt_cursor	= NULL;
	
	struct virtualList *hvlist 	= NULL;
	struct virtualList *cvlist 	= NULL;
	struct virtualList *hvorder	= NULL;
	struct virtualList *cvorder	= NULL;
	struct mathUnit	   *mathChain	= NULL;
	struct mathUnit	   *mathcur	= NULL;
	struct VirtualContent *VirCol	= NULL;
	struct VirtualContent *VirCur 	= NULL; 


	OGRSpatialReferenceH            hSourceSRS, hTargetSRS;
	OGRSpatialReferenceH 		geoSRSsrc;
	char 				*wktTargetSRS; 
	GDALDatasetH    		hDstDS;
	GDALRasterBandH 		hBandSrc;
	void    			*hTransformArg;
	OGRGeometryH  			hGeom;
	CPLErr          		err;

	GDALDatasetH			hVRTSrcDS;
	GDALDatasetH 			hMEMSrcDS, hMEMDstDS;
	char            		**papszOptions          = NULL;
	char				*outFormat		= NULL;
	GDALDriverH 			hVRTDriver;
	GDALDriverH 			hMEMDriver;
	GDALDriverH			hOutDriver;

	int				outType			= XML;
	double				lowerCorner[2]		= { 0.0, 0.0 };
	double				upperCorner[2]		= { 0.0, 0.0 };
	int				label_index		= 0;
	int				time_index		= 1;
	time_t 				*time_serie 		= NULL; 
	int				time_serie_index	= 0;


	int SAME_YEAR 	= FALSE;
	int SAME_MONTH 	= FALSE;
	int SAME_DAY	= FALSE;
	int SAME_TIME	= FALSE;


	vsi_l_offset 			pnOutDataLength = 0;
	GDALDatasetH 			realImg;
	GByte				*realRaster	= NULL;

	double	resByZoom[20]		= { 156412, 78206, 39103, 19551, 9776, 4888, 2444, 1222, 610.984, 305.492, 152.746, 76.373, 38.187, 19.093, 9.547, 4.773, 2.387, 1.193, 0.596, 0.298 };



	if ( ExtHostname == NULL ){ ExtHostname = (char *)malloc(MAX_STR_LEN); bzero(ExtHostname, MAX_STR_LEN - 1 ); sprintf(ExtHostname, "http://%s/%s", r->hostname, "wcs" ); }
	addStatToPush(info, "wcs_host",  ExtHostname, GFT_String);  // STATS

	if ( IOThreads != NULL ){
		if ( ! strcmp(IOThreads, "AUTO" ) ) 	MAX_THREAD = sysconf(_SC_NPROCESSORS_ONLN); 
		else					MAX_THREAD = atoi(IOThreads);
	
		if ( MAX_THREAD <=0 ) MAX_THREAD = 1;
	} else	MAX_THREAD = sysconf(_SC_NPROCESSORS_ONLN);
	

	if ( MaxMGRSTiles != NULL ) { MAX_MGRS_TILES = atoi(MaxMGRSTiles); if ( MAX_MGRS_TILES <= 0 ) MAX_MGRS_TILES = DEF_MGRS_TILES; }

	ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "START: Starting processing (%d thread for I/O) ...", MAX_THREAD);



	DIR		*(*openSource)(const char *)			= NULL;
	struct dirent 	*(*listSource)(DIR *) 				= NULL;
	int 		 (*closeSource)(DIR *)				= NULL;

	FILE 		*(*openSourceFile)(const char *, const char *) 	= NULL;
	int		 (*readSourceFile)(FILE *, const char *, ...)	= NULL;
	int 		 (*closeSourceFile)(FILE *)			= NULL;

	if ( KEEP_OLD_STYLE_NAME == TRUE ) OLD_STYLE_NAME = TRUE;

	if ( ( ROOT != NULL ) && ( strstr(ROOT, "ftp://") ) ){
		openSource	= openURL;
		listSource 	= readURL;
		closeSource	= closeURL;
		openSourceFile	= openURLfile;
		readSourceFile	= readURLfile;
		closeSourceFile	= closeURLfile;
	} else {
		listSource 	= readdir;
		openSource	= opendir;
		closeSource	= closedir;
		openSourceFile	= fopen;
		readSourceFile	= fscanf;
		closeSourceFile = fclose;
	}


        GDALAllRegister();
	OGRRegisterAll();
	CPLSetErrorHandler(NULL);
	CPLSetConfigOption("GDAL_PAM_ENABLED", 			"NO"	);
	CPLGetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", 	"TRUE"	);

//	CPLSetConfigOption("GDAL_CACHEMAX", 			"0" 	);
//	CPLSetConfigOption("VSI_CACHE", 			"FALSE" );
//	CPLGetConfigOption("CPL_VSIL_CURL_CHUNK_SIZE",		"65536"	);
//	CPLGetConfigOption("CPL_VSIL_CURL_NON_CACHED",		""	);
//	CPLGetConfigOption("GDAL_DATASET_CACHING", 		"NO"	);
//	CPLSetConfigOption("CPL_DEBUG",				"ON" 	);

	// GDALSetCacheMax64( 1024 * 1024 * 200 );
	 
	// fprintf(stderr, "step 1 %f MByte\n", (float)GDALGetCacheMax64() / 1024 / 1024); fflush(stderr);

	// AWS Global variable ... s3 bucket motherfucker!!!
	if ( ( AWS_ACCESS_KEY_ID != NULL ) && ( AWS_SECRET_ACCESS_KEY != NULL ) && ( AWS_DEFAULT_REGION != NULL ) ){ 
		setenv("AWS_ACCESS_KEY_ID", 	AWS_ACCESS_KEY_ID, 	1);  
		setenv("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY, 	1);  
		setenv("AWS_DEFAULT_REGION", 	AWS_DEFAULT_REGION, 	1);  
	}

	// Set generic gloabal variables
	if ( ( ENVIRONMENT_VARIABLE[0] != NULL ) && ( ENVIRONMENT_VARIABLE[1] != NULL ) )
		for ( i = 0; i < MAX_D; i++) if ( ( ENVIRONMENT_VARIABLE[0][i] != NULL ) && ( ENVIRONMENT_VARIABLE[1][i] != NULL ) ) setenv(ENVIRONMENT_VARIABLE[0][i], ENVIRONMENT_VARIABLE[1][i],     1);

	if (!init_math_parser()) {  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to init math parser" ); 	return 500; }
	//------------------------------------------------------------------------------------------------
	
	t_time.tm_sec   = s_time.tm_sec   = e_time.tm_sec   = 0; /* seconds */
	t_time.tm_min   = s_time.tm_min   = e_time.tm_min   = 0; /* minutes */
	t_time.tm_hour  = s_time.tm_hour  = e_time.tm_hour  = 0; /* hours */
	t_time.tm_mday  = s_time.tm_mday  = e_time.tm_mday  = 0; /* day of the month */
	t_time.tm_mon   = s_time.tm_mon   = e_time.tm_mon   = 0; /* month */
	t_time.tm_year  = s_time.tm_year  = e_time.tm_year  = 0; /* year */
	t_time.tm_wday  = s_time.tm_wday  = e_time.tm_wday  = 0; /* day of the week */
	t_time.tm_yday  = s_time.tm_yday  = e_time.tm_yday  = 0; /* day in the year */
	t_time.tm_isdst = s_time.tm_isdst = e_time.tm_isdst = 0; /* daylight saving time */

	t_min 	= time(NULL);
	t_max 	= 0;
	hTargetSRS = OSRNewSpatialReference(NULL);
	hMEMDriver = GDALGetDriverByName("MEM");
	hVRTDriver = GDALGetDriverByName("VRT"); 

	prod_array 	= (char **)malloc(sizeof(char *) * MAX_D);		for (i = 0; i < MAX_D; i++ )		prod_array[i] 		= NULL;
	prod_path_array = (char **)malloc(sizeof(char *) * MAX_D);		for (i = 0; i < MAX_D; i++ )		prod_path_array[i] 	= NULL;
	mgrs_tile_arr   = (char **)malloc(sizeof(char *) * MAX_MGRS_TILES ); 	for (i = 0; i < MAX_MGRS_TILES; i++ ) 	mgrs_tile_arr[i] 	= NULL;

	if ( prod_array 	== NULL ) {  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc prod_array" );  	return 500; }
	if ( prod_path_array 	== NULL ) {  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc prod_path_array" );  	return 500; }
	if ( mgrs_tile_arr 	== NULL ) {  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc mgrs_tile_arr" );  	return 500; }


	for ( i = 0; i < MAX_D; i++) { prod_array[i] = NULL; prod_path_array[i] = NULL; }
	for (i = 0 ; i < MAX_D; i++) { projRes[i].proj = -1; for (j = 0 ; j < MAX_D; j++) projRes[i].res[j] = -1; }
	//------------------------------------------------------------------------------------------------
	

	// WCS 3.0 parsing URI
	/*	
	// uri: wcs/3.0/
	tok = strtok( uri, "/" ); if ((tok!=NULL) && ( ! strcmp(tok,"wcs") )) { tok = strtok( NULL, "/" ); if ((tok!=NULL) && ( ! strcmp(tok,"3.0") )) {
			// Parsing URI
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: WCS 3.0 Request ..." ); 

			tok = strtok( NULL, "/" ); if ((tok!=NULL) && ( ! strcmp(tok,"coverages") )) {
				Request = GetCapabilities;

				tok = strtok( NULL, "/" ); if ( tok != NULL ) { 
					Request = DescribeCoverage;
					prod_path_array[i_prod] = (char *)malloc(sizeof(char) * MAX_STR_LEN); 
					prod_array[i_prod]	= (char *)malloc(sizeof(char) * MAX_STR_LEN); 
					if ( ROOT != NULL ) 	sprintf(prod_path_array[i_prod],  	"%s/%s",  ROOT, tok);
					else		 	sprintf(prod_path_array[i_prod],  	"%s",  		tok);

					sprintf(prod_array[i_prod],  		"%s",  		tok);

					tok = strtok( NULL, "/" ); if ((tok!=NULL) && ( ! strcmp(tok,"rangeSet") )) {
						Request = GetCoverage;

						tok = strtok( NULL, "/" ); if ( (tok!=NULL) && ( ( ! strcmp(tok, "image" ) ) || ( ! strcmp(tok, "text" ) ) || ( ! strcmp(tok, "application" ) ) ) ){

							tok 	  = strtok( NULL, "/" );
							outType	  = IMAGE;
							outFormat = malloc(256); bzero(outFormat,   255);
							if 	( ! strcmp(tok, "tiff") 	)	strcpy(outFormat, "GTiff"); 
							else if ( ! strcmp(tok, "png")  	)	strcpy(outFormat, "PNG");
							else if ( ! strcmp(tok, "jp2")  	)	strcpy(outFormat, "JP2OpenJPEG");
							else if ( ! strcmp(tok, "envi")  	)	strcpy(outFormat, "ENVI");
							else if ( ! strcmp(tok, "plain")  	)	strcpy(outFormat, "XYZ");
							else if ( ! strcmp(tok, "xml") 		)	{ free(outFormat); outFormat = NULL; outType = XML; }
							else if ( ! strcmp(tok, "gml+xml") 	)	{ free(outFormat); outFormat = NULL; outType = XML; }
							else if ( ! strcmp(tok, "tar") 		)	{ free(outFormat); outFormat = NULL; outType = TAR; }
							else if ( ! strcmp(tok, "gif") 		)	{ 		   outFormat = "gif"; outType = MAGIC; }	
							else { free(outFormat); outFormat = NULL; outType = XML; ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Output format understandable"); 
								GoodbyeMessage(info,"Output format understandable"); return 415; }

								
						}
					}
				}
			}


			// Parsing arguments
			if( query_string != NULL ) { 
				len = strlen(query_string); if( len > 0 ) {
			
					query = malloc(len + 1); if ( query == NULL) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc query %s", query_string); return 500; }
					bzero(query, len); unencode(query_string, query);
					tok = strtok( query, DELIMS );
					while( tok != NULL ) {
				                pszValue = (const char *)CPLParseNameValue( tok, &pszKey );
				                if ( pszValue == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to parsing '%s'", tok); return 400; }
						for(p = pszKey;*p;++p) *p=*p>0x40&&*p<0x5b?*p|0x60:*p;

						// fprintf(stderr, "%s %s\n", pszKey, pszValue);
	
						if 		( ! strcmp(pszKey, "lon" ) ) { 	sscanf(pszValue, "%[^','],%[^'']", p1, p2 );	if ( strlen(p2) <= 0 ) strcpy(p2,p1);
												x_input_ul = atof(p1); x_input_lr = atof(p2); X_RANGE_DEFINED = TRUE; X_INPUT_COORD_IS = LATLON;

						} else if	( ! strcmp(pszKey, "lat" ) ) { 	sscanf(pszValue, "%[^','],%[^'']", p1, p2 ); 	if ( strlen(p2) <= 0 ) strcpy(p2,p1);
												y_input_ul = atof(p1); y_input_lr = atof(p2); Y_RANGE_DEFINED = TRUE; Y_INPUT_COORD_IS = LATLON;

						} else if	( ! strcmp(pszKey, "time") ) {
							sscanf(pszValue, "%[^'/']/%[^''])", p1, p2 );

							sscanf(p1, "%d-%d-%dT%d:%d:%d", &s_time.tm_year , &s_time.tm_mon, &s_time.tm_mday, &s_time.tm_hour, &s_time.tm_min, &s_time.tm_sec ); s_time.tm_year -= 1900; s_time.tm_mon -= 1;
							t_start = timegm(&s_time);		START_TIME_DEFINED	= TRUE;

							sscanf(p2, "%d-%d-%dT%d:%d:%d", &e_time.tm_year , &e_time.tm_mon, &e_time.tm_mday, &e_time.tm_hour, &e_time.tm_min, &e_time.tm_sec ); e_time.tm_year -= 1900; e_time.tm_mon -= 1;
							t_finish = timegm(&e_time);		END_TIME_DEFINED	= TRUE;
						}
						tok = strtok( NULL, DELIMS );	
					}
				}
			}
			goto Skip_WCS2_Query;
		}
	}*/
		
	
	
	//------------------------------------------------------------------------------------------------


					if( query_string  	== NULL ){ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Error in invocation - wrong FORM probably (NULL)"); 	GoodbyeMessage(info, "Wrong FORM probably"); return 400; }
	len = strlen(query_string); 	if( len 		<= 0    ){ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Error in invocation - wrong FORM probably (Size zero)"); GoodbyeMessage(info, "wrong FORM probably"); return 400; }

	query = malloc(len + 1); if ( query == NULL) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc query %s", query_string); return 500; }
	bzero(query, len);
	unencode(query_string, query);

	char *query_ptr;

	//------------------------------------------------------------------------------------------------
	tok = strtok_r( query, DELIMS, &query_ptr );
	while( tok != NULL ) {
		// fprintf(stderr, "%s\n", tok); fflush(stderr);
		pszValue = (const char *)CPLParseNameValue( tok, &pszKey );
		if ( pszValue == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to parsing '%s'", tok); GoodbyeMessage(info, "Unable to parsing '%s'", tok); return 400; }

		for(p = pszKey;*p;++p) *p=*p>0x40&&*p<0x5b?*p|0x60:*p;
		
		if 		( ! strcmp(pszKey, "service") && ( strcmp(pszValue, "WCS") ) ) {
				ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unsupported service %s", pszValue); GoodbyeMessage(info,"Unsupported service %s", pszValue ); return 400; 

		} else if 	( ! strcmp(pszKey, "token"  ) ) { token_from_user = malloc(MAX_STR_LEN); bzero(token_from_user, MAX_STR_LEN - 1); strcpy(token_from_user, pszValue); 

		} else if	( ! strcmp(pszKey, "request") ) {
				if 	( ! strcmp(pszValue, "GetCoverage"	) ) 	Request = GetCoverage;
				else if ( ! strcmp(pszValue, "GetCapabilities"	) )	Request = GetCapabilities;
				else if ( ! strcmp(pszValue, "DescribeCoverage"	) )	Request = DescribeCoverage;
				else if ( ! strcmp(pszValue, "GetList"		) )	Request = GetList;
				else if ( ! strcmp(pszValue, "GetInfo"		) )	Request = GetInfo;
				else if ( ! strcmp(pszValue, "GetFile"		) )	Request = GetFile;
				else if ( ! strcmp(pszValue, "Status"		) )	Request = Status;
				else { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unsupported request %s", pszValue); GoodbyeMessage(info,"Unsupported request %s", pszValue ); return 400; }
		} else if	( ! strcmp(pszKey, "format") ) {
					outType	  = IMAGE;
					outFormat = malloc(256); bzero(outFormat,   255);
					if 	( ! strcmp(pszValue, "image/tiff") 	)	strcpy(outFormat, "GTiff"); 
					else if ( ! strcmp(pszValue, "image/png")  	)	strcpy(outFormat, "PNG");
					else if ( ! strcmp(pszValue, "image/jp2")  	)	strcpy(outFormat, "JP2OpenJPEG");
					else if ( ! strcmp(pszValue, "image/jpeg")  	)	strcpy(outFormat, "JPEG");
					else if ( ! strcmp(pszValue, "image/envi")  	)	strcpy(outFormat, "ENVI");
					else if ( ! strcmp(pszValue, "text/plain")  	)	strcpy(outFormat, "XYZ");
					else if ( ! strcmp(pszValue, "application/xml") )	{ free(outFormat); outFormat = NULL; 		outType = XML;   }
					else if ( ! strcmp(pszValue, "application/gml+xml") )	{ free(outFormat); outFormat = NULL; 		outType = XML;   }
					else if ( ! strcmp(pszValue, "application/tar") )	{ free(outFormat); outFormat = NULL; 		outType = TAR;   }
					else if ( ! strcmp(pszValue, "application/json") )	{ free(outFormat); outFormat = NULL; 		outType = JSON;  }
					else if ( ! strcmp(pszValue, "image/gif") 	)	{ 		   strcpy(outFormat, "gif"); 	outType = MAGIC; }
					else if ( ! strcmp(pszValue, "text/csv")	)	{ free(outFormat); outFormat = NULL;		outType = CSV;   }
					else if ( ! strcmp(pszValue, "image/chart")	)	{ free(outFormat); outFormat = NULL;		outType = CHART; }

					else { free(outFormat); outFormat = NULL; outType = XML; ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Output format understandable"); GoodbyeMessage(info,"Output format understandable"); return 415; }

		} else if	( ! strcmp(pszKey, "proc") ) { 
					if 	( ! strcmp(pszValue, "average") 	)	MERGING_IMAGE = AVERAGE;
					else if ( ! strcmp(pszValue, "overlap")  	)	MERGING_IMAGE = OVERLAP;
					else if ( ! strcmp(pszValue, "mostrecent")  	)	MERGING_IMAGE = MOSTRECENT;
					else if ( ! strcmp(pszValue, "leastrecent")  	)	MERGING_IMAGE = OVERLAP;
					else if ( ! strcmp(pszValue, "minvalue")  	)	MERGING_IMAGE = MINVALUE;
					else if ( ! strcmp(pszValue, "maxvalue")  	)	MERGING_IMAGE = MAXVALUE;
					else							MERGING_IMAGE = AVERAGE;

		} else if	( ! strcmp(pszKey, "equaliz_sigma") ) {
					equaliz_sigma = malloc(256); bzero(equaliz_sigma, 255);
					strcpy(equaliz_sigma, pszValue);	
		} else if	( ! strcmp(pszKey, "magick") ) {
					magic_filter_func   = (char *)malloc(MAX_STR_LEN); bzero(magic_filter_func,   MAX_STR_LEN - 1);
					magic_filter_params = (char *)malloc(MAX_STR_LEN); bzero(magic_filter_params, MAX_STR_LEN - 1);
					sscanf(pszValue, "%[^'('](%[^')'])", magic_filter_func, magic_filter_params );
		} else if	( ! strcmp(pszKey, "colortable") ) {
					if ( strcmp ( pszValue, "default" ) ) {
						colortable = malloc(256); bzero(colortable, 255);
						strcpy(colortable, pszValue);
					}
		} else if	( ! strcmp(pszKey, "colorrange") ) {
					sscanf(pszValue, "(%lf,%lf)", &color_range_min, &color_range_max );
					COLOR_RANGE_DEFINED = TRUE;
		} else if 	( ! strcmp(pszKey, "nodata" ) ){
					if 	( ! strcmp(pszValue, "automin"  ) ) NODATA_AUTO = NODATA_MIN;
					else if ( ! strcmp(pszValue, "automax"  ) ) NODATA_AUTO = NODATA_MAX;
					else { nodata_user_defined = atof(pszValue); NO_DATA_DEFINED = TRUE; }
		} else if 	( ! strcmp(pszKey, "coeff" ) ){
					scale_user_defined  = atof(pszValue); SCALE_DEFINED   = TRUE;

		} else if 	( ! strcmp(pszKey, "offset" ) ){
					offset_user_defined = atof(pszValue); OFFSET_DEFINED  = TRUE;

		} else if	( ! strcmp(pszKey, "gzoom") ) {
				zoom = atoi(pszValue); 		if ( zoom 	> 19 ) zoom 	= 19;

		} else if	( ! strcmp(pszKey, "scale") ) {
					outscale = atof(pszValue);	if ( outscale 	<= 0 ) outscale = 1;

		} else if	( ! strcmp(pszKey, "subdataset") ) {
					j = strlen(pszValue); for ( i = 0; i < j; i++ ) if ( ! isdigit(pszValue[i]) ) break;
					if ( i == j ) 	{ subDataSet = atoi(pszValue);	if ( subDataSet <= 0 ) subDataSet = 1; }
					else		{ subdataset = malloc(j + 1); bzero(subdataset, j); strcpy(subdataset, pszValue); }


		} else 	if 	( ! strcmp(pszKey, "_id" ) ) { 
					for ( i = 0; i < MAX_D; i++ ) granuleId[i] = NULL; char *tok_gr = NULL; char *tok_ptr = NULL;
					for ( tok_gr = strtok_r( (char *)pszValue, ",", &tok_ptr ), i = 0; tok_gr != NULL; pszValue = tok_ptr, tok_gr = strtok_r( (char *)pszValue, ",", &tok_ptr), i++ ){ 
						granuleId[i] = malloc( sizeof(char) * (strlen(tok_gr) + 1) ); strcpy(granuleId[i], tok_gr); granuleId[i][strlen(tok_gr)] = '\0'; 
					}
		} else if	( ! strcmp(pszKey, "size") ) {

 					bzero(p1,        255);
                        		bzero(p2,        255);
                       			sscanf(pszValue, "(%[^',)'],%[^')'])", p1, p2 );

					outsize_x = atoi(p1); outsize_y = atoi(p2);

		} else if 	( ! strcmp(pszKey, "epsg_in" ) ){
					input_proj  = malloc(256); bzero(input_proj, 254);  strcpy(input_proj,  pszValue);				
				
		} else if 	( ! strcmp(pszKey, "epsg_out" ) ){
					force_out_proj = malloc(256); bzero(force_out_proj, 254); strcpy(force_out_proj, pszValue);

		} else if 	( ! strcmp(pszKey, "resolution_out" ) ){
					outresolution = atof(pszValue);	 if ( outresolution    <= 0 ) outresolution  = 0;
		// FILTER
		} else if 	( ! strcmp(pszKey, "filter" ) ){
					if ( ! strcmp(pszValue, "true"  ) ) FILTER = TRUE;
					if ( ! strcmp(pszValue, "false" ) ) FILTER = FALSE;
		} else if 	( ! strcmp(pszKey, "wmts" ) ){
					if ( ! strcmp(pszValue, "true"  ) ) WMTS_MODE = TRUE;
					if ( ! strcmp(pszValue, "false" ) ) WMTS_MODE = FALSE;
		// 
		} else if 	( ! strcmp(pszKey, "math" ) ){
					if ( ! strcmp(pszValue, "squeeze" ) ) math_method = SQUEEZE;
					if ( ! strcmp(pszValue, "single"  ) ) math_method = SINGLE;

		} else if 	( ! strcmp(pszKey, "crop" ) ){
					if ( ! strcmp(pszValue, "true"  ) ) CROP = TRUE;
					if ( ! strcmp(pszValue, "false" ) ) CROP = FALSE;

		} else if 	( ! strcmp(pszKey, "compression" ) ){
					if ( ! strcmp(pszValue, "true"  ) ) COMPRESSION = TRUE;
					if ( ! strcmp(pszValue, "false" ) ) COMPRESSION = FALSE;

		} else if 	( ! strcmp(pszKey, "oldstylename" ) ){
					if ( ! strcmp(pszValue, "true"  ) ) OLD_STYLE_NAME = TRUE;
					if ( ! strcmp(pszValue, "false" ) ) OLD_STYLE_NAME = FALSE;

		} else if 	( ! strcmp(pszKey, "gml_version" ) ){ // GML version 3.2.1
					if ( ! strcmp(pszValue, "3.2.1"  ) ) gml_version = 321;
		// And if i wand i single file?					
		} else if 	( ! strcmp(pszKey, "filename" ) ){ 
					if ( strlen(pszValue) > 0 ) { input_name   = malloc( strlen(pszValue) + 1) ; bzero(input_name,   strlen(pszValue) ); strcpy(input_name,   pszValue); }
		} else if 	( ! strcmp(pszKey, "outname" ) ){ 
					if ( strlen(pszValue) > 0 ) { output_name  = malloc( strlen(pszValue) + 1) ; bzero(output_name,  strlen(pszValue) ); strcpy(output_name,  pszValue); }
		} else if 	( ! strcmp(pszKey, "module" ) ){ 
					if ( strlen(pszValue) > 0 ) { info->module = malloc( strlen(pszValue) + 1) ; bzero(info->module, strlen(pszValue) ); strcpy(info->module, pszValue); }
		// LC8_path
		} else if 	( ! strcmp(pszKey, "path") ) {
				if (( strlen(pszValue) == 1 ) && ( pszValue[0] == '*' ) ) 	LC8_path = DEFAULT_NODATA;
				else 								LC8_path = atoi(pszValue); 	LC8_PathRow++;
		} else if 	( ! strcmp(pszKey, "row") ) {
				if (( strlen(pszValue) == 1 ) && ( pszValue[0] == '*' ) )	LC8_row  = DEFAULT_NODATA;
				else 								LC8_row  = atoi(pszValue);	LC8_PathRow++;
		// MGRS tile filter for Sentinel 2
		} else if 	( ! strcmp(pszKey, "mgrs_tile") ) {
				char *mgrs_tok  = NULL;
				char *mgrs_ptr  = NULL;
				char *mgrs_tile	= NULL;
				mgrs_tile = (char *)malloc( sizeof(char) * MAX_STR_LEN); bzero(mgrs_tile, MAX_STR_LEN - 1);
				for (i = 0; i < strlen(pszValue); i++ ) mgrs_tile[i] = tolower(pszValue[i]);

				mgrs_tok = strtok_r(mgrs_tile, ",", &mgrs_ptr ); i = 0;
				if ( ! strcmp(mgrs_tok, "auto" ) ) AUTO_MGRS_TILE = TRUE; 
				else while ( mgrs_tok != NULL){
					if (	( strlen(mgrs_tok)	== 6 	) &&
						( mgrs_tok[0] 		== 't' 	) &&				// T
						(( mgrs_tok[1] 	>= 48  	) && ( mgrs_tok[1]	<= 57   ) ) && 	// 3
						(( mgrs_tok[2] 	>= 48  	) && ( mgrs_tok[2]	<= 57	) ) &&	// 2
						(( mgrs_tok[3] 	>= 97  	) && ( mgrs_tok[3]	<= 122  ) ) &&	// T
						(( mgrs_tok[4] 	>= 97  	) && ( mgrs_tok[4]	<= 122  ) ) &&	// Q
						(( mgrs_tok[5] 	>= 97  	) && ( mgrs_tok[5]	<= 122  ) ) ){	// Q
						mgrs_tile_arr[i] = (char *)malloc( sizeof(char) * 10); bzero(mgrs_tile_arr[i], 9);	
						sprintf(mgrs_tile_arr[i], "%c%c%c/%c/%c/%c", mgrs_tok[0], mgrs_tok[1], mgrs_tok[2], mgrs_tok[3], mgrs_tok[4], mgrs_tok[5]);
						// fprintf(stderr, "%s\n", mgrs_tile_arr[i]); fflush(stderr);
						i++;
						if ( i >= MAX_MGRS_TILES ) break;
					} else  mgrs_tile_arr[i] = NULL;
					mgrs_tile = mgrs_ptr;
					mgrs_tok  = strtok_r(mgrs_tile, ",", &mgrs_ptr );
				}
		} else if 	( ! strcmp(pszKey, "pathrow_tile") ) {
					if ( ! strcmp(pszValue, "auto"  ) ) AUTO_PATHROW_TILE = TRUE;

		} else if	( ! strcmp(pszKey, "coverageid") ) {
				prod_path_array[i_prod] = (char *)malloc(sizeof(char) * MAX_STR_LEN); 
				prod_array[i_prod]	= (char *)malloc(sizeof(char) * MAX_STR_LEN);
			        if (strlen(pszValue) <= 0 ) { GoodbyeMessage(info,"Unable to get CoverageId"); return 400; }
			
				if ( ROOT !=NULL ) sprintf(prod_path_array[i_prod],  	"%s/%s",  ROOT, 	pszValue);
				else		   sprintf(prod_path_array[i_prod],     "%s",  	         	pszValue);
				sprintf(prod_array[i_prod],  		"%s",  					pszValue);
				sprintf(prod,  				"%s",  					pszValue);


		} else if	( ! strcmp(pszKey, "coveragemath") ) {
				mathChain = (struct mathUnit *)malloc(sizeof(struct mathUnit)); mathChain->next = NULL; 
				mathChain->coveragemath = (char *)malloc(sizeof(char) * (strlen(pszValue) + 1 ) );
				sprintf(mathChain->coveragemath, "%s", pszValue); 
		} else if 	( ! strcmp(pszKey, "subset") ){
		
	 			bzero(subsetKey, 255);
	 			bzero(p1,	 255);
	 			bzero(p2,	 255);

				sscanf(pszValue, "%[a-z,A-Z](%[^',)'],%[^')'])", subsetKey, p1, p2 );
				if ( strlen(p2) <= 0 ) strcpy(p2,p1);

				if 	( ! strcmp(subsetKey, "Long" ) ) { x_input_ul   = atof(p1); x_input_lr    = atof(p2); X_RANGE_DEFINED = TRUE; X_INPUT_COORD_IS = LATLON; 	} 
				else if	( ! strcmp(subsetKey, "Lon"  ) ) { x_input_ul   = atof(p1); x_input_lr    = atof(p2); X_RANGE_DEFINED = TRUE; X_INPUT_COORD_IS = LATLON; 	} // Stupid humans that miss the "G"!!
				else if ( ! strcmp(subsetKey, "Lat"  ) ) { y_input_ul   = atof(p1); y_input_lr    = atof(p2); Y_RANGE_DEFINED = TRUE; Y_INPUT_COORD_IS = LATLON;	} 
				else if	( ! strcmp(subsetKey, "E"    ) ) { x_input_ul   = atof(p1); x_input_lr    = atof(p2); X_RANGE_DEFINED = TRUE; X_INPUT_COORD_IS = UTM;		} 
				else if ( ! strcmp(subsetKey, "N"    ) ) { y_input_ul   = atof(p1); y_input_lr    = atof(p2); Y_RANGE_DEFINED = TRUE; Y_INPUT_COORD_IS = UTM;		} 
				else if ( ! strcmp(subsetKey, "h"    ) ) { h_input_low  = atof(p1); h_input_high  = atof(p2); H_RANGE_DEFINED = TRUE; 					} 
				else if ( ! strcmp(subsetKey, "x"    ) ) { x_input_ul   = atof(p1); x_input_lr    = atof(p2); X_RANGE_DEFINED = TRUE; X_RANGE_DEFINED_PX = TRUE; 	} 
				else if ( ! strcmp(subsetKey, "y"    ) ) { y_input_ul   = atof(p1); y_input_lr    = atof(p2); Y_RANGE_DEFINED = TRUE; Y_RANGE_DEFINED_PX = TRUE;	} 

				else if ( ! strcmp(subsetKey, "sol"  ) ) { sol_input_start   = atof(p1); sol_input_stop   = atof(p2); SOL_RANGE_DEFINED = TRUE;	} 
				else if ( ! strcmp(subsetKey, "sl"   ) ) { sl_input_start    = atof(p1); sl_input_stop    = atof(p2); SL_RANGE_DEFINED = TRUE; 	} 



				else if ( ! strcmp(subsetKey, "ansi" ) ) { 
					s_time.tm_mday  = ( atol(p1) - 109207 ); 
					s_time.tm_hour  = 0;
					s_time.tm_min   = 0;
					s_time.tm_sec   = 0;
					t_start  	= timegm(&s_time); 	START_TIME_DEFINED 	= TRUE;

					e_time.tm_mday  = ( atol(p2) - 109207 );
					e_time.tm_hour  = 23; 
					e_time.tm_min   = 59; 
					e_time.tm_sec   = 59; 
					t_finish 	= timegm(&e_time); 	END_TIME_DEFINED 	= TRUE;
				}else if ( ! strcmp(subsetKey, "t" ) ) { 
					t_start  	= atol(p1);		START_TIME_DEFINED 	= TRUE;
					t_finish 	= atol(p2);		END_TIME_DEFINED 	= TRUE;

					if ( t_start > t_finish ) { t_tmp = t_finish; t_finish = t_start; t_start = t_tmp; }
					gmtime_r( &t_start,  &s_time);
					gmtime_r( &t_finish, &e_time);
				}else if ( ! strcmp(subsetKey, "unix" ) ){
					i = sscanf(p1, "%d-%d-%dT%d:%d:%d", &s_time.tm_year , &s_time.tm_mon, &s_time.tm_mday, &s_time.tm_hour, &s_time.tm_min, &s_time.tm_sec ); s_time.tm_year -= 1900; s_time.tm_mon -= 1;
					t_start = timegm(&s_time);		START_TIME_DEFINED	= TRUE;

					j = sscanf(p2, "%d-%d-%dT%d:%d:%d", &e_time.tm_year , &e_time.tm_mon, &e_time.tm_mday, &e_time.tm_hour, &e_time.tm_min, &e_time.tm_sec ); e_time.tm_year -= 1900; e_time.tm_mon -= 1;
					t_finish = timegm(&e_time);		END_TIME_DEFINED	= TRUE;

					if ( ( i == 3 ) && ( j == 3 ) ) {
						s_time.tm_hour =  0; s_time.tm_min  =  0; s_time.tm_sec =  0;
						e_time.tm_hour = 23; e_time.tm_min  = 59; e_time.tm_sec = 59;	t_start = timegm(&s_time);	t_finish = timegm(&e_time);  // Search in a day
					} else if ( ( i != 6 ) && ( j != 6 ) ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: subset=%s not well-formed", pszValue); GoodbyeMessage(info,"subset=%s not well-formed", pszValue); return 400; }

				}else if ( ! strcmp(subsetKey, "gmin" ) ){
					gmin_str = (char *)malloc(MAX_STR_LEN); bzero(gmin_str, MAX_STR_LEN - 1);
					sscanf(pszValue, "gmin(%[^')'])", gmin_str );
				}else if ( ! strcmp(subsetKey, "gmax" ) ){
					gmax_str = (char *)malloc(MAX_STR_LEN); bzero(gmax_str, MAX_STR_LEN - 1);
					sscanf(pszValue, "gmax(%[^')'])", gmax_str );
				}else if ( ! strcmp(subsetKey, "gfix" ) ){
					gmin_str = (char *)malloc(MAX_STR_LEN); bzero(gmin_str, MAX_STR_LEN - 1);
					gmax_str = (char *)malloc(MAX_STR_LEN); bzero(gmax_str, MAX_STR_LEN - 1);
					sscanf(pszValue, "gfix(%[^')'])", gmin_str );
					sscanf(pszValue, "gfix(%[^')'])", gmax_str );	
				}else if ( ! strcmp(subsetKey, "Layer" ) ){
					layer_str = (char *)malloc(MAX_STR_LEN); bzero(layer_str, MAX_STR_LEN - 1);
					sscanf(pszValue, "Layer(%[^')'])", layer_str );	
				}else if ( ! strcmp(subsetKey, "band" ) ){
					layer_str = (char *)malloc(MAX_STR_LEN); bzero(layer_str, MAX_STR_LEN - 1);
					sscanf(pszValue, "band(%[^')'])", layer_str );	
				}




		}
		query = query_ptr; tok = strtok_r( query, DELIMS, &query_ptr );
	}
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	
	// Skip_WCS2_Query: ;
	/*
	apr_bucket_brigade	*bucket_brigate;
	apr_bucket		*bucket;
	int 			upload_end 	= 0;
	int			upload_status 	= 0;
	int 			prefixFlag 	= FALSE;
	char			*python_script	= NULL;
	apr_size_t 		bytes;
	int			tot_upload_byte = 0;
	const char		*buf;
	int			UPLOAD_BLOCKSIZE = 512;

	bucket_brigate = apr_brigade_create(r->pool, r->connection->bucket_alloc);

	do {
		upload_status = ap_get_brigade(r->input_filters, bucket_brigate, AP_MODE_READBYTES, APR_BLOCK_READ, UPLOAD_BLOCKSIZE) ;
		if ( upload_status == APR_SUCCESS ) {
			for (bucket = APR_BRIGADE_FIRST(bucket_brigate) ; bucket != APR_BRIGADE_SENTINEL(bucket_brigate) ; bucket = APR_BUCKET_NEXT(bucket)) {
				if 	( APR_BUCKET_IS_EOS(bucket) ) { upload_end = 1; break; }
				else if ( apr_bucket_read(bucket, &buf, &bytes, APR_BLOCK_READ) == APR_SUCCESS ) {
					if ( ( tot_upload_byte + bytes ) >= MAX_UPLOAD_SIZE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Error upload file is too big"); return 400; }
					if ( python_script == NULL ) { python_script = (char *)malloc(MAX_UPLOAD_SIZE); bzero(python_script, MAX_UPLOAD_SIZE - 1); }

					memcpy(python_script + tot_upload_byte, apr_pstrndup(r->pool, buf, bytes), bytes );
					tot_upload_byte += bytes;
				} else { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Error in upload file reading"); return 500; }
        		}
    		} else { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Error in Brigade error"); return 500; }
		apr_brigade_cleanup(bucket_brigate);
	} while ( !upload_end && upload_status == APR_SUCCESS );

	apr_brigade_destroy(bucket_brigate);
	*/




	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	// Read POST
	apr_array_header_t *pairs = NULL;


	// Ok the problem is ... when i pass coord from wmts i lose precision in decimas of coord, so i have to store in data structure and copy
	if ( info->passthrough == TRUE ){ x_input_ul = info->x_input_ul; x_input_lr = info->x_input_lr; y_input_ul = info->y_input_ul; y_input_lr = info->y_input_lr; }



	// ap_parse_form_data(r, NULL, &pairs, -1, HUGE_STRING_LEN);
	
	if ( info->pairs == NULL ){
		if ( GeomPOSTSize > 0 )	ap_parse_form_data(r, NULL, &info->pairs, -1, GeomPOSTSize );
		else			ap_parse_form_data(r, NULL, &info->pairs, -1, 1024 * 1024 * 3 );
	}

	pairs = apr_array_copy(r->pool, info->pairs);

	char 	*geometry_post_string 	= NULL;
	char	*geometry_post_epsg	= NULL;

	while (pairs && !apr_is_empty_array(pairs)) {
		apr_off_t 	len	 = 0;
		apr_size_t 	size	 = 0;
		char 		*buffer	 = NULL;
		ap_form_pair_t	*pair 	= (ap_form_pair_t *) apr_array_pop(pairs);
		apr_brigade_length(pair->value, 1, &len); 	size = (apr_size_t) len;
		buffer = apr_palloc(r->pool, size + 1); 	apr_brigade_flatten(pair->value, buffer, &size); buffer[len] = '\0';
		if 	( ! strcmp( pair->name, "geometry" 	) ) { geometry_post_string = malloc( sizeof(char) * (strlen(buffer) + 1) ); strcpy(geometry_post_string,	buffer); geometry_post_string[	strlen(buffer)] = '\0'; }
		else if ( ! strcmp( pair->name, "epsg" 		) ) { geometry_post_epsg   = malloc( sizeof(char) * (strlen(buffer) + 1) ); strcpy(geometry_post_epsg,		buffer); geometry_post_epsg[	strlen(buffer)] = '\0'; }
		else if	( ! strcmp( pair->name, "module"  	) ) { info->module 	   = malloc( sizeof(char) * (strlen(buffer) + 1) ); strcpy(info->module,            	buffer); info->module[          strlen(buffer)] = '\0'; }
		else if ( ! strcmp( pair->name, "moduleId"	) ) addStatToPush(info, "moduleId",  buffer, GFT_String );  // Progatate to analytics .. why? .. dunno
		else if ( ! strcmp( pair->name, "_id" 		) ) { 
			for ( i = 0; i < MAX_D; i++ ) granuleId[i] = NULL;
			char *tok_gr = NULL; char *tok_ptr = NULL;
			for ( tok_gr = strtok_r( buffer, ",", &tok_ptr ), i = 0; tok_gr != NULL; buffer = tok_ptr, tok_gr = strtok_r( buffer, ",", &tok_ptr), i++ ){ 
				granuleId[i] = malloc( sizeof(char) * (strlen(tok_gr) + 1) ); strcpy(granuleId[i], tok_gr); granuleId[i][strlen(tok_gr)] = '\0'; 
			}
			
		}
	}	

	if ( geometry_post_string != NULL ) {
		geoSRSsrc = OSRNewSpatialReference(NULL);

		int tmp_epsg = 4326;
		if ( geometry_post_epsg != NULL )  	tmp_epsg = atoi(geometry_post_epsg);
		else					tmp_epsg = 4326;	
		
		if ( ImportFromEPSG(&geoSRSsrc, tmp_epsg ) != OGRERR_NONE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: EPSG for geometry `%s'", geometry_post_epsg ); GoodbyeMessage(info,"EPSG for geometry error"); return 400; } 
		#if GDAL_VERSION >= 304
		OSRSetAxisMappingStrategy(geoSRSsrc, OAMS_TRADITIONAL_GIS_ORDER); // GDAL 3.0 Fix
		#endif
		addStatToPush(info, "polygon",  geometry_post_string, GFT_String); // STATS

		OGREnvelope sEnvelope;
		if ( OGR_G_CreateFromWkt(&geometry_post_string, geoSRSsrc ,&hGeom) ) 
			{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to parsing geometry `%.*s ...'", 50, geometry_post_string); 	GoodbyeMessage(info, "Unable to parsing geometry `%.*s ...'");	return 400; }
		if ( OGR_G_IsValid(hGeom) == FALSE	   	   	     	   ) 
			{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Input geometry is not valid!" ); 						GoodbyeMessage(info, "Input geometry is not valid!"); 		return 400; }


		OGR_G_GetEnvelope(hGeom, &sEnvelope);
		 
		if ( X_RANGE_DEFINED != TRUE ) { x_input_ul = sEnvelope.MinX; x_input_lr = sEnvelope.MaxX; X_RANGE_DEFINED = TRUE; X_INPUT_COORD_IS = ( tmp_epsg == 4326 ) ? LATLON : UTM; } 
		if ( Y_RANGE_DEFINED != TRUE ) { y_input_lr = sEnvelope.MinY; y_input_ul = sEnvelope.MaxY; Y_RANGE_DEFINED = TRUE; Y_INPUT_COORD_IS = ( tmp_epsg == 4326 ) ? LATLON : UTM; } 
		INPUT_GEOM = TRUE;			


		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Using to clip raster input %s (UL: %f,%f LR: %f,%f )", OGR_G_GetGeometryName (hGeom), sEnvelope.MinX,  sEnvelope.MaxY, sEnvelope.MaxX, sEnvelope.MinY );
	}


	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

	if ( outFormat   == NULL ){ outFormat   = malloc(256); bzero(outFormat,   254); sprintf(outFormat,   "GTiff"); } // JPEG GTiff


	if ( ( hOutDriver = GDALGetDriverByName(outFormat) ) == NULL ){
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: GDALGetDriverByName incorrect outFormat %s", outFormat); 
		return 500;
	}



	if ( x_input_ul   > x_input_lr    ) { tmp_d = x_input_lr;   x_input_lr   = x_input_ul;    x_input_ul    = tmp_d; }
	if ( y_input_ul   < y_input_lr    ) { tmp_d = y_input_lr;   y_input_lr   = y_input_ul;    y_input_ul    = tmp_d; }
	if ( h_input_low  > h_input_high  ) { tmp_d = h_input_high; h_input_high = h_input_low;   h_input_low   = tmp_d; }
	if ( ( X_RANGE_DEFINED == TRUE   ) && ( Y_RANGE_DEFINED == TRUE   ) && ( x_input_ul  == x_input_lr    ) && ( y_input_ul == y_input_lr    ) ) outscale = 1.0;

	if ( Y_RANGE_DEFINED_PX == TRUE ) if ( y_input_ul   > y_input_lr    ) { tmp_d = y_input_lr;   y_input_lr   = y_input_ul;    y_input_ul    = tmp_d; } 
	if ( X_RANGE_DEFINED_PX == TRUE ) { if ( x_input_ul < 0 ) x_input_ul = 0; if ( x_input_lr < 0 ) x_input_lr = 0; }
	if ( Y_RANGE_DEFINED_PX == TRUE ) { if ( y_input_ul < 0 ) y_input_ul = 0; if ( y_input_lr < 0 ) y_input_lr = 0; }


	if ( 	( START_TIME_DEFINED == FALSE ) && ( END_TIME_DEFINED == FALSE )	&&  
		( t_start == 0 ) 		&& ( t_finish == 0 ) 			){ time(&t_finish); gmtime_r( &t_start,  &s_time); gmtime_r( &t_finish, &e_time); START_TIME_DEFINED = TRUE; END_TIME_DEFINED = TRUE;}

	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	
	if ( ( outType == CSV 	) && ( ( x_input_ul != x_input_lr ) || ( y_input_ul != y_input_lr ) ) ) { 
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Output format CSV Only for time series"); 	GoodbyeMessage(info,"Output format CSV Only for time series"); 	 return 415; }
	if ( ( outType == CHART ) && ( ( x_input_ul != x_input_lr ) || ( y_input_ul != y_input_lr ) ) ) { 
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Output format Chart Only for time series"); GoodbyeMessage(info,"Output format Chart Only for time series"); return 415; }

	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	
	
	
	if ( 	( INPUT_GEOM 		== FALSE  ) && ( CROP 			== FALSE  ) &&
		( X_RANGE_DEFINED 	== TRUE   ) && ( Y_RANGE_DEFINED 	== TRUE   ) && 
		( X_INPUT_COORD_IS 	== LATLON ) && ( Y_INPUT_COORD_IS 	== LATLON ) ){

	 		OGRSpatialReferenceH geoSRSsrc = OSRNewSpatialReference(NULL);
	                #if GDAL_VERSION >= 304
	                OSRSetAxisMappingStrategy(geoSRSsrc, OAMS_TRADITIONAL_GIS_ORDER); // GDAL 3.0 Fix
	                #endif

			ImportFromEPSG(&geoSRSsrc, 4326 );
			OGREnvelope sEnvelope;
			char *buffer  = (char *)malloc(MAX_STR_LEN); bzero(buffer, MAX_STR_LEN - 1);
			
			if ( ( x_input_ul == x_input_lr ) && ( y_input_ul == y_input_lr ) ) 	sprintf(buffer, "POINT (%f %f)", x_input_ul, y_input_ul );
			else 									sprintf(buffer, "POLYGON ((%f %f, %f %f, %f %f, %f %f, %f %f))",  
																x_input_ul, y_input_ul, 
																x_input_ul, y_input_lr,
																x_input_lr, y_input_lr, 
																x_input_lr, y_input_ul,
																x_input_ul, y_input_ul );



			if ( OGR_G_CreateFromWkt(&buffer, geoSRSsrc ,&hGeom) ) {  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to parsing geometry `%.*s ...'", 50, buffer); 
			       							GoodbyeMessage(info, "Unable to parsing geometry `%.*s ...'", 50, buffer ); 	return 400; }
			if ( OGR_G_IsValid(hGeom) == FALSE	   	     ) {  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Input geometry is not valid!" ); 
										GoodbyeMessage(info, "Input geometry is not valid!" );				return 400; }

			INPUT_GEOM = TRUE;			
	}


	
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

	if ( mathChain != NULL ){
		if (!parse(mathChain))	{  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to parsing `%s'", mathChain->coveragemath);  GoodbyeMessage(info, "Unable to parsing `%s'", mathChain->coveragemath); return 400; }

		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Input math string: '%s' is OK ..", mathChain->coveragemath); 

		for (i = 0, i_prod = 0; i < mathChain->l_queue; i++){
			if ( mathChain->queue[i].s[0] == '+') continue;
			if ( mathChain->queue[i].s[0] == '-') continue;
			if ( mathChain->queue[i].s[0] == '*') continue;
			if ( mathChain->queue[i].s[0] == '/') continue;
			if ( mathChain->queue[i].s[0] == '^') continue;
			if ( mathChain->queue[i].s[0] == '%') continue;
			if ( mathChain->queue[i].s[0] == '<') continue;
			if ( mathChain->queue[i].s[0] == '>') continue;
			if ( mathChain->queue[i].s[0] == ':') continue;
	                if ( mathChain->queue[i].s[0]   == '&') continue;
	                if ( mathChain->queue[i].s[0]   == '|') continue;
       	              	if ( ( mathChain->queue[i].s[0] == '>') && ( mathChain->queue[i].s[1] == '>') ) continue;
       	               	if ( ( mathChain->queue[i].s[0] == '<') && ( mathChain->queue[i].s[1] == '<') ) continue;
			if ( ( mathChain->queue[i].s[0] == '!') && ( mathChain->queue[i].s[1] == '=') ) continue;
			if ( ( mathChain->queue[i].s[0] == '=') && ( mathChain->queue[i].s[1] == '=') ) continue;
			if ( ( mathChain->queue[i].s[0] == '&') && ( mathChain->queue[i].s[1] == '&') ) continue;
			if ( ( mathChain->queue[i].s[0] == '|') && ( mathChain->queue[i].s[1] == '|') ) continue;
			if ( ! strncmp(mathChain->queue[i].s, "AND", mathChain->queue[i].len) )	  	continue;	
			if ( ! strncmp(mathChain->queue[i].s, "OR",  mathChain->queue[i].len) )	  	continue;	
			
			double x;
			bzero(buff, 99); sprintf(buff, "%.*s\n", mathChain->queue[i].len, mathChain->queue[i].s);
			if ( sscanf(buff, "%lf", &x)  > 0 ) continue;
			for (j = 0, PROD_FOUND = FALSE; prod_array[j] != NULL; j++ ) if ( ! strncmp(prod_array[j], mathChain->queue[i].s, mathChain->queue[i].len ) )  { PROD_FOUND = TRUE; break; }
			if ( PROD_FOUND == TRUE )  continue;

//	                ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Using for math product: %.*s", mathChain->queue[i].len, mathChain->queue[i].s);
			
                        prod_path_array[i_prod] = (char *)malloc(sizeof(char) * MAX_STR_LEN);
                        prod_array[i_prod]      = (char *)malloc(sizeof(char) * MAX_STR_LEN);
			sprintf(prod_path_array[i_prod],  	"%s/%.*s",  ROOT, mathChain->queue[i].len, mathChain->queue[i].s);
			sprintf(prod_array[i_prod],  		"%.*s", 	  mathChain->queue[i].len, mathChain->queue[i].s);
			i_prod++;
			
		}
	}
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	if ( Request == GetCapabilities ){
		clock_gettime(CLOCK_REALTIME, &time_before);
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Enter in GetCapabilities");


		if ( ROOT != NULL ) dp = (*openSource)(ROOT);
		if (  dp == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Product ROOT not exists %s", ROOT); return 500; }
	
		if ( outType != JSON ){


		if ( output_name != NULL ) { bzero(final_path, MAX_PATH_LEN - 1); sprintf(final_path, "attachment; filename=\"%s\"", output_name ); apr_table_add(r->headers_out, "Content-disposition", final_path); }
		ap_set_content_type(r, "text/xml");

		ap_rprintf(r, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		ap_rprintf(r, "<wcs:Capabilities\n");
		ap_rprintf(r, "	xsi:schemaLocation='http://www.opengis.net/wcs/2.0 http://schemas.opengis.net/wcs/2.0/wcsAll.xsd'\n");
		ap_rprintf(r, "	version='2.0.1'\n");
		ap_rprintf(r, "	xmlns:wcs='http://www.opengis.net/wcs/2.0'\n");
		ap_rprintf(r, "	xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'\n");
		ap_rprintf(r, "	xmlns:crs='http://www.opengis.net/wcs/service-extension/crs/1.0'\n");
		ap_rprintf(r, "	xmlns:ows='http://www.opengis.net/ows/2.0'\n");
		ap_rprintf(r, "	xmlns:gml='http://www.opengis.net/gml/3.2'\n");
		ap_rprintf(r, "	xmlns:xlink='http://www.w3.org/1999/xlink'>\n");
		ap_rprintf(r, "  <ows:ServiceIdentification xmlns='http://www.opengis.net/ows/2.0'>\n");
		#ifdef MWCS_VERSION
		ap_rprintf(r, "    <ows:Title>%s - MWCS (Ver. %s)</ows:Title>\n", MWCSTitle != NULL ? MWCSTitle : "MEEO WCS Server",  MWCS_VERSION);
		#else
		ap_rprintf(r, "    <ows:Title>%s - MWCS</ows:Title>\n", MWCSTitle != NULL ? MWCSTitle : "MEEO WCS Server" );
		#endif
		ap_rprintf(r, "    <ows:Abstract>The WCS Server implementation to access datacubes and images on filesystem</ows:Abstract>\n");
		ap_rprintf(r, "    <ows:ServiceType>OGC WCS</ows:ServiceType>\n");
		ap_rprintf(r, "    <ows:ServiceTypeVersion>2.0.1</ows:ServiceTypeVersion>\n");
		ap_rprintf(r, "    <ows:Profile>http://www.opengis.net/spec/WCS_coverage-encoding_jpeg2000/1.0/</ows:Profile>\n");
		ap_rprintf(r, "    <ows:Profile>http://www.opengis.net/spec/WCS_protocol-binding_get-rest/1.0/conf/get-rest</ows:Profile>\n");
		ap_rprintf(r, "    <ows:Profile>http://www.opengis.net/spec/GMLCOV/1.0/conf/gml-coverage</ows:Profile>\n");
		ap_rprintf(r, "    <ows:Profile>http://www.opengis.net/spec/WCS_service-extension_processing/2.0/conf/processing</ows:Profile>\n");
		ap_rprintf(r, "    <ows:Profile>http://www.opengis.net/spec/WCS_protocol-binding_post-xml/1.0</ows:Profile>\n");
		ap_rprintf(r, "    <ows:Profile>http://www.opengis.net/spec/WCS_protocol-binding_soap/1.0</ows:Profile>\n");
		ap_rprintf(r, "    <ows:Profile>http://www.opengis.net/spec/WCS_coverage-encoding_netcdf/1.0/</ows:Profile>\n");
		ap_rprintf(r, "    <ows:Profile>http://www.opengis.net/spec/GMLJP2/2.0/</ows:Profile>\n");
		ap_rprintf(r, "    <ows:Profile>http://www.opengis.net/spec/WCS_protocol-binding_get-kvp/1.0/conf/get-kvp</ows:Profile>\n");
		ap_rprintf(r, "    <ows:Profile>http://www.opengis.net/spec/WCS_service-extension_range-subsetting/1.0/conf/</ows:Profile>\n");
		ap_rprintf(r, "    <ows:Profile>http://www.opengis.net/spec/WCS_service-extension_scaling/1.0/conf/scaling</ows:Profile>\n");
		ap_rprintf(r, "    <ows:Profile>http://www.opengis.net/spec/WCS_service-extension_interpolation/1.0/conf/interpolation</ows:Profile>\n");
		ap_rprintf(r, "    <ows:Profile>http://www.opengis.net/spec/WCS_coverage-encoding_geotiff/1.0/</ows:Profile>\n");
		ap_rprintf(r, "  </ows:ServiceIdentification>\n");
		ap_rprintf(r, "  <ows:ServiceProvider xmlns='http://www.opengis.net/ows/2.0'>\n");
		ap_rprintf(r, "    <ows:ProviderName>%s</ows:ProviderName>\n", 			MWCSProviderName 	!= NULL ? MWCSProviderName 	: "MEEO S.r.l." );
		ap_rprintf(r, "    <ows:ProviderSite xlink:href='%s'/>\n", 			MWCSProviderSite 	!= NULL ? MWCSProviderSite 	: "http://www.meeo.it/" );
		ap_rprintf(r, "    <ows:ServiceContact>\n");
		ap_rprintf(r, "      <ows:IndividualName>%s</ows:IndividualName>\n", 		MWCSIndividualName 	!= NULL ? MWCSIndividualName 	: "MEEO Help Desk" );
		ap_rprintf(r, "      <ows:ContactInfo>\n");
		ap_rprintf(r, "        <ows:Address>\n");
		ap_rprintf(r, "          <ows:City>%s</ows:City>\n", 				MWCSCity 		!= NULL ? MWCSCity 		: "Ferrara");
		ap_rprintf(r, "          <ows:PostalCode>%s</ows:PostalCode>\n",		MWCSPostalCode 		!= NULL ? MWCSPostalCode 	: "I-44123");
		ap_rprintf(r, "          <ows:Country>%s</ows:Country>\n",			MWCSCountry          	!= NULL ? MWCSCountry        	: "Italy");
		ap_rprintf(r, "          <ows:ElectronicMailAddress>%s</ows:ElectronicMailAddress>\n", MWCSElectronicMailAddress != NULL ? MWCSElectronicMailAddress : "helpdesk@meeo.it" );
		ap_rprintf(r, "        </ows:Address>\n");
		ap_rprintf(r, "      </ows:ContactInfo>\n");
		ap_rprintf(r, "      <ows:Role>Service Provider</ows:Role>\n");
		ap_rprintf(r, "    </ows:ServiceContact>\n");
		ap_rprintf(r, "  </ows:ServiceProvider>\n");
		ap_rprintf(r, "  <ows:OperationsMetadata xmlns='http://www.opengis.net/ows/2.0'>\n");
		ap_rprintf(r, "    <ows:Operation name='GetCapabilities'>\n");
		ap_rprintf(r, "      <ows:DCP>\n");
		ap_rprintf(r, "        <ows:HTTP>\n");
		ap_rprintf(r, "          <ows:Post xlink:href='%s'/>\n", ExtHostname);
		ap_rprintf(r, "          <ows:Get xlink:href='%s'/>\n",  ExtHostname);
		ap_rprintf(r, "        </ows:HTTP>\n");
		ap_rprintf(r, "      </ows:DCP>\n");
		ap_rprintf(r, "      <ows:Constraint name='PostEncoding'>\n");
		ap_rprintf(r, "        <ows:AllowedValues>\n");
		ap_rprintf(r, "          <ows:Value>XML</ows:Value>\n");
		ap_rprintf(r, "          <ows:Value>SOAP</ows:Value>\n");
		ap_rprintf(r, "        </ows:AllowedValues>\n");
		ap_rprintf(r, "      </ows:Constraint>\n");
		ap_rprintf(r, "    </ows:Operation>\n");
		ap_rprintf(r, "    <ows:Operation name='DescribeCoverage'>\n");
		ap_rprintf(r, "      <ows:DCP>\n");
		ap_rprintf(r, "        <ows:HTTP>\n");
		ap_rprintf(r, "          <ows:Post xlink:href='%s'/>\n", ExtHostname);
		ap_rprintf(r, "          <ows:Get xlink:href='%s'/>\n",   ExtHostname);
		ap_rprintf(r, "        </ows:HTTP>\n");
		ap_rprintf(r, "      </ows:DCP>\n");
		ap_rprintf(r, "      <ows:Constraint name='PostEncoding'>\n");
		ap_rprintf(r, "        <ows:AllowedValues>\n");
		ap_rprintf(r, "          <ows:Value>XML</ows:Value>\n");
		ap_rprintf(r, "          <ows:Value>SOAP</ows:Value>\n");
		ap_rprintf(r, "        </ows:AllowedValues>\n");
		ap_rprintf(r, "      </ows:Constraint>\n");
		ap_rprintf(r, "    </ows:Operation>\n");
		ap_rprintf(r, "    <ows:Operation name='GetCoverage'>\n");
		ap_rprintf(r, "      <ows:DCP>\n");
		ap_rprintf(r, "        <ows:HTTP>\n");
		ap_rprintf(r, "          <ows:Post xlink:href='%s'/>\n", ExtHostname);
		ap_rprintf(r, "          <ows:Get xlink:href='%s'/>\n",  ExtHostname);
		ap_rprintf(r, "        </ows:HTTP>\n");
		ap_rprintf(r, "      </ows:DCP>\n");
		ap_rprintf(r, "      <ows:Constraint name='PostEncoding'>\n");
		ap_rprintf(r, "        <ows:AllowedValues>\n");
		ap_rprintf(r, "          <ows:Value>XML</ows:Value>\n");
		ap_rprintf(r, "          <ows:Value>SOAP</ows:Value>\n");
		ap_rprintf(r, "        </ows:AllowedValues>\n");
		ap_rprintf(r, "      </ows:Constraint>\n");
		ap_rprintf(r, "    </ows:Operation>\n");
		ap_rprintf(r, "    <ows:Constraint name='PostEncoding'>\n");
		ap_rprintf(r, "      <ows:AllowedValues>\n");
		ap_rprintf(r, "        <ows:Value>XML</ows:Value>\n");
		ap_rprintf(r, "        <ows:Value>SOAP</ows:Value>\n");
		ap_rprintf(r, "      </ows:AllowedValues>\n");
		ap_rprintf(r, "    </ows:Constraint>\n");
		ap_rprintf(r, "  </ows:OperationsMetadata>\n");
		ap_rprintf(r, "  <wcs:ServiceMetadata xmlns='http://www.opengis.net/ows/2.0'>\n");
		ap_rprintf(r, "    <wcs:formatSupported>image/tiff</wcs:formatSupported>\n");
		ap_rprintf(r, "    <wcs:formatSupported>image/jp2</wcs:formatSupported>\n");
		ap_rprintf(r, "    <wcs:formatSupported>image/png</wcs:formatSupported>\n");
		ap_rprintf(r, "    <wcs:formatSupported>application/gml+xml</wcs:formatSupported>\n");
		ap_rprintf(r, "    <wcs:Extension>\n");
		ap_rprintf(r, "      <int:InterpolationMetadata xmlns:int='http://www.opengis.net/wcs/interpolation/1.0'>\n");
		ap_rprintf(r, "        <int:InterpolationSupported>http://www.opengis.net/def/interpolation/OGC/0/nearest-neighbor</int:InterpolationSupported>\n");
		ap_rprintf(r, "      </int:InterpolationMetadata>\n");
		ap_rprintf(r, "    </wcs:Extension>\n");
		ap_rprintf(r, "  </wcs:ServiceMetadata>\n");
		ap_rprintf(r, "  <Contents xmlns='http://www.opengis.net/wcs/2.0'>\n");
		}


		if ( ROOT != NULL ) while ( (ep = (*listSource)(dp)) ){
                	if ( ep->d_name[0] == '.' ) continue; 
			bzero(prod_path, MAX_STR_LEN - 1);
			sprintf(prod_path, "%s/%s", ROOT, ep->d_name);

			if ( generateProdInfo(prod_path, killable, r, token_from_user )) continue;
			bzero(imgs_path, MAX_PATH_LEN - 1);

			//-------------------------------------------------------------------------------
			

			// JSON NEW PART

			sprintf(imgs_path, "%s/DescribeCoverage.json", prod_path );


			DC = (*openSourceFile)( imgs_path, "r" );
			if ( DC == NULL ) continue;
			
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: GetCapabilities Using JSON DescribeCoverage for %s ...", ep->d_name );  

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
			high_min = high_max = DEFAULT_NODATA;	
			if ( jobj_dc != NULL ) {
				for( i = 0; i < json_object_array_length(jobj_dc); i++){
					json_object_object_foreach( json_object_array_get_idx(jobj_dc, i) , key, val){
						if 	( ! strcmp(key, "id" 		) ) strcpy(epsg_res_tmp, json_object_get_string(val) );
						else if ( ! strcmp(key, "EPSG" 		) ) proj 		= json_object_get_int(val);
						else if ( ! strcmp(key, "GeoY_lr" 	) ) lowerCorner[0] 	= json_object_get_double(val);
						else if ( ! strcmp(key, "GeoX_ul" 	) ) lowerCorner[1] 	= json_object_get_double(val);
						else if ( ! strcmp(key, "GeoY_ul" 	) ) upperCorner[0] 	= json_object_get_double(val);
						else if ( ! strcmp(key, "GeoX_lr" 	) ) upperCorner[1] 	= json_object_get_double(val);
						else if ( ! strcmp(key, "x_res" 	) ) x_min_res	 	= json_object_get_double(val);
						else if ( ! strcmp(key, "y_res" 	) ) y_min_res	 	= json_object_get_double(val);
						else if ( ! strcmp(key, "pxAOISizeX" 	) ) pxAOISizeX	 	= json_object_get_int(val);
						else if ( ! strcmp(key, "pxAOISizeY" 	) ) pxAOISizeY	 	= json_object_get_int(val);
						else if ( ! strcmp(key, "nband" 	) ) nband	 	= json_object_get_int(val);
						else if ( ! strcmp(key, "t_min" 	) ) t_min	 	= json_object_get_int64(val);
						else if ( ! strcmp(key, "t_max" 	) ) t_max	 	= json_object_get_int64(val);
						else if ( ! strcmp(key, "high_min" 	) ) high_min	 	= json_object_get_double(val);
						else if ( ! strcmp(key, "high_max" 	) ) high_max	 	= json_object_get_double(val);
						else if ( ! strcmp(key, "type" 		) ) type	 	= json_object_get_int(val);
						else if ( ! strcmp(key, "hit_number" 	) ) hit_number	 	= json_object_get_int(val);
						else continue; 
					}
				if ( ( prod_array[0] != NULL ) && ( ! strstr(ep->d_name, prod_array[0] ) ) ) continue; // GetCoverage filter

		bzero(buff, 99); sprintf(buff, "%g", x_min_res);
		ap_rprintf(r, "    <CoverageSummary>\n");
		ap_rprintf(r, "      <CoverageId>%s_%s", ep->d_name, epsg_res_tmp); ap_rprintf(r, "</CoverageId>\n");
		ap_rprintf(r, "      <CoverageSubtype>RectifiedGridCoverage</CoverageSubtype>\n");
		ap_rprintf(r, "      <CoverageSubtypeParent>\n");
		ap_rprintf(r, "        <CoverageSubtype>AbstractDiscreteCoverage</CoverageSubtype>\n");
		ap_rprintf(r, "        <CoverageSubtypeParent>\n");
		ap_rprintf(r, "          <CoverageSubtype>AbstractCoverage</CoverageSubtype>\n");
		ap_rprintf(r, "        </CoverageSubtypeParent>\n");
		ap_rprintf(r, "      </CoverageSubtypeParent>\n");
		ap_rprintf(r, "      <BoundingBox\n");
		ap_rprintf(r, " crs='http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s'\n", proj, time_URL[time_index]);
		ap_rprintf(r, "	 dimensions='%d' xmlns='http://www.opengis.net/ows/2.0'>\n", (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA )) ? 4 : 3 );
		ap_rprintf(r, "        <lowerCorner>%f %f %ld", lowerCorner[0], lowerCorner[1], t_min);	if (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA ))   ap_rprintf(r, " %f", high_min);	ap_rprintf(r, "</lowerCorner>\n");
		ap_rprintf(r, "        <upperCorner>%f %f %ld", upperCorner[0], upperCorner[1], t_max); if (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA ))   ap_rprintf(r, " %f", high_max);	ap_rprintf(r, "</upperCorner>\n");
		ap_rprintf(r, "      </BoundingBox>\n");
		ap_rprintf(r, "    </CoverageSummary>\n");
					bzero(epsg_res_tmp, 255);

				}
			}
		}
		if ( outType != JSON ){
		ap_rprintf(r, "  </Contents>\n");
		ap_rprintf(r, "</wcs:Capabilities>\n");
		}

		(*closeSource)(dp);
		
		clock_gettime(CLOCK_REALTIME, &time_after);
		time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;

		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Finish in GetCapabilities in %.3f sec, Total time %.3f sec", 
				(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);		

		return 0;
	}


	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	addStatToPush(info, "datasetId",  prod_array[0], GFT_String ); // STATS

	if ( Request == DescribeCoverage ){
		clock_gettime(CLOCK_REALTIME, &time_before); ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Enter in DescribeCoverage");

		DescCoverage head	= NULL;
		DescCoverage cursor	= NULL;

		i_prod = 0; 
		if ( ROOT != NULL ) dp = (*openSource)(ROOT);
		if ( dp == NULL  )				{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Product ROOT not exists %s", ROOT);					return 500; }
		if ( prod_array[i_prod] == NULL )		{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Product not defined"); GoodbyeMessage(info, "Product not defined"); 	return 404; }
		strcpy( prod, prod_array[i_prod] );

		PROD_FOUND = FALSE;
		while ( (ep = (*listSource)(dp)) ){
                	if ( ep->d_name[0] == '.' ) continue; 

			if ( strncmp(prod, ep->d_name, strlen(ep->d_name)) ) continue;

			bzero(prod_path, MAX_STR_LEN - 1);
			sprintf(prod_path, "%s/%s", ROOT, ep->d_name);

			if (generateProdInfo(prod_path, killable, r, token_from_user) ) continue;
			
			bzero( epsg_res_ref, 255);
			strcpy(epsg_res_ref, prod + strlen(ep->d_name) + 1 );

			//-------------------------------------------------------------------------------
			// JSON NEW PART
			bzero(imgs_path, MAX_STR_LEN - 1);
			sprintf(imgs_path, "%s/DescribeCoverage.json", prod_path );

			DC = (*openSourceFile)( imgs_path, "r" );
			if ( DC == NULL ) continue;
			
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: DescribeCoverage Using JSON DescribeCoverage for %s ...", ep->d_name );	

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
						if 	( ! strcmp(key, "id" 		) ) strcpy(epsg_res_tmp, json_object_get_string(val) );
						else if ( ! strcmp(key, "EPSG" 		) ) proj 		= json_object_get_int(val);
						else if ( ! strcmp(key, "GeoY_lr" 	) ) lowerCorner[0] 	= json_object_get_double(val);
						else if ( ! strcmp(key, "GeoX_ul" 	) ) lowerCorner[1] 	= json_object_get_double(val);
						else if ( ! strcmp(key, "GeoY_ul" 	) ) upperCorner[0] 	= json_object_get_double(val);
						else if ( ! strcmp(key, "GeoX_lr" 	) ) upperCorner[1] 	= json_object_get_double(val);
						else if ( ! strcmp(key, "x_res" 	) ) x_min_res	 	= json_object_get_double(val);
						else if ( ! strcmp(key, "y_res" 	) ) y_min_res	 	= json_object_get_double(val);
						else if ( ! strcmp(key, "pxAOISizeX" 	) ) pxAOISizeX	 	= json_object_get_int(val);
						else if ( ! strcmp(key, "pxAOISizeY" 	) ) pxAOISizeY	 	= json_object_get_int(val);
						else if ( ! strcmp(key, "nband" 	) ) nband	 	= json_object_get_int(val);
						else if ( ! strcmp(key, "t_min" 	) ) t_min	 	= json_object_get_int64(val);
						else if ( ! strcmp(key, "t_max" 	) ) t_max	 	= json_object_get_int64(val);
						else if ( ! strcmp(key, "high_min" 	) ) high_min	 	= json_object_get_double(val);
						else if ( ! strcmp(key, "high_max" 	) ) high_max	 	= json_object_get_double(val);
						else if ( ! strcmp(key, "type" 		) ) type	 	= json_object_get_int(val);
						else if ( ! strcmp(key, "hit_number" 	) ) hit_number	 	= json_object_get_int(val);
						else continue; 
					}

					if (	( strlen(epsg_res_tmp) == strlen(epsg_res_ref) ) 	&&
						( ! strcmp(epsg_res_ref, epsg_res_tmp ) ) 		) { PROD_FOUND = TRUE; break; }

					if ( ( strlen(epsg_res_ref) > 0 ) && ( ! strstr( epsg_res_tmp, epsg_res_ref ) ) ) continue;

					if ( head == NULL ) 	{ head = cursor = (DescCoverage)malloc(sizeof(struct sDescCoverage));				}
					else			{ cursor->next  = (DescCoverage)malloc(sizeof(struct sDescCoverage)); cursor = cursor->next; 	}



					cursor->epsg		= proj;
					cursor->GeoY_lr 	= lowerCorner[0];
					cursor->GeoX_ul 	= lowerCorner[1];
					cursor->GeoY_ul 	= upperCorner[0];
					cursor->GeoX_lr 	= upperCorner[1];
					cursor->x_res 		= x_min_res;
					cursor->y_res 		= y_min_res;
					cursor->pxAOISizeX	= pxAOISizeX;
					cursor->pxAOISizeY	= pxAOISizeY;
					cursor->nband		= nband;
					cursor->t_min		= t_min;
					cursor->t_max		= t_max;
					cursor->high_min 	= high_min;
					cursor->high_max 	= high_max;
					cursor->type		= type;
					cursor->hit_number 	= hit_number;
					cursor->next		= NULL;

					bzero(epsg_res_tmp, 255);
				}
			}

			if ( ( PROD_FOUND == FALSE ) && ( i == 1 ) && ( proj == 4326 ) ) PROD_FOUND = TRUE; // if not specify epsg and resolution for EPSG:4326 and only one coverage it's ok ... 

			if ( proj != 4326 ) 	i = 1;
			else			i = 0;
			if ( PROD_FOUND == TRUE ) break;

		}
		(*closeSource)(dp);


		
		if ( ( PROD_FOUND == FALSE ) && ( proj != 4326 ) && ( head != NULL ) ) { printCoverageDescriptions(r, prod, head); goto EO_DescribeCoverage; }

		if ( PROD_FOUND == FALSE ) { printNoCoverageFound(r, prod); ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Product %s not exists", prod ); GoodbyeMessage(info, "Product %s not exists", prod ); return 404; };
			
		//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
		if ( output_name != NULL ) { bzero(final_path, MAX_PATH_LEN - 1); sprintf(final_path, "attachment; filename=\"%s\"", output_name ); apr_table_add(r->headers_out, "Content-disposition", final_path); }
                ap_set_content_type(r, "text/xml");

		ap_rprintf(r, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");

		if ( gml_version == 321 ){
	
		ap_rprintf(r, "<wcs:CoverageDescriptions xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n");
		ap_rprintf(r, " xmlns=\"http://www.opengis.net/gml/3.2\"\n");
		ap_rprintf(r, " xmlns:wcs=\"http://www.opengis.net/wcs/2.0\"\n");
		ap_rprintf(r, " xmlns:ows=\"http://www.opengis.net/ows/2.0\"\n");
		ap_rprintf(r, " xmlns:gml=\"http://www.opengis.net/gml/3.2\"\n");
		ap_rprintf(r, " xmlns:gmlcov=\"http://www.opengis.net/gmlcov/1.0\"\n");
		ap_rprintf(r, " xmlns:swe=\"http://www.opengis.net/swe/2.0\"\n");
		ap_rprintf(r, " xmlns:crs=\"http://www.opengis.net/wcs/service-extension/crs/1.0\"\n");
		ap_rprintf(r, " xmlns:xlink=\"http://www.w3.org/1999/xlink\"\n");
		ap_rprintf(r, " xsi:schemaLocation=\"http://schemas.opengis.net/wcs/2.0 http://www.opengis.net/wcs/2.0/wcsDescribeCoverage.xsd\">\n");
		ap_rprintf(r, "  <wcs:CoverageDescription>\n");
                ap_rprintf(r, "  <gml:boundedBy>\n");
                ap_rprintf(r, "     <gml:Envelope srsName=\"http://www.opengis.net/def/crs/EPSG/0/%d\" axisLabels=\"%s %s %s\" uomLabels=\"%s %s %s\" srsDimension=\"%d\">\n",
                                proj, dimenstion_Label[i][0], dimenstion_Label[i][1], time_Label[time_index], dimenstion_unit[i], dimenstion_unit[i], time_unit[time_index], (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA )) ? 4 : 3 );
                ap_rprintf(r, "      <gml:lowerCorner>%f %f %ld", lowerCorner[0], lowerCorner[1], t_min); if (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA ))   ap_rprintf(r, " %f", high_min); ap_rprintf(r, "</gml:lowerCorner>\n");
                ap_rprintf(r, "      <gml:upperCorner>%f %f %ld", upperCorner[0], upperCorner[1], t_max); if (( high_max != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA ))   ap_rprintf(r, " %f", high_max); ap_rprintf(r, "</gml:upperCorner>\n");
                ap_rprintf(r, "    </gml:Envelope>\n");
		ap_rprintf(r, "  </gml:boundedBy>\n");
		ap_rprintf(r, "    <wcs:CoverageId>%s</wcs:CoverageId>\n", prod);
		ap_rprintf(r, "    <gml:domainSet>\n");
		ap_rprintf(r, "      <gml:RectifiedGrid gml:id=\"grid_%s\" dimension=\"3\">\n", prod);
		ap_rprintf(r, "        <gml:limits>\n");
		ap_rprintf(r, "          <gml:GridEnvelope>\n");
		ap_rprintf(r, "            <gml:low>0 0 0</gml:low>\n");
		ap_rprintf(r, "            <gml:high>%d %d %d</gml:high>\n",  pxAOISizeX - 1, pxAOISizeY - 1, hit_number - 1 );
		ap_rprintf(r, "          </gml:GridEnvelope>\n");
		ap_rprintf(r, "        </gml:limits>\n");
		ap_rprintf(r, "        <gml:axisLabels>y x</gml:axisLabels>\n");
		ap_rprintf(r, "        <gml:origin>\n");
		ap_rprintf(r, "          <gml:Point gml:id=\"grid_origin_%s\" srsName=\"http://www.opengis.net/def/crs/EPSG/0/%d\">\n", prod, proj);
		ap_rprintf(r, "            <gml:pos>%f %f</gml:pos>\n", upperCorner[0], lowerCorner[1] );
		ap_rprintf(r, "          </gml:Point>\n");
		ap_rprintf(r, "        </gml:origin>\n");
		ap_rprintf(r, "          <gml:offsetVector srsName=\"EPSG:%d\" dimension=\"2\">%f 0</gml:offsetVector>\n", proj, y_min_res);
		ap_rprintf(r, "          <gml:offsetVector srsName=\"EPSG:%d\" dimension=\"2\">0 %f</gml:offsetVector>\n", proj, x_min_res);
		ap_rprintf(r, "      </gml:RectifiedGrid>\n");
		ap_rprintf(r, "    </gml:domainSet>\n");
		ap_rprintf(r, "    <gmlcov:rangeType>\n");
		ap_rprintf(r, "      <swe:DataRecord>\n");
		for ( i = 0; i < nband; i++){
                ap_rprintf(r, "       <swe:field name=\"band_%d\">\n", i + 1);
                ap_rprintf(r, "        <swe:Quantity>\n");
		ap_rprintf(r, "          <swe:description>Band %d</swe:description>\n", i + 1);
                ap_rprintf(r, "          <swe:uom code=\"unknown\"/>\n");
                ap_rprintf(r, "          <swe:constraint>\n");
                ap_rprintf(r, "            <swe:AllowedValues>\n");
                ap_rprintf(r, "              <swe:interval>-3.4028234E+38 3.4028234E+38</swe:interval>\n");
                ap_rprintf(r, "            </swe:AllowedValues>\n");
                ap_rprintf(r, "          </swe:constraint>\n");
                ap_rprintf(r, "        </swe:Quantity>\n");
                ap_rprintf(r, "      </swe:field>\n");
		}
		ap_rprintf(r, "      </swe:DataRecord>\n");
		ap_rprintf(r, "    </gmlcov:rangeType>\n");
		ap_rprintf(r, "    <wcs:ServiceParameters>\n");
		ap_rprintf(r, "      <wcs:CoverageSubtype>RectifiedGridCoverage</wcs:CoverageSubtype>\n");
		ap_rprintf(r, "      <wcs:nativeFormat>image/tiff</wcs:nativeFormat>\n");
		ap_rprintf(r, "    </wcs:ServiceParameters>\n");
		ap_rprintf(r, "  </wcs:CoverageDescription>\n");
		ap_rprintf(r, "</wcs:CoverageDescriptions>\n");
		goto EO_DescribeCoverage;
		}




		ap_rprintf(r, "<wcs:CoverageDescriptions\n");
		ap_rprintf(r, "	xsi:schemaLocation='http://www.opengis.net/wcs/2.0 http://schemas.opengis.net/wcs/2.0/wcsAll.xsd'\n");
		ap_rprintf(r, "	xmlns:wcs='http://www.opengis.net/wcs/2.0' \n");
		ap_rprintf(r, "	xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'\n");
		ap_rprintf(r, "	xmlns:crs='http://www.opengis.net/wcs/service-extension/crs/1.0'\n");
		ap_rprintf(r, "	xmlns:ows='http://www.opengis.net/ows/2.0' xmlns:gml='http://www.opengis.net/gml/3.2' \n");
		ap_rprintf(r, "	xmlns:xlink='http://www.w3.org/1999/xlink'>\n");
                
		ap_rprintf(r, " <wcs:CoverageDescription\n");
		ap_rprintf(r, "	 gml:id='%s'\n", prod);
		ap_rprintf(r, "	 xmlns='http://www.opengis.net/gml/3.2'\n");
		ap_rprintf(r, "	 xmlns:gmlcov='http://www.opengis.net/gmlcov/1.0'\n");
		ap_rprintf(r, "	 xmlns:swe='http://www.opengis.net/swe/2.0'>\n");
		ap_rprintf(r, "  <boundedBy>\n");

		if (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA )) 
                ap_rprintf(r, "    <Envelope srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s;\" axisLabels=\"%s %s %s %s\" uomLabels=\"%s %s %s %s\" srsDimension=\"4\">\n",
                                proj, time_URL[time_index], dimenstion_Label[i][0], dimenstion_Label[i][1], time_Label[time_index], dimenstion_Label[2][1], dimenstion_unit[i], dimenstion_unit[i], time_unit[time_index], dimenstion_unit[2] );
		else
                ap_rprintf(r, "    <Envelope srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s;\" axisLabels=\"%s %s %s\" uomLabels=\"%s %s %s\" srsDimension=\"3\">\n",
                                proj, time_URL[time_index], dimenstion_Label[i][0], dimenstion_Label[i][1], time_Label[time_index], dimenstion_unit[i], dimenstion_unit[i], time_unit[time_index] );

                ap_rprintf(r, "      <lowerCorner>%f %f %ld", lowerCorner[0], lowerCorner[1], t_min); if (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA ))   ap_rprintf(r, " %f", high_min); ap_rprintf(r, "</lowerCorner>\n");
                ap_rprintf(r, "      <upperCorner>%f %f %ld", upperCorner[0], upperCorner[1], t_max); if (( high_max != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA ))   ap_rprintf(r, " %f", high_max); ap_rprintf(r, "</upperCorner>\n");
		ap_rprintf(r, "    </Envelope>\n");
		ap_rprintf(r, "  </boundedBy>\n\n");
		ap_rprintf(r, "  <domainSet>\n");
		ap_rprintf(r, "    <gmlrgrid:ReferenceableGridByVectors dimension=\"%d\" gml:id=\"%s-grid\"\n", (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA )) ? 4 : 3, prod);
		ap_rprintf(r, "		xmlns:gmlrgrid=\"http://www.opengis.net/gml/3.3/rgrid\"\n");
		ap_rprintf(r, "		xsi:schemaLocation=\"http://www.opengis.net/gml/3.3/rgrid http://schemas.opengis.net/gml/3.3/referenceableGrid.xsd\">\n");
		ap_rprintf(r, "      <limits>\n");
		ap_rprintf(r, "         <GridEnvelope>\n");
		ap_rprintf(r, "           <low>0 0 0"); 							if (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA ))   ap_rprintf(r, " %f", high_min);	ap_rprintf(r, "</low>\n");
		ap_rprintf(r, "           <high>%d %d %d", pxAOISizeX - 1, pxAOISizeY - 1, hit_number - 1); 	if (( high_max != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA ))   ap_rprintf(r, " %f", high_max);	ap_rprintf(r, "</high>\n");
		ap_rprintf(r, "         </GridEnvelope>\n");
		ap_rprintf(r, "      </limits>\n");
		if (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA ))
		ap_rprintf(r, "      <axisLabels>%s %s %s %s</axisLabels>\n", dimenstion_Label[i][1], dimenstion_Label[i][0], time_Label[time_index], dimenstion_Label[2][0]);
		else
		ap_rprintf(r, "      <axisLabels>%s %s %s</axisLabels>\n", dimenstion_Label[i][1], dimenstion_Label[i][0], time_Label[time_index]);
		ap_rprintf(r, "      <gmlrgrid:origin>\n");

		if (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA )) {
		ap_rprintf(r, "        <Point gml:id=\"%s-origin\" srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s %s\" uomLabels=\"%s %s %s %s\" srsDimension=\"4\">\n",
				prod, proj, time_URL[time_index], dimenstion_Label[i][0], dimenstion_Label[i][1],  time_Label[time_index],  dimenstion_Label[2][0], dimenstion_unit[i], dimenstion_unit[i], time_unit[time_index], dimenstion_unit[2]);
		ap_rprintf(r, "          <pos>%f %f %ld %f</pos>\n", upperCorner[0] - (y_min_res/2), lowerCorner[1] - (x_min_res/2), t_min, high_min);
		} else {
		ap_rprintf(r, "        <Point gml:id=\"%s-origin\" srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s\" uomLabels=\"%s %s %s\" srsDimension=\"3\">\n",
				prod, proj, time_URL[time_index], dimenstion_Label[i][0], dimenstion_Label[i][1], time_Label[time_index], dimenstion_unit[i], dimenstion_unit[i], time_unit[time_index]);
		ap_rprintf(r, "          <pos>%f %f %ld</pos>\n", upperCorner[0] - (y_min_res/2), lowerCorner[1] - (x_min_res/2), t_min); }

		ap_rprintf(r, "        </Point>\n");

		if (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA )) {
		ap_rprintf(r, "      </gmlrgrid:origin>\n");
		ap_rprintf(r, "      <gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "        <gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "          <gmlrgrid:offsetVector srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s %s\" uomLabels=\"%s %s %s %s\" srsDimension=\"4\">0 %f 0 0</gmlrgrid:offsetVector>\n", 
				proj, time_URL[time_index], dimenstion_Label[i][0], dimenstion_Label[i][1], time_Label[time_index], dimenstion_Label[2][0], dimenstion_unit[i], dimenstion_unit[i], time_unit[time_index], dimenstion_unit[2], x_min_res);
		ap_rprintf(r, "          <gmlrgrid:coefficients />\n");
		ap_rprintf(r, "          <gmlrgrid:gridAxesSpanned>%s</gmlrgrid:gridAxesSpanned>\n", dimenstion_Label[i][1]);
		ap_rprintf(r, "          <gmlrgrid:sequenceRule axisOrder=\"+1\">Linear</gmlrgrid:sequenceRule>\n");
		ap_rprintf(r, "        </gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "      </gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "      <gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "        <gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "          <gmlrgrid:offsetVector srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s %s\" uomLabels=\"%s %s %s %s\" srsDimension=\"4\">%f 0 0 0</gmlrgrid:offsetVector>\n", 
				proj, time_URL[time_index], dimenstion_Label[i][0], dimenstion_Label[i][1], time_Label[time_index], dimenstion_Label[2][0], dimenstion_unit[i], dimenstion_unit[i], time_unit[time_index], dimenstion_unit[2], y_min_res);
		ap_rprintf(r, "          <gmlrgrid:coefficients />\n");
		ap_rprintf(r, "          <gmlrgrid:gridAxesSpanned>%s</gmlrgrid:gridAxesSpanned>\n", dimenstion_Label[i][0]);
		ap_rprintf(r, "          <gmlrgrid:sequenceRule axisOrder=\"+1\">Linear</gmlrgrid:sequenceRule>\n");
		ap_rprintf(r, "        </gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "      </gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "      <gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "        <gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "          <gmlrgrid:offsetVector srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s %s\" uomLabels=\"%s %s %s %s\" srsDimension=\"4\">0 0 1 0</gmlrgrid:offsetVector>\n",
				proj, time_URL[time_index], dimenstion_Label[i][0], dimenstion_Label[i][1], time_Label[time_index], dimenstion_Label[2][0], dimenstion_unit[i], dimenstion_unit[i], time_unit[time_index], dimenstion_unit[2]);
		ap_rprintf(r, "          <gmlrgrid:coefficients>\n");
		ap_rprintf(r, "          </gmlrgrid:coefficients>\n");
		ap_rprintf(r, "          <gmlrgrid:gridAxesSpanned>t</gmlrgrid:gridAxesSpanned>\n");
		ap_rprintf(r, "        <gmlrgrid:sequenceRule axisOrder=\"+1\">Linear</gmlrgrid:sequenceRule>\n");
		ap_rprintf(r, "      </gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "    </gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "      <gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "        <gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "          <gmlrgrid:offsetVector srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s %s\" uomLabels=\"%s %s %s %s\" srsDimension=\"4\">0 0 0 1</gmlrgrid:offsetVector>\n",
				proj, time_URL[time_index], dimenstion_Label[i][0], dimenstion_Label[i][1], time_Label[time_index], dimenstion_Label[2][0], dimenstion_unit[i], dimenstion_unit[i], time_unit[time_index], dimenstion_unit[2]);
		ap_rprintf(r, "          <gmlrgrid:coefficients>\n");
		ap_rprintf(r, "          </gmlrgrid:coefficients>\n");
		ap_rprintf(r, "          <gmlrgrid:gridAxesSpanned>t</gmlrgrid:gridAxesSpanned>\n");
		ap_rprintf(r, "        <gmlrgrid:sequenceRule axisOrder=\"+1\">Linear</gmlrgrid:sequenceRule>\n");
		ap_rprintf(r, "      </gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "    </gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "  </gmlrgrid:ReferenceableGridByVectors>\n");
		ap_rprintf(r, "</domainSet>\n");
		} else {
		ap_rprintf(r, "      </gmlrgrid:origin>\n");
		ap_rprintf(r, "      <gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "        <gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "          <gmlrgrid:offsetVector srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s\" uomLabels=\"%s %s %s\" srsDimension=\"3\">0 %f 0</gmlrgrid:offsetVector>\n", 
				proj, time_URL[time_index], dimenstion_Label[i][0], dimenstion_Label[i][1], time_Label[time_index], dimenstion_unit[i], dimenstion_unit[i], time_unit[time_index], x_min_res);
		ap_rprintf(r, "          <gmlrgrid:coefficients />\n");
		ap_rprintf(r, "          <gmlrgrid:gridAxesSpanned>%s</gmlrgrid:gridAxesSpanned>\n", dimenstion_Label[i][1]);
		ap_rprintf(r, "          <gmlrgrid:sequenceRule axisOrder=\"+1\">Linear</gmlrgrid:sequenceRule>\n");
		ap_rprintf(r, "        </gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "      </gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "      <gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "        <gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "          <gmlrgrid:offsetVector srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s\" uomLabels=\"%s %s %s\" srsDimension=\"3\">%f 0 0</gmlrgrid:offsetVector>\n", 
				proj, time_URL[time_index], dimenstion_Label[i][0], dimenstion_Label[i][1], time_Label[time_index], dimenstion_unit[i], dimenstion_unit[i], time_unit[time_index], y_min_res);
		ap_rprintf(r, "          <gmlrgrid:coefficients />\n");
		ap_rprintf(r, "          <gmlrgrid:gridAxesSpanned>%s</gmlrgrid:gridAxesSpanned>\n", dimenstion_Label[i][0]);
		ap_rprintf(r, "          <gmlrgrid:sequenceRule axisOrder=\"+1\">Linear</gmlrgrid:sequenceRule>\n");
		ap_rprintf(r, "        </gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "      </gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "      <gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "        <gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "          <gmlrgrid:offsetVector srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s\" uomLabels=\"%s %s %s\" srsDimension=\"3\">0 0 1</gmlrgrid:offsetVector>\n",
				proj, time_URL[time_index], dimenstion_Label[i][0], dimenstion_Label[i][1], time_Label[time_index], dimenstion_unit[i], dimenstion_unit[i], time_unit[time_index]);
		ap_rprintf(r, "          <gmlrgrid:coefficients>\n");
		ap_rprintf(r, "          </gmlrgrid:coefficients>\n");
		ap_rprintf(r, "          <gmlrgrid:gridAxesSpanned>t</gmlrgrid:gridAxesSpanned>\n");
		ap_rprintf(r, "        <gmlrgrid:sequenceRule axisOrder=\"+1\">Linear</gmlrgrid:sequenceRule>\n");
		ap_rprintf(r, "      </gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "    </gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "  </gmlrgrid:ReferenceableGridByVectors>\n");
		ap_rprintf(r, "</domainSet>\n"); }



		ap_rprintf(r, "<gmlcov:rangeType>\n");
		ap_rprintf(r, "  <swe:DataRecord>\n");


		for ( i = 0; i < nband; i++){
		ap_rprintf(r, "    <swe:field name=\"band%d\">\n", i + 1);
		ap_rprintf(r, "        <swe:Quantity definition=\"%s\">\n", OGC_dataTypes[type]);
		ap_rprintf(r, "          <swe:label>%s</swe:label>\n", basename(OGC_dataTypes[type]));
		ap_rprintf(r, "          <swe:description>primitive</swe:description>\n");
		ap_rprintf(r, "          <swe:uom code=\"10^0\" />\n");
		ap_rprintf(r, "          <swe:constraint>\n");
		ap_rprintf(r, "            <swe:AllowedValues>\n");
		ap_rprintf(r, "              <swe:interval>-3.4028234E+38 3.4028234E+38</swe:interval>\n");
		ap_rprintf(r, "            </swe:AllowedValues>\n");
		ap_rprintf(r, "          </swe:constraint>\n");
		ap_rprintf(r, "        </swe:Quantity>\n");
		ap_rprintf(r, "      </swe:field>\n");
		}



		ap_rprintf(r, "    </swe:DataRecord>\n");
		ap_rprintf(r, "  </gmlcov:rangeType>\n");
		ap_rprintf(r, "  <wcs:ServiceParameters>\n");
		ap_rprintf(r, "  <wcs:CoverageSubtype>RectifiedGridCoverage</wcs:CoverageSubtype>\n");
		ap_rprintf(r, "    <CoverageSubtypeParent xmlns=\"http://www.opengis.net/wcs/2.0\">\n");
		ap_rprintf(r, "      <CoverageSubtype>AbstractDiscreteCoverage</CoverageSubtype>\n");
		ap_rprintf(r, "        <CoverageSubtypeParent>\n");
		ap_rprintf(r, "          <CoverageSubtype>AbstractCoverage</CoverageSubtype>\n");
		ap_rprintf(r, "        </CoverageSubtypeParent>\n");
		ap_rprintf(r, "      </CoverageSubtypeParent>\n");
		ap_rprintf(r, "      <wcs:nativeFormat>application/octet-stream</wcs:nativeFormat>\n");
		ap_rprintf(r, "    </wcs:ServiceParameters>\n");
		ap_rprintf(r, "  </wcs:CoverageDescription>\n");

		ap_rprintf(r, "</wcs:CoverageDescriptions>\n");


		//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
		EO_DescribeCoverage: ;

		clock_gettime(CLOCK_REALTIME, &time_after);
		time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;


		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "Finish in DescribeCoverage in %.3f sec, Total Time %.3f sec", 
				(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
				
		bzero(log_tmp_str, MAX_STR_LEN - 1); sprintf(log_tmp_str, "%f", time_total); addStatToPush(info, "exec_time",  log_tmp_str, GDT_Float32 ); // STATS
		return 0;
	}

	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	// try to output some server info
	//
	
	if ( Request == Status ) {
		clock_gettime(CLOCK_REALTIME, &time_before); ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Enter in Server Status Information");

		struct sysinfo myinfo; sysinfo(&myinfo);
		unsigned long 	wcs_memory_info = 0;
		int	 	wcs_task_info   = 0;
		int 		mem_info 	= 0;
		int 		task_info 	= 0;
		int		loadavg		= 0;
		float		cpuload		= 0.0;
		char		FileBuffer[255];

		mem_info = shm_open(SHMOBJ_PATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR); 
		i 	 = ftruncate(mem_info, sizeof(unsigned long) ); 
		memcpy( &wcs_memory_info,  mmap(NULL, sizeof(unsigned long), PROT_READ|PROT_WRITE, MAP_SHARED, mem_info, 0), sizeof(unsigned long) );
		close(mem_info);

		task_info = shm_open(SHTOBJ_PATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
		i	  = ftruncate(task_info, sizeof(int) );
		memcpy( &wcs_task_info,  mmap(NULL, sizeof(int), PROT_READ|PROT_WRITE, MAP_SHARED, task_info, 0), sizeof(int) ); 
		close(task_info);

		loadavg =  open("/proc/loadavg", O_RDONLY);
		if ( loadavg > 0 ){ i = read(loadavg, FileBuffer, 254); if ( i >  0 ) sscanf(FileBuffer, "%f", &cpuload); close(loadavg); }



		ap_rprintf(r, "{\n");
		#ifdef MWCS_VERSION
		ap_rprintf(r, "  \"version\": \"%s\",\n", MWCS_VERSION);
		#endif
		ap_rprintf(r, "  \"url\": \"%s\",\n", ExtHostname );
		ap_rprintf(r, "  \"memory\": {\n");
		ap_rprintf(r, "    \"phy_total\": \"%ld\",\n", 	(unsigned long)myinfo.mem_unit *  myinfo.totalram );
		ap_rprintf(r, "    \"phy_free\": \"%ld\",\n",   (unsigned long)myinfo.mem_unit *  myinfo.freeram );
		ap_rprintf(r, "    \"vir_total\": \"%ld\",\n", 	L_MaxMemoryUse );
		ap_rprintf(r, "    \"vir_free\": \"%ld\"\n", 	wcs_memory_info );
		ap_rprintf(r, "  },\n");
		ap_rprintf(r, "  \"cpus\": {\n");
		ap_rprintf(r, "    \"configured\": \"%d\",\n", 	get_nprocs_conf());
		ap_rprintf(r, "    \"available\": \"%d\",\n", 	get_nprocs());
		ap_rprintf(r, "    \"load\": \"%f\"\n", 	cpuload);
		ap_rprintf(r, "  },\n");
		ap_rprintf(r, "  \"tasks\": {\n");
		ap_rprintf(r, "    \"configured\": \"%d\",\n", 	L_MaxTasks);
		ap_rprintf(r, "    \"available\": \"%d\"\n", 	wcs_task_info - 1 );
		ap_rprintf(r, "  }\n");
		ap_rprintf(r, "}\n");



		clock_gettime(CLOCK_REALTIME, &time_after);
		time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;


		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "Finish in Status in %.3f sec, Total Time %.3f sec", 
				(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);

		bzero(log_tmp_str, MAX_STR_LEN - 1); sprintf(log_tmp_str, "%f", time_total); addStatToPush(info, "exec_time",  log_tmp_str, GDT_Float32 ); // STATS
		return 0;
	}
	
	
	
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------






	if ( ( Request != GetCoverage ) && (  Request != GetList ) && ( Request != GetInfo ) && ( Request != GetFile ) ) return 500;
	if ( prod_array[0] == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: No CoverageId specified ..."); GoodbyeMessage(info, "No CoverageId specified"); return 400; }

  					     ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Query with range time from %s (%ld) to %s (%ld) ...", datatimeStringFromUNIX(&s_time), t_start, datatimeStringFromUNIX(&e_time), t_finish);
	if ( X_RANGE_DEFINED == FALSE )    { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: No X coordinates Range Defined ...");						}
	else				   { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: X coordinates Range Defined (%f,%f) ...",  x_input_ul,  x_input_lr );      	}
	if ( Y_RANGE_DEFINED == FALSE )    { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: No Y coordinates Range Defined ...");						}
	else				   { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Y coordinates Range Defined (%f,%f) ...",  y_input_ul,  y_input_lr );      	}
	if ( H_RANGE_DEFINED == FALSE )    { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: No H coordinates Range Defined ...");                                      	}
	else				   { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: H coordinates Range Defined (%f,%f) ...",  h_input_low, h_input_high);		}
	if ( input_name   != NULL )	   { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Filter for file name: '%s' ...", 	   input_name);          		}
	if ( info->module != NULL )	   { 	if (  use_module_by_name(info->module,  NULL) != NULL ) ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Force to use reading module: '%s' ...",    info->module);          		
						else	{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Module '%s' not exists!", info->module); GoodbyeMessage(info, "Module '%s' not exists", info->module); return 400;  } }


	if ( colortable   == NULL ) { i = strlen(DefaultColorTable); colortable = (char *)malloc(i + 1); strcpy(colortable, DefaultColorTable); colortable[i] = '\0'; }
	if ( ( colortable != NULL ) && ( MERGING_IMAGE == AVERAGE ) ) 
	{ i = strlen(colortable) - 6; if ( i < 0 ) i = 0; if ( ! strcmp(colortable + i, ".class" ) ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Class colortable, manage data in these terms ..." );  MERGING_IMAGE = OVERLAP; } }

	ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Merging image type %d ...", MERGING_IMAGE ); 

	// Time limit range
	if ( ( L_MaxTimeRange 	> 0 ) && ( ( t_finish   - t_start    ) >= L_MaxTimeRange ) ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "WARN: Time range requeste is too big (%ld >= %ld)", ( t_finish - t_start ),      L_MaxTimeRange ); 
		GoodbyeMessage(info, "Time range requeste is too big (%ld >= %ld)", ( t_finish - t_start ),      L_MaxTimeRange); return 413; }

	clock_gettime(CLOCK_REALTIME, &time_before); ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Enter in GetCoverage");

	// Are you fucking kidding me?!? You defined range on all world?!? So sed ... I remove your definition
	if ( ( X_RANGE_DEFINED == TRUE ) && ( X_INPUT_COORD_IS == LATLON ) && ( x_input_ul == -180.0 ) && ( x_input_lr == 180.0 ) ) X_RANGE_DEFINED = FALSE;
	if ( ( Y_RANGE_DEFINED == TRUE ) && ( Y_INPUT_COORD_IS == LATLON ) && ( y_input_ul ==   90.0 ) && ( y_input_lr == -90.0 ) ) Y_RANGE_DEFINED = FALSE;


	
	
	//-------------------------------


	for (i_prod = 0; prod_path_array[i_prod] != NULL; i_prod++) {
		strcpy( prod, 		prod_array[i_prod] 	);
		strcpy( prod_path, 	prod_path_array[i_prod] );

		PROD_FOUND 	= FALSE;
		dp		= NULL;
		dp 		= (*openSource)(prod_path);	

		if ( dp == NULL ) { 
			dp = (*openSource)(ROOT);
			if ( dp == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Product ROOT not exists %s", ROOT); return 500; }

			while ( (ep = (*listSource)(dp)) ){
		        	if ( ep->d_name[0] == '.' ) continue; 
	
				if ( strncmp(prod, ep->d_name, strlen(ep->d_name)) ) continue;

				bzero(prod_path,  		MAX_STR_LEN - 1); sprintf(prod_path, "%s/%s", ROOT, ep->d_name);
				bzero(prod_path_array[i_prod],  MAX_STR_LEN - 1); strcpy(prod_path_array[i_prod], prod_path);

				if ( generateProdInfo(prod_path, killable, r, token_from_user ) ) continue;

				bzero( epsg_res_ref, 255);
				strcpy(epsg_res_ref, prod + strlen(ep->d_name) + 1 );
				DC = NULL;

				//-------------------------------------------------------------------------------
				// JSON NEW PART
				sprintf(imgs_path, "%s/DescribeCoverage.json", prod_path );
				DC = (*openSourceFile)( imgs_path, "r" );
				if ( DC == NULL ) continue; 
			
				ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: GetCoverage/List Using JSON DescribeCoverage for %s ...", ep->d_name );	
	
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
							if 	( ! strcmp(key, "id" 		) ) strcpy(epsg_res_tmp, json_object_get_string(val) );
							else if ( ! strcmp(key, "EPSG" 		) ) proj 		= json_object_get_int(val);
							else if ( ! strcmp(key, "GeoY_lr" 	) ) lowerCorner[0] 	= json_object_get_double(val);
							else if ( ! strcmp(key, "GeoX_ul" 	) ) lowerCorner[1] 	= json_object_get_double(val);
							else if ( ! strcmp(key, "GeoY_ul" 	) ) upperCorner[0] 	= json_object_get_double(val);
							else if ( ! strcmp(key, "GeoX_lr" 	) ) upperCorner[1] 	= json_object_get_double(val);
							else if ( ! strcmp(key, "x_res" 	) ) x_min_res	 	= json_object_get_double(val);
							else if ( ! strcmp(key, "y_res" 	) ) y_min_res	 	= json_object_get_double(val);
							else if ( ! strcmp(key, "pxAOISizeX" 	) ) pxAOISizeX	 	= json_object_get_int(val);
							else if ( ! strcmp(key, "pxAOISizeY" 	) ) pxAOISizeY	 	= json_object_get_int(val);
							else if ( ! strcmp(key, "nband" 	) ) nband	 	= json_object_get_int(val);
							else if ( ! strcmp(key, "t_min" 	) ) t_min	 	= json_object_get_int(val);
							else if ( ! strcmp(key, "t_max" 	) ) t_max	 	= json_object_get_int(val);
							else if ( ! strcmp(key, "high_min" 	) ) high_min	 	= json_object_get_double(val);
							else if ( ! strcmp(key, "high_max" 	) ) high_max	 	= json_object_get_double(val);
							else if ( ! strcmp(key, "type" 		) ) type	 	= json_object_get_int(val);
							else if ( ! strcmp(key, "hit_number" 	) ) hit_number	 	= json_object_get_int(val);
							else continue; 
						}
						if ( ! strcmp(epsg_res_ref, epsg_res_tmp) ) { PROD_FOUND = TRUE; break; }
						bzero(epsg_res_tmp, 255);
					}
				}
				if ( PROD_FOUND == TRUE ) break;

			}
			(*closeSource)(dp);

			dp = (*openSource)(prod_path);
			if ( ( dp == NULL ) || ( PROD_FOUND == FALSE ) ) { printNoCoverageFound(r, prod); ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Product %s not exists!", prod ); 
				GoodbyeMessage(info, "Product %s not exists!", prod ); return 404; };

			epgs_in  = proj;
			epgs_out = proj;


			if ( proj != 4326 ) 	label_index = 1;
			else			label_index = 0;

			// If you search using coord labels Lat Long 
			if ( ( X_INPUT_COORD_IS == LATLON ) && ( Y_INPUT_COORD_IS == LATLON ) && ( X_RANGE_DEFINED == TRUE ) && ( Y_RANGE_DEFINED == TRUE ) && ( proj != 4326 ) ) epgs_in = 4326; 

		}

		if ( dp == NULL ) { printNoCoverageFound(r, prod); ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Product %s not exists!", prod ); GoodbyeMessage(info, "Product %s not exists!", prod ); return 404; }
		(*closeSource)(dp);
	}

	if ((( X_INPUT_COORD_IS == LATLON ) || ( Y_INPUT_COORD_IS == LATLON )) && ( epgs_in == DEFAULT_NODATA ) ) epgs_in = 4326;
	if ((( X_INPUT_COORD_IS == UTM    ) || ( Y_INPUT_COORD_IS == UTM    )) && ( input_proj  != NULL ) ) {
		OGRSpatialReferenceH hSRS = OSRNewSpatialReference(NULL);
		if ( OSRSetFromUserInput( hSRS, input_proj ) != OGRERR_NONE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Translating input user projection failed %s", input_proj ); return 400; }
		if ( OSRGetAttrValue (hSRS, "AUTHORITY", 1)  == NULL        ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Translating input user projection failed %s", input_proj ); return 400; }
		epgs_in = atoi(OSRGetAttrValue (hSRS, "AUTHORITY", 1));
		OSRDestroySpatialReference(hSRS);
	}

	// if not defined in input i fill the coordiantes with domain borders	
	if (( PROD_FOUND == TRUE ) && ( X_RANGE_DEFINED	== FALSE )) { x_input_ul = lowerCorner[1]; x_input_lr = upperCorner[1]; }
	if (( PROD_FOUND == TRUE ) && ( Y_RANGE_DEFINED	== FALSE )) { y_input_ul = upperCorner[0]; y_input_lr = lowerCorner[0]; }


	//------------------------------------------------------------------------
	strftime(time_path, 256, "%Y", 		&s_time); s_ref_year   	= atoi(time_path);
	strftime(time_path, 256, "%m", 		&s_time); s_ref_month  	= atoi(time_path);
	strftime(time_path, 256, "%d", 		&s_time); s_ref_day    	= atoi(time_path);
	strftime(time_path, 256, "%H%M%S", 	&s_time); s_ref_time	= atoi(time_path);

	strftime(time_path, 256, "%Y", 		&e_time); e_ref_year   	= atoi(time_path);
	strftime(time_path, 256, "%m", 		&e_time); e_ref_month  	= atoi(time_path);
	strftime(time_path, 256, "%d", 		&e_time); e_ref_day    	= atoi(time_path);
	strftime(time_path, 256, "%H%M%S", 	&e_time); e_ref_time	= atoi(time_path);

	if ( s_ref_year == e_ref_year ) {
		SAME_YEAR = TRUE;
		if ( s_ref_month == e_ref_month ){
			SAME_MONTH = TRUE;
			if ( s_ref_day 	== e_ref_day ){
				SAME_DAY	= TRUE;
				if ( s_ref_time == e_ref_time 	)  SAME_TIME = TRUE;
			}
		}
	}
	if ( SAME_YEAR 	== FALSE ) s_ref_year 	= e_ref_year 	= -1;
	if ( SAME_MONTH == FALSE ) s_ref_month 	= e_ref_month 	= -1;
	if ( SAME_DAY 	== FALSE ) s_ref_day 	= e_ref_day 	= -1;
	if ( SAME_TIME 	== FALSE ) s_ref_time	= e_ref_time	= -1;
	//------------------------------------------------------------------------


	if ( epgs_in != DEFAULT_NODATA ) {
		hSourceSRS = OSRNewSpatialReference(NULL);
		if ( ImportFromEPSG(&hSourceSRS, epgs_in ) != OGRERR_NONE ){ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: OSRImportFromEPSG proj incorrect %d", epgs_in); return 500; }
	} else hSourceSRS = NULL;


	#if GDAL_VERSION >= 304
	if ( hSourceSRS != NULL ) OSRSetAxisMappingStrategy(hSourceSRS, OAMS_TRADITIONAL_GIS_ORDER); // GDAL 3.0 FIX
	#endif

	clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;
		time_search = (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Finish in GetCoverage Searcning using DescribeCoverage in %.3f sec, Total Time %.3f  ...", 
			(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);

	clock_gettime(CLOCK_REALTIME, &time_before);

	//------------------------------------------------------------------------------------------------

        if ( layer_str != NULL ){
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Filtering band %s ...", layer_str); 
                for (i = 0, layer_index = 0; i < strlen(layer_str); i++) if (layer_str[i] ==',') layer_index++;
                layers          = (int *)malloc(sizeof(int) * (layer_index+1) );
                for( tok = strtok(layer_str, ","), i = 0; tok != NULL; tok = strtok(NULL, ","), i++ ) {
                        layers[i]  = ( tok[0] == '*' ) ? INT_MAX : atoi(tok);
                        if ( layers[i] <= 0 ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Layer / Band %d incorrect (must to be >= 1)", layers[i]); GoodbyeMessage(info, "Layer / Band %d incorrect (must to be >= 1)", layers[i]); return 400; }
                }
                layer_index = i;
        }


	//------------------------------------------------------------------------------------------------
	
	if (gmin_str != NULL ){
		for (i = 0, grid_index = 0; i < strlen(gmin_str); i++) if (gmin_str[i] ==',') grid_index++;
		grid_starts     = (double *)malloc(sizeof(double) * (grid_index+1) );
		for( tok = strtok(gmin_str, ","), i = grid_index; tok != NULL; tok = strtok(NULL, ","), i-- ) grid_starts[i] = ( tok[0] == '*' ) ? INT_MIN : atof(tok);
		j = grid_index;

	}

	if (gmax_str != NULL ){
		for (i = 0, grid_index = 0; i < strlen(gmax_str); i++) if (gmax_str[i] ==',') grid_index++;
		grid_stops     = (double *)malloc(sizeof(double) * (grid_index+1) );
		for( tok = strtok(gmax_str, ","), i = grid_index; tok != NULL; tok = strtok(NULL, ","), i-- ) grid_stops[i]  = ( tok[0] == '*' ) ? INT_MAX : atof(tok);
		k = grid_index;
		
	}

	if ( ( grid_starts != NULL ) && ( grid_stops != NULL ) ) grid_index = ( j < k ) ? j : k;



	if ( LC8_PathRow == 2 ){	
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Filtering for LC8 root directory, Path: %03d, Row: %03d ..", LC8_path, LC8_row );

		grid_starts 	= (double *)malloc(sizeof(double) * 2);
		grid_stops  	= (double *)malloc(sizeof(double) * 2);

		// I have to put in revers order
		// Row			Path 
		grid_starts[0]	= LC8_row != DEFAULT_NODATA ? LC8_row : INT_MIN; 	grid_starts[1] 	= LC8_path != DEFAULT_NODATA ? LC8_path : INT_MIN;
		grid_stops[0]	= LC8_row != DEFAULT_NODATA ? LC8_row : INT_MAX;	grid_stops[1] 	= LC8_path != DEFAULT_NODATA ? LC8_path : INT_MAX;
		grid_index	= 1;


	}

	if ( ( SOL_RANGE_DEFINED == TRUE ) || ( SL_RANGE_DEFINED == TRUE ) ) {
		if ( SOL_RANGE_DEFINED == TRUE ) ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Filtering for Mars SOL range from %f to %f ..", 			sol_input_start, 	sol_input_stop );	
		if ( SL_RANGE_DEFINED  == TRUE ) ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Filtering for Mars Solar Longitude Ls range from %f to %f ..", 	sl_input_start, 	sl_input_stop );	

                grid_starts     = (double *)malloc(sizeof(double) * 2);
                grid_stops      = (double *)malloc(sizeof(double) * 2);

		grid_starts[0]	= SL_RANGE_DEFINED == TRUE ? sl_input_start : -DBL_MAX;	grid_starts[1] 	= SOL_RANGE_DEFINED == TRUE ? sol_input_start : -DBL_MAX;
		grid_stops[0]	= SL_RANGE_DEFINED == TRUE ? sl_input_stop  :  DBL_MAX;	grid_stops[1] 	= SOL_RANGE_DEFINED == TRUE ? sol_input_stop  :  DBL_MAX;
		grid_index	= 1;

	}



	//------------------------------------------------------------------------------------------------
	// /opt/mea/dar/mwcs/shapes/mgrs/sentinel_2_index_shapefile.shp
	// sentinel_query="SELECT Name FROM  $( basename "${sentinel_grid_shape/.shp/}" ) WHERE ST_Intersects(ST_GeomFromText('$poly'), $( basename "${sentinel_grid_shape/.shp/}" ).geometry)"
	// ogr2ogr -f "CSV" -sql "SELECT Name FROM sentinel_2_index_shapefile WHERE ST_Intersects(ST_GeomFromText('POLYGON (( -0.624275 39.498636, -0.293064 39.498636, -0.293064 39.305258, -0.624275 39.305258, -0.624275 39.498636 ))'), 
	// sentinel_2_index_shapefile.geometry )"  -dialect SQLITE  -q  -q  /vsistdout/ /opt/mea/dar/mwcs/shapes/mgrs/sentinel_2_index_shapefile.shp
	//
	// SELECT Name FROM sentinel_2_index_shapefile WHERE ST_Intersects(ST_GeomFromText('POINT ( -0.624275 39.498636 )'), 
	// sentinel_2_index_shapefile.geometry )
	//
	//
	// Do you Queen? ? No ?!? 'couse this is a king of magic DUDE!
	//
	if (	( X_RANGE_DEFINED == TRUE ) && ( X_INPUT_COORD_IS == LATLON ) &&
		( Y_RANGE_DEFINED == TRUE ) && ( Y_INPUT_COORD_IS == LATLON ) && ( AUTO_MGRS_TILE == TRUE ) ) {
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: AUTO detecting MGRS tiles ..." );	
		 
		char		*pszDS  	= NULL; 
		char		*pszSQL 	= NULL;
		char		*pszTBL		= NULL;
		char		*mgrs_tok 	= NULL;
		OGRDataSourceH 	poDS;
		OGRLayerH 	hLayer;
		OGRFeatureH 	hFeature;


		pszDS  		= (char *)malloc(sizeof(char) * MAX_STR_LEN); bzero(pszDS,  	MAX_STR_LEN - 1 );	strcpy(pszDS, Sentinel_2_grid_shape);
		pszSQL 		= (char *)malloc(sizeof(char) * MAX_STR_LEN); bzero(pszSQL, 	MAX_STR_LEN - 1 );	
		pszTBL 		= (char *)malloc(sizeof(char) * MAX_STR_LEN); bzero(pszSQL, 	MAX_STR_LEN - 1 );	strcpy(pszTBL, basename(removet_filename_ext(pszDS)) );
		mgrs_tok	= (char *)malloc(sizeof(char) * MAX_STR_LEN); bzero(mgrs_tok, 	MAX_STR_LEN - 1 ); 

		// x_input_ul,     y_input_ul,     x_input_ur,     y_input_ur,     x_input_lr,      y_input_lr,    x_input_ll,      y_input_ll	
		if ( ( x_input_ul == x_input_lr ) && ( y_input_ul == y_input_lr ) ) {
			sprintf(pszSQL,"SELECT Name FROM %s WHERE ST_Intersects(ST_GeomFromText('POINT ( %f %f )'), %s.geometry )", 
				pszTBL,
				x_input_ul,     y_input_ul,
		        	pszTBL );			
		} else {
		       	sprintf(pszSQL,"SELECT Name FROM %s WHERE ST_Intersects(ST_GeomFromText('POLYGON (( %f %f, %f %f, %f %f, %f %f, %f %f ))'), %s.geometry )", 
			pszTBL,
			x_input_ul,     y_input_ul,
			x_input_lr,     y_input_ul,
			x_input_lr,     y_input_lr,
			x_input_ul,     y_input_lr,
			x_input_ul,     y_input_ul,
		        pszTBL );			
		}

		poDS  = OGROpen(pszDS, FALSE, NULL);
		if( poDS == NULL )  { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to open MGRS shapefile %s!", pszDS ); return 500; }
		hLayer = OGR_DS_ExecuteSQL( poDS, pszSQL, NULL, "SQLITE" );

		OGRFeatureDefnH hFDefn = OGR_L_GetLayerDefn(hLayer);
		while( (hFeature = OGR_L_GetNextFeature(hLayer)) != NULL ){
		        int iField;
		        for( iField = 0; iField < OGR_FD_GetFieldCount(hFDefn); iField++ ) {
				OGRFieldDefnH hFieldDefn = OGR_FD_GetFieldDefn( hFDefn, iField );
				if( OGR_Fld_GetType(hFieldDefn) != OFTString ) 	continue;
				for ( i = 0; mgrs_tile_arr[i] != NULL; i++) 	continue;
				if  ( i >= MAX_MGRS_TILES ) 			break;		

				strcpy( mgrs_tok, OGR_F_GetFieldAsString( hFeature, iField) );
				mgrs_tile_arr[i] = (char *)malloc( sizeof(char) * 10); bzero(mgrs_tile_arr[i], 9);	
				sprintf(mgrs_tile_arr[i], "t%c%c/%c/%c/%c", tolower(mgrs_tok[0]), tolower(mgrs_tok[1]), tolower(mgrs_tok[2]), tolower(mgrs_tok[3]), tolower(mgrs_tok[4]) );
				bzero(mgrs_tok,   MAX_STR_LEN - 1 );
			}
			OGR_F_Destroy( hFeature );	
		}
		OGR_DS_Destroy(poDS);
		free(mgrs_tok);
	}

	for (i = 0; mgrs_tile_arr[i] != NULL && i < MAX_MGRS_TILES ; i++) if ( mgrs_tile_arr[i] != NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Filtering for MGRS tiling root directory \"%s\" ...", mgrs_tile_arr[i] ); fflush(stderr); }
	for (i = 1; mgrs_tile_arr[i] != NULL && i < MAX_MGRS_TILES ; i++){
		i_prod = i;
		prod_path_array[i_prod] = (char *)malloc(sizeof(char) * MAX_STR_LEN);
		prod_array[i_prod]      = (char *)malloc(sizeof(char) * MAX_STR_LEN);
		strcpy(prod_path_array[i_prod], prod_path_array[0]);
		strcpy(prod_array[i_prod], 	prod_array[0]);
	} 


	//------------------------------------------------------------------------------------------------
	//
	// SELECT PATH, ROW, Geometry FROM $( basename "${sentinel_grid_shape/.shp/}" ) WHERE ST_Intersects(ST_GeomFromText('POLYGON( $poly_wtk )'), $( basename "${sentinel_grid_shape/.shp/}" ).geometry)
	//

	if (	( X_RANGE_DEFINED == TRUE ) && ( X_INPUT_COORD_IS == LATLON ) &&
		( Y_RANGE_DEFINED == TRUE ) && ( Y_INPUT_COORD_IS == LATLON ) && ( AUTO_PATHROW_TILE == TRUE ) ) {
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: AUTO detecting Landsat PATH/ROW tiles ..." );	
		 
		char		*pszDS  	= NULL; 
		char		*pszSQL 	= NULL;
		char		*pszTBL		= NULL;
		OGRDataSourceH 	poDS;
		OGRLayerH 	hLayer;
		OGRFeatureH 	hFeature;


		pszDS  		= (char *)malloc(sizeof(char) * MAX_STR_LEN); bzero(pszDS,  	MAX_STR_LEN - 1 );	strcpy(pszDS, Landsat_grid_shape);
		pszSQL 		= (char *)malloc(sizeof(char) * MAX_STR_LEN); bzero(pszSQL, 	MAX_STR_LEN - 1 );	
		pszTBL 		= (char *)malloc(sizeof(char) * MAX_STR_LEN); bzero(pszSQL, 	MAX_STR_LEN - 1 );	strcpy(pszTBL, basename(removet_filename_ext(pszDS)) );

		// x_input_ul,     y_input_ul,     x_input_ur,     y_input_ur,     x_input_lr,      y_input_lr,    x_input_ll,      y_input_ll	
		if ( ( x_input_ul == x_input_lr ) && ( y_input_ul == y_input_lr ) ) {
			sprintf(pszSQL,"SELECT PATH, ROW FROM %s WHERE ST_Intersects(ST_GeomFromText('POINT ( %f %f )'), %s.geometry )", 
				pszTBL,
				x_input_ul,     y_input_ul,
		        	pszTBL );			
		} else {
		       	sprintf(pszSQL,"SELECT PATH, ROW FROM %s WHERE ST_Intersects(ST_GeomFromText('POLYGON (( %f %f, %f %f, %f %f, %f %f, %f %f ))'), %s.geometry )", 
			pszTBL,
			x_input_ul,     y_input_ul,
			x_input_lr,     y_input_ul,
			x_input_lr,     y_input_lr,
			x_input_ul,     y_input_lr,
			x_input_ul,     y_input_ul,
		        pszTBL );			
		}

		poDS  = OGROpen(pszDS, FALSE, NULL);
		if( poDS == NULL )  { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to open Landsat shapefile %s!", pszDS ); return 500; }
		hLayer = OGR_DS_ExecuteSQL( poDS, pszSQL, NULL, "SQLITE" );

		grid_starts	= (double *)malloc(sizeof(double) * 2); grid_starts[0] = grid_starts[1] = INT_MAX;
		grid_stops	= (double *)malloc(sizeof(double) * 2); grid_stops[0]  = grid_stops[1]  = INT_MIN;
		grid_index 	= 1;

		OGRFeatureDefnH hFDefn = OGR_L_GetLayerDefn(hLayer);
		while( (hFeature = OGR_L_GetNextFeature(hLayer)) != NULL ){
			OGRFieldDefnH 	hFieldDefn; int path, row;
			if ( OGR_FD_GetFieldCount(hFDefn) != 2 ) continue;

			hFieldDefn = OGR_FD_GetFieldDefn( hFDefn, 0 ); if( OGR_Fld_GetType(hFieldDefn) != OFTInteger ) continue;
			hFieldDefn = OGR_FD_GetFieldDefn( hFDefn, 1 ); if( OGR_Fld_GetType(hFieldDefn) != OFTInteger ) continue;
			path = OGR_F_GetFieldAsInteger(hFeature, 0);
			row  = OGR_F_GetFieldAsInteger(hFeature, 1);

			if ( row  < grid_starts[0] ) grid_starts[0] = row;
			if ( row  > grid_stops[0]  ) grid_stops[0]  = row;
			if ( path < grid_starts[1] ) grid_starts[1] = path;
			if ( path > grid_stops[1]  ) grid_stops[1]  = path;

			OGR_F_Destroy( hFeature );	
		}
		OGR_DS_Destroy(poDS);
	}


	//------------------------------------------------------------------------------------------------

	if ( ( SOL_RANGE_DEFINED != TRUE ) && ( SL_RANGE_DEFINED != TRUE ) ) {
	if ( grid_starts != NULL ) { fprintf(stderr, "INFO: Filtering for GRID directory, lower limit: " ); for ( i = grid_index; i != -1; i--) fprintf(stderr, "%f ", grid_starts[i]); fprintf(stderr, "\n"); fflush(stderr); }
	if ( grid_stops  != NULL ) { fprintf(stderr, "INFO: Filtering for GRID directory, upper limit: " ); for ( i = grid_index; i != -1; i--) fprintf(stderr, "%f ", grid_stops[i] ); fprintf(stderr, "\n"); fflush(stderr); }
	}

	//------------------------------------------------------------------------------------------------

	// gdalinfo  /vsicurl/http://download.osgeo.org/geotiff/samples/usgs/c41078a1.tif
	//
	
	t_max		= 0;
	t_min		= time(NULL); 
	hit_number 	= 0;

	x_input_ur	= x_input_lr;	
	y_input_ur	= y_input_ul;

	x_input_ll	= x_input_ul;
	y_input_ll	= y_input_lr;

	for (i_prod = 0, i = 0; prod_path_array[i_prod] != NULL; i_prod++) {
	// depth = -1; // Init Variable for retro compatibility
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Searching in collection %s on %s ...", prod_array[i_prod], ROOT); // prod_path_array[i_prod]); 

		// Get limits for this collection
		bzero(imgs_path, MAX_PATH_LEN - 1); sprintf(imgs_path, "%s/.limits",   prod_path_array[i_prod] );
		L_MaxTimeRange 	= atol( getValueFromKey(imgs_path, "MaxTimeRange") );
		L_MaxSixeX 	= atof( getValueFromKey(imgs_path, "MaxSixeX") );
		L_MaxSixeY 	= atof( getValueFromKey(imgs_path, "MaxSixeY") );

		if ( ( L_MaxTimeRange   > 0 ) && (( t_finish - t_start ) >= L_MaxTimeRange  )) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "WARN: Time range requeste is too big (%ld >= %ld) ... ", ( t_finish - t_start ), L_MaxTimeRange ); return 413; }
		if ( ( L_MaxSixeX 	> 0 ) && (( x_input_lr - x_input_ul ) >= L_MaxSixeX )) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "WARN: X range requeste is too big (%f >= %f)", ( x_input_lr - x_input_ul ), L_MaxSixeX ); 	 return 413; }
		if ( ( L_MaxSixeY 	> 0 ) && (( y_input_ul - y_input_lr ) >= L_MaxSixeY )) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "WARN: Y range requeste is too big (%f >= %f)", ( y_input_ul - y_input_lr ), L_MaxSixeY ); 	 return 413; }

		
		//----------------------------------------

		if ( tmp == NULL )  tmp = malloc(sizeof(struct sblock)); initBlock(tmp); 
		bzero(tmp->math_id,  MAX_STR_LEN  - 1); strcpy( tmp->math_id, prod_array[i_prod]); 

		for( j = 0; j < 6; j++) tmp->infoflag[j] = FALSE; tmp->high = DEFAULT_NODATA; 

		//----------------------------------------
		FILE *virt = NULL;
		bzero(imgs_path, MAX_PATH_LEN - 1); sprintf(imgs_path, "%s/.virtual", 	prod_path_array[i_prod] ); virt	= fopen ( imgs_path, "r" );

		if ( virt != NULL ){
			int virt_size = 0;
			fseek(virt, 0L, SEEK_END); virt_size = ftell(virt); rewind(virt);

			bzero(imgs_path, MAX_PATH_LEN - 1);
			i = 0; 	if ( virt_size > 0 ) while( fgets(imgs_path, MAX_PATH_LEN, virt) ) { char *pos; if ((pos=strchr(imgs_path, '\n')) != NULL) *pos = '\0';
				if ( imgs_path[0] == '\n' ) continue;
				if ( imgs_path[0] == ' '  ) continue;
				if ( imgs_path[0] == '#'  ) continue;


				if ( VirCol == NULL ) 	{ VirCol = VirCur 	= (struct VirtualContent *)malloc(sizeof(struct VirtualContent)); 				VirCur->next = NULL; }
				else			{ VirCur->next    	= (struct VirtualContent *)malloc(sizeof(struct VirtualContent)); VirCur = VirCur->next; 	VirCur->next = NULL; }

				for (j = 0 ; j < MAX_D; j++) VirCur->collection[j] = NULL;

				struct 		dirent **namelist;
				int 		n = scandir(prod_path_array[i_prod], &namelist, 0, alphasort); 	if (n < 0) continue;

				j = 0; while (n--) {
					if ( namelist[n]->d_name[0] == '.' ) 		  	continue;
					if ( strstr(namelist[n]->d_name, "DescribeCoverage" ) )	continue;
					if ( ! strstr(imgs_path,  namelist[n]->d_name ) ) 	continue;

					// Check if collection is used in other math
					gotcha = FALSE;
					for (struct VirtualContent *VirTmp = VirCol; VirTmp != NULL; VirTmp = VirTmp->next ) for (k = 0; k < MAX_D; k++) { 
						if ( VirTmp->collection[k] == NULL ) break; if ( ! strcmp(VirTmp->collection[k], namelist[n]->d_name ) ) 	{ gotcha = TRUE; break; }
					}
					if ( gotcha == TRUE ) continue;
					VirCur->collection[j] = malloc(strlen(namelist[n]->d_name)+1); strcpy(VirCur->collection[j], namelist[n]->d_name);
					j++;
				}
				VirCur->size	= virt_size;
				VirCur->band 	= i;
				VirCur->num	= 1;
				VirCur->src[0]	= malloc(strlen(imgs_path)+1); strcpy(VirCur->src[0], imgs_path); 
				i++;
			}
			fclose(virt);
			if ( VirCol == NULL ){
				struct	dirent **namelist;
				int 	n = scandir(prod_path_array[i_prod], &namelist, 0, alphasort); 	if (n < 0) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to open `%s'", prod_path_array[i_prod]); return 400; }
				VirCol    = (struct VirtualContent *)malloc(sizeof(struct VirtualContent)); VirCol->next = NULL; VirCol->band = 1;

				VirCol->num = 0; while (n--) {
                                 	if ( namelist[n]->d_name[0] == '.' ) 				continue;
					if ( strstr(namelist[n]->d_name, "DescribeCoverage" ) ) 	continue;
					VirCol->src[VirCol->num]  = malloc(strlen(namelist[n]->d_name)+1); strcpy(VirCol->src[VirCol->num], namelist[n]->d_name);
					VirCol->num++;
					VirCol->size = 0;
				}
			}


			//------------------------
			// for ( VirCur = VirCol; VirCur != NULL; VirCur = VirCur->next) for ( i = 0; i < VirCur->num; i++){ fprintf(stderr, "band: %d - %d:%s\n", VirCur->band, i, VirCur->src[i]); fflush(stderr); }
			//------------------------
		}	

		//----------------------------------------


		if ( VirCol == NULL ) { // This case is a normal collection no virtual ...
			VIRTUAL_COLLECTION = FALSE;
			walkthrough(prod_path_array[i_prod], prod_array[i_prod], subdataset, &head, &cursor, tmp, &t_time,
					t_start,	t_finish,	s_ref_year, 	s_ref_month, 	s_ref_day, 	s_ref_time,									// Time input
					hSourceSRS,
					x_input_ul, 	y_input_ul, 	x_input_ur,     y_input_ur,	x_input_lr, 	 y_input_lr, 	x_input_ll,      y_input_ll, h_input_low, h_input_high,
					x_min_res, 	y_min_res, X_RANGE_DEFINED, Y_RANGE_DEFINED, X_RANGE_DEFINED_PX, Y_RANGE_DEFINED_PX, H_RANGE_DEFINED, PROD_FOUND, VIRTUAL_COLLECTION, 		// Geographics input
					input_name,																			// Specify file name
					grid_starts,	grid_stops,	grid_index,															// Generic grid information
					mgrs_tile_arr[i_prod],																		// MGRS tiling filtering
					&hit_number, r );
		} else { // But in this case is a virutal collection mother fucker! I'm starting to hate virtual collection
			char *v_prod_path_array = (char *)malloc(sizeof(char) * MAX_STR_LEN); 
			VIRTUAL_COLLECTION = TRUE;
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Found Virtual Collection in %s ...", prod_path_array[i_prod] );

			bzero(imgs_path, MAX_PATH_LEN - 1);
			for ( VirCur = VirCol; VirCur != NULL; VirCur = VirCur->next){	if ( VirCur->size <= 0 ) continue;
				strcpy( imgs_path, VirCur->src[0]);
				if ( imgs_path[0] == '\n' ) continue;
				if ( imgs_path[0] == ' '  ) continue;
				if ( imgs_path[0] == '#'  ) continue;
				if ( imgs_path[0] == '='  ) { // wait wait wait ... are you fucking kidding me?!? Math in a virtual collection .... oh em gi
					for (j = 0 ; j < MAX_D; j++) { 	if ( VirCur->collection[j] == NULL ) break;
						bzero(tmp->math_id,  MAX_STR_LEN  - 1); strcpy( tmp->math_id, VirCur->collection[j]);
						snprintf(v_prod_path_array, MAX_STR_LEN, "%s/%s", prod_path_array[i_prod],  VirCur->collection[j]);
					
						walkthrough(v_prod_path_array, prod_array[i_prod], subdataset, &head, &cursor, tmp, &t_time,
								t_start,	t_finish,	s_ref_year, 	s_ref_month, 	s_ref_day, 	s_ref_time,									// Time input
								hSourceSRS,
								x_input_ul, 	y_input_ul, 	x_input_ur,     y_input_ur,	x_input_lr, 	 y_input_lr, 	x_input_ll,  	y_input_ll, h_input_low, h_input_high,
								x_min_res, 	y_min_res, X_RANGE_DEFINED, Y_RANGE_DEFINED, X_RANGE_DEFINED_PX, Y_RANGE_DEFINED_PX, H_RANGE_DEFINED, PROD_FOUND, VIRTUAL_COLLECTION, 		// Geographics input
								input_name,																			// Specify file name
								grid_starts,	grid_stops,	grid_index,															// Generic grid information
								mgrs_tile_arr[i_prod],																		// MGRS tiling filtering
								&hit_number, r );

					}

					continue;

				} else { snprintf(v_prod_path_array, MAX_STR_LEN, "%s/%s", prod_path_array[i_prod], imgs_path); }

				walkthrough(v_prod_path_array, prod_array[i_prod], subdataset, &head, &cursor, tmp, &t_time,
						t_start,	t_finish,	s_ref_year, 	s_ref_month, 	s_ref_day, 	s_ref_time,									// Time input
						hSourceSRS,
						x_input_ul, 	y_input_ul, 	x_input_ur,     y_input_ur,	x_input_lr, 	 y_input_lr, 	x_input_ll,      y_input_ll, h_input_low, h_input_high,
						x_min_res, 	y_min_res, X_RANGE_DEFINED, Y_RANGE_DEFINED, X_RANGE_DEFINED_PX, Y_RANGE_DEFINED_PX, H_RANGE_DEFINED, PROD_FOUND, VIRTUAL_COLLECTION, 		// Geographics input
						input_name,																			// Specify file name
						grid_starts,	grid_stops,	grid_index,															// Generic grid information
						mgrs_tile_arr[i_prod],																		// MGRS tiling filtering
						&hit_number, r );

				bzero(imgs_path, MAX_PATH_LEN - 1);
	
			}

			if ( head == NULL ) { // This case in when .virtual file is empty of without definition ... so le collections are merged in one
				VIRTUAL_COLLECTION = TRUE;
				for ( i = 0; i < VirCol->num; i++){
					sprintf(v_prod_path_array, "%s/%s", prod_path_array[i_prod],  VirCol->src[i]);

					walkthrough(v_prod_path_array, prod_array[i_prod], subdataset, &head, &cursor, tmp, &t_time,
							t_start,	t_finish,	s_ref_year, 	s_ref_month, 	s_ref_day, 	s_ref_time,									// Time input
							hSourceSRS,
							x_input_ul,     y_input_ul,     x_input_ur,     y_input_ur,     x_input_lr,     y_input_lr,    x_input_ll,      y_input_ll, h_input_low, h_input_high,
							x_min_res, 	y_min_res, X_RANGE_DEFINED, Y_RANGE_DEFINED, X_RANGE_DEFINED_PX, Y_RANGE_DEFINED_PX, H_RANGE_DEFINED, PROD_FOUND, VIRTUAL_COLLECTION,		// Geographics input
							input_name,																			// Specify file name
							grid_starts,	grid_stops,	grid_index,															// Generic grid information
							mgrs_tile_arr[i_prod],																		// MGRS tiling filtering
							&hit_number, r );
				}
			
			}
		}


		if ( tmp != NULL ) { free(tmp); tmp = NULL; }
	}	


	// If tile mode and multi band and .... ok you know
	if ( ( WMTS_MODE == TRUE ) && ( head != NULL  ) && ( head->nband > 3 ) ) { layers = (int *)malloc(sizeof(int) * 3); layers[0] = 0; layers[1] = 1; layers[2] = 2; layer_index = 3; }


	//------------------------------------------------------------------------------------------------
	// Checking for exists some Virtual Collection

	for (i_prod = 0; prod_path_array[i_prod] != NULL; i_prod++) {
		char imgs_path_search[MAX_PATH_LEN]; i = k = 0;
		int math_out_band = 0; 
		for ( VirCur = VirCol; VirCur != NULL; VirCur = VirCur->next){	strcpy( imgs_path, VirCur->src[0]);
			if ( imgs_path[0] == '\n' ) continue;
			if ( imgs_path[0] == ' '  ) continue;
			if ( imgs_path[0] == '#'  ) continue;
			if ( imgs_path[0] == '='  ) {
				if ( mathChain == NULL ) 	{ mathChain = mathcur 	= (struct mathUnit *)malloc(sizeof(struct mathUnit)); mathcur->next = mathcur->prev = NULL; }
				else				{ mathcur->next    	= (struct mathUnit *)malloc(sizeof(struct mathUnit)); mathcur->next->prev = mathcur; mathcur = mathcur->next; mathcur->next = NULL; }
	
				mathcur->coveragemath = (char *)malloc(MAX_PATH_LEN * sizeof(char)); bzero(mathcur->coveragemath, MAX_PATH_LEN - 1);
				for(j = 0; j < strlen(imgs_path); j++) if ( ( imgs_path[j] != '=' ) && ( imgs_path[j] != ' ' ) ) break;
				sprintf(mathcur->coveragemath, "%s", &(imgs_path[j]));

				if (!parse(mathcur))		{  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to parsing `%s'", mathcur->coveragemath);  	return 400; }
				math_memory_factor += mathcur->l_stack_max;
				mathcur->band = math_out_band; math_out_band++;


				for (int q = 0; q < mathcur->l_queue; q++){
					if ( mathcur->queue[q].s[0] == '+') continue;
					if ( mathcur->queue[q].s[0] == '-') continue;
					if ( mathcur->queue[q].s[0] == '*') continue;
					if ( mathcur->queue[q].s[0] == '/') continue;
					if ( mathcur->queue[q].s[0] == '^') continue;
					if ( mathcur->queue[q].s[0] == '%') continue;
					if ( mathcur->queue[q].s[0] == '<') continue;
					if ( mathcur->queue[q].s[0] == '>') continue;
					if ( mathcur->queue[q].s[0] == ':') continue;
      			                if ( mathcur->queue[q].s[0] == '&') continue;
			                if ( mathcur->queue[q].s[0] == '|') continue;
       	        		      	if ( ( mathcur->queue[q].s[0] == '>') && ( mathcur->queue[q].s[1] == '>') ) continue;
       	               			if ( ( mathcur->queue[q].s[0] == '<') && ( mathcur->queue[q].s[1] == '<') ) continue;
					if ( ( mathcur->queue[q].s[0] == '!') && ( mathcur->queue[q].s[1] == '=') ) continue;
					if ( ( mathcur->queue[q].s[0] == '=') && ( mathcur->queue[q].s[1] == '=') ) continue;
					if ( ( mathcur->queue[q].s[0] == '&') && ( mathcur->queue[q].s[1] == '&') ) continue;
					if ( ( mathcur->queue[q].s[0] == '|') && ( mathcur->queue[q].s[1] == '|') ) continue;
					if ( ! strncmp(mathcur->queue[q].s, "AND", mathcur->queue[q].len) )	    continue;	
					if ( ! strncmp(mathcur->queue[q].s, "OR",  mathcur->queue[q].len) )	    continue;	
					
					double 	x;
					bzero(buff, 99); snprintf(buff, 100, "%.*s", mathcur->queue[q].len, mathcur->queue[q].s);
					if ( sscanf(buff, "%lf", &x)  > 0 ) continue;


					PROD_FOUND = FALSE; for ( cvorder = hvlist; cvorder != NULL; cvorder = cvorder->next) {	if ( ! strncmp(cvorder->name, mathcur->queue[q].s,  mathcur->queue[q].len) ) { PROD_FOUND = TRUE; break; } } 
					if ( PROD_FOUND == TRUE )  continue;
					
//					ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Using for math product: %.*s", mathcur->queue[q].len, mathcur->queue[q].s);
					bzero(imgs_path, MAX_PATH_LEN - 1); strncpy(imgs_path, mathcur->queue[q].s, mathcur->queue[q].len);
					bzero(imgs_path_search, MAX_PATH_LEN - 1); snprintf(imgs_path_search, MAX_PATH_LEN, "/%s/", imgs_path);
					for( cursor = head; cursor != NULL; cursor = cursor->next) {
						if ( 	( ( cursor->datasetId != NULL ) && ( strstr(imgs_path, cursor->datasetId )) 	) ||					
						   	( ( strstr(cursor->file, imgs_path_search ) ) 	) ){
							if ( hvlist == NULL ) 	{ hvlist = cvlist = (struct virtualList *)malloc(sizeof(struct virtualList)); 				cvlist->next = NULL; }
							else			{ cvlist->next    = (struct virtualList *)malloc(sizeof(struct virtualList)); cvlist = cvlist->next; 	cvlist->next = NULL; }
	
							cvlist->name  		 = (char *)malloc(strlen(imgs_path)+1); 		strcpy(cvlist->name, 		imgs_path);
							cvlist->prod_path_array  = (char *)malloc(strlen(prod_path_array[i_prod])+1); 	strcpy(cvlist->prod_path_array, prod_path_array[i_prod]);
							cvlist->nband 		 = cursor->nband;
							cvlist->tband		 = (int *)malloc(sizeof(int) * cvlist->nband); for (j = 0; j < cvlist->nband; j++) cvlist->tband[j] = j;
							bzero(cursor->math_id,  MAX_STR_LEN  - 1); strcpy( cursor->math_id, imgs_path);
							i++;
							break;
						}
					}
					k++; 
				}

			}			
			if ( mathChain != NULL ) continue; // Ok i dont know ... but, in this pre case if a found math i skip the fowlloing part

			gotcha = FALSE;
			for( cursor = head; cursor != NULL; cursor = cursor->next) { 
				for (int q = 0; q < VirCur->num; q++ ) {
					bzero(imgs_path_search, MAX_PATH_LEN - 1); sprintf(imgs_path_search, "/%s/", 	VirCur->src[q]); 

					

					if ( 	( ( cursor->datasetId 	!= NULL ) && ( ! strcmp(cursor->datasetId, 	imgs_path_search ) ) 	) ||					
					   	(  ( strstr(cursor->file, 		imgs_path_search ) ) 	) ){

						if ( hvlist == NULL ) 	{ hvlist = cvlist = (struct virtualList *)malloc(sizeof(struct virtualList)); 				cvlist->next = NULL; }
						else			{ cvlist->next    = (struct virtualList *)malloc(sizeof(struct virtualList)); cvlist = cvlist->next; 	cvlist->next = NULL; }

						cvlist->name  		 = (char *)malloc(strlen(imgs_path)+1); 		strcpy(cvlist->name, 		imgs_path);
						cvlist->prod_path_array  = (char *)malloc(strlen(prod_path_array[i_prod])+1); 	strcpy(cvlist->prod_path_array, prod_path_array[i_prod]);
						cvlist->nband 		 = cursor->nband;
						cvlist->tband		 = (int *)malloc(sizeof(int) * cvlist->nband); for (j = 0; j < cvlist->nband; j++) cvlist->tband[j] = j;
						i++;
						gotcha = TRUE;
						break;
					}
				}
				if ( gotcha == TRUE) break;

			}
			k++; bzero(imgs_path, MAX_PATH_LEN - 1);
		}

		if ( i != k ) {
			if ( Request != GetList ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "WARN: Not Found enough dataset found to use this Virtual Collection %s ...", 		    	   prod_path_array[i_prod] );  
							GoodbyeMessage(info, "Not Found enough dataset found to use this Virtual Collection %s", prod_path_array[i_prod]); return 404; }
			else			  { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "WARN: Not Found enough dataset found to use this Virtual Collection %s, continue for GetList...", prod_path_array[i_prod] ); 		   }
		}
	}
	// /mnt/mea-disk/mwcs/LC8_naturalSWIR/LC8_B2/194/30/Y2013/M08/D17/T101332/E32632/G296685.000000x4900815.000000_30.000000x-30.000000_7851x7971x1_2/LC81940302013229NSG00_B2.TIF
	//------------------------------------------------------------------------------------------------
	// Filter for selected layer .. I mean ... bands ...
	if ( ( layers != NULL ) && ( layer_index > 0 ) ) { 
		
		if  ( mathChain == NULL ){
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Filtering layers ..." );
			if ( hvlist != NULL ) { // WTF Virtual collection ... dude, it's hard, very hard.

				// target bands set to -1 
				for( cursor = head; cursor != NULL; cursor = cursor->next) { free(cursor->tband); cursor->tband = (int *)malloc(sizeof(int) * MAX_D); for(i = 0; i < MAX_D; i++) cursor->tband[i] = -1; }

				int layer_max = layers[0]; 				for(i = 0; i < layer_index; i++ ) if ( layers[i] > layer_max ) layer_max = layers[i];
				int *done = (int *)malloc(sizeof(int) * layer_max ); 	for(i = 0; i < layer_max;   i++ ) done[i] = FALSE;

				for(i = 0; i < layer_index; i++ ){
					gotcha = FALSE;
					for ( cvlist = hvlist, nband = 0; cvlist != NULL; cvlist = cvlist->next){
						k = layers[i] - 1;
						if ( ( k >= nband ) && ( k <= (cvlist->nband + nband - 1) ) ){
							if ( done[k] == FALSE ) for( cursor = head; cursor != NULL; cursor = cursor->next){
	
								///char imgs_path_search[MAX_PATH_LEN]; bzero(imgs_path_search, MAX_PATH_LEN - 1); sprintf(imgs_path_search, "/%s/", imgs_path);
								// if ( ! strstr(cursor->file, cvlist->name ) ) continue;
								 
								if    ( cursor->datasetId != NULL )	{ if ( strcmp(cvlist->name, cursor->datasetId)   ) continue; }
								else					{ if ( ! strstr(cursor->file, cvlist->name ) 	 ) continue; }

								for(j = 0; j < MAX_D; j++) if ( cursor->tband[j] == -1 ) break;
								cursor->tband[j] = k - nband;	
	
							}
							done[k] = TRUE;
	
							if ( hvorder == NULL ) 	{ hvorder = cvorder = (struct virtualList *)malloc(sizeof(struct virtualList)); 			 cvorder->next = NULL; }
							else			{ cvorder->next     = (struct virtualList *)malloc(sizeof(struct virtualList)); cvorder = cvorder->next; cvorder->next = NULL; }
	
							cvorder->name  	= (char *)malloc(strlen(cvlist->name)+1); 	strcpy(cvorder->name, cvlist->name);
							cvorder->tband 	= (int *)malloc(sizeof(int)); 			cvorder->tband[0] = k - nband;
							cvorder->nband 	= 1;
							cvorder->next  	= NULL;
							gotcha		= TRUE;
						} 
						nband += cvlist->nband;
					}
	
					if ( gotcha == FALSE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: No dataset suitable for band %d ...", layers[i] ); GoodbyeMessage(info, "No dataset suitable for band %d", layers[i]); return 404;}
	
				}
				// Setting nband with number of target bands
				for( cursor = head; cursor != NULL; cursor = cursor->next) for(i = 0; i < MAX_D; i++) if ( cursor->tband[i] == -1 ) { cursor->nband = i; break; }
				
			} else for( cursor = head; cursor != NULL; cursor = cursor->next){
				for(i = 0;i < layer_index; i++) 
					if ( layers[i] > cursor->nband ){ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: %s dataset not suitable for band %d (max %d) ...", basename(cursor->file), layers[i], cursor->nband ); 
						GoodbyeMessage(info,"%s dataset not suitable for band %d (max %d)", basename(cursor->file), layers[i], cursor->nband );  return 404;}
					if ( cursor->tband != NULL ) free(cursor->tband);
				cursor->tband = (int *)malloc( sizeof(int) * layer_index);
				for(i = 0; i < layer_index; i++) cursor->tband[i] = layers[i]-1;
				cursor->nband = layer_index;
			
			}
		} else {
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Filtering layers for math computation ..." );
			// No no no ... first of all i have to check if stupid used ask for a band that not exists ... but wtf
			for(i = 0; i < layer_index; i++ ) {
				gotcha = FALSE; for ( mathcur = mathChain; mathcur != NULL; mathcur = mathcur->next ) if ( ( layers[i] - 1 ) == mathcur->band ) { gotcha = TRUE; break; } if ( gotcha != FALSE ) continue;

				ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: No math not suitable for band %d ...", layers[i] ); GoodbyeMessage(info, "No math not suitable for band %d", layers[i] ); return 404; 
			}

			// Oh shit ... step by step ... first i have to remove the unused math because skiped by band selection
			for ( mathcur = mathChain; mathcur != NULL; mathcur = mathcur->next ) {
				gotcha = FALSE; for(i = 0; i < layer_index; i++ ) if ( ( layers[i] - 1 ) == mathcur->band ) { gotcha = TRUE; break; } if ( gotcha == TRUE ) continue;

				struct mathUnit *mathdel = mathcur;
			//	fprintf(stderr, "to delete %d %s, ",  mathcur->band,  mathcur->coveragemath); fflush(stderr);
			//	if ( mathcur->next != NULL ) fprintf(stderr, "next != null %s, ", mathcur->next->coveragemath); else fprintf(stderr, "next == null, "); fflush(stderr);
			//	if ( mathcur->prev != NULL ) fprintf(stderr, "prev != null %s\n", mathcur->prev->coveragemath); else fprintf(stderr, "prev == null\n"); fflush(stderr);

				if 	( ( mathcur->next != NULL ) && ( mathcur->prev != NULL ) ) 	{ mathcur->prev->next = mathcur->next; mathcur->next->prev = mathcur->prev;	free(mathdel); } // In the middle
				else if ( ( mathcur->next == NULL ) && ( mathcur->prev != NULL ) )	{ mathcur = mathcur->prev; mathcur->next = NULL;				free(mathdel); } // Last of long chain
				else if ( ( mathcur->next != NULL ) && ( mathcur->prev == NULL ) )	{ mathChain = mathcur->next; mathChain->prev = NULL;				free(mathdel); } // First of long chain
			
			}

			// Second i have to remove useless dataset because not involved in math
		        for( cursor = head; cursor != NULL; cursor = cursor->next){
				gotcha = FALSE;	for ( mathcur = mathChain; mathcur != NULL; mathcur = mathcur->next  ) if ( strstr(mathcur->coveragemath, cursor->math_id ) ) { gotcha = TRUE; break; }
				if ( gotcha != FALSE ) continue;
				tmp = cursor;
				if 	( cursor 	== head )	{ head			= cursor->next; if (head != NULL) 	head->prev   		= NULL;		}
				else if ( cursor->next 	== NULL )	{ cursor		= cursor->prev;				cursor->next 		= NULL;		}
				else 					{ cursor->next->prev 	= cursor->prev;				cursor->prev->next 	= cursor->next;	}
				if ( head == NULL ) { hit_number = 0; break; }
				free(tmp); hit_number--;
			}
		}
	}


	
		clock_gettime(CLOCK_REALTIME, &time_after);	
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Finish in GetCoverage Searcning in %.3f sec, Total Time %.3f (%d hits)", 
				(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total, hit_number );
		time_search = (double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;


	clock_gettime(CLOCK_REALTIME, &time_before);
	//------------------------------------------------------------------------------------------------
	// return 0;
	// No images found ... nothing to do.

	if ( hit_number <= 0 ) {  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: No images found ..."); GoodbyeMessage(info, "No images found"); return 404; }

	// Sort list by time
	head = sortList(head);

	// Reparir prev links
	for( tmp = head, cursor = head->next; cursor != NULL; cursor = cursor->next, tmp = tmp->next) cursor->prev = tmp;
	head->prev = NULL;

	// found min and max time
	for( cursor = head; cursor != NULL; cursor = cursor->next ) { 
		if ( t_min > cursor->time ) t_min = cursor->time;
		if ( t_max < cursor->time ) t_max = cursor->time; 

		for ( shp_cursor = cursor->shp_head; shp_cursor != NULL; shp_cursor = shp_cursor->next ){
			if ( t_min > shp_cursor->time ) t_min = shp_cursor->time;
			if ( t_max < shp_cursor->time ) t_max = shp_cursor->time; 
		}
	}

	//------------------------------------------------------------------------------------------------

	if (( epgs_out 	 == DEFAULT_NODATA ) && ( hSourceSRS == NULL ) && ( PROD_FOUND == FALSE ) && ( Y_RANGE_DEFINED == FALSE ) && ( Y_RANGE_DEFINED == FALSE )){
		epgs_out = head->epsg;
		for( cursor = head->next; cursor != NULL; cursor = cursor->next ){
			if ( epgs_out != cursor->epsg ) { epgs_out = 4326; break; }
		}
	}

	if (( epgs_out 	 == DEFAULT_NODATA ) && ( hSourceSRS == NULL ) ){
		epgs_out = head->epsg;
		for( cursor = head->next; cursor != NULL; cursor = cursor->next ){
			if ( epgs_out != cursor->epsg ) { 
				epgs_out = 4326; break; 
			}
		}
	}

	// Last changes
	if ( epgs_out   == DEFAULT_NODATA ) epgs_out = head->epsg;
	if ( epgs_out 	== 0 		  ) epgs_out = DEFAULT_NODATA;

					
	// Labeling for getList
	if (( epgs_out  >= 32601 ) && ( epgs_out 	<= 32660 ) 	) 	label_index = 1;
	else									label_index = 0;
	proj = epgs_out;


	if ( epgs_out != DEFAULT_NODATA ) {
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Conversion of EPGS %d to Wkt ...", epgs_out);
		if ( ImportFromEPSG(&hTargetSRS, epgs_out ) 	!= OGRERR_NONE ){ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: OSRImportFromEPSG proj incorrect %d", 	epgs_out); fflush(stderr); return 500; }
		if ( OSRExportToWkt(hTargetSRS, &wktTargetSRS ) != OGRERR_NONE ){ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: OSRExportToWkt proj incorrect %d",	epgs_out); fflush(stderr); return 500; }
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: wktTargetSRS: %.80s ...", wktTargetSRS);
	} else { 
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "WARN: No Conversion of EPGS to Wkt, using Wkt ..."); 

		if ( head->wkt != NULL ) { wktTargetSRS = malloc(strlen(head->wkt) + 1 ); 	strcpy(wktTargetSRS, head->wkt); }
		else			 { wktTargetSRS = malloc(1); 				wktTargetSRS[0] = '\0'; }
	}



	//------------------------------------------------------------------------------------------------
	// Force output projection
	if ( force_out_proj != NULL ){
		OGRSpatialReferenceH hSRS;
		char *pszResult = NULL;
		hSRS 		= OSRNewSpatialReference(NULL);

		#if GDAL_VERSION >= 304
		OSRSetAxisMappingStrategy(hSRS, OAMS_TRADITIONAL_GIS_ORDER); // GDAL 3.0 Fix
		#endif
	

		if( OSRSetFromUserInput( hSRS, force_out_proj ) == OGRERR_NONE ) 	OSRExportToWkt( hSRS, &pszResult ); 
		else									ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Translating input user projection failed %s", force_out_proj);
		
		if ( pszResult != NULL ){
			OSRDestroySpatialReference(hTargetSRS); hTargetSRS = OSRClone(hSRS);
			wktTargetSRS = (char *)malloc(strlen(pszResult)+1); strcpy(wktTargetSRS, pszResult);

			const char *auth = OSRGetAttrValue(hSRS, "AUTHORITY",1 );
			if ( auth == NULL ) {
			        Unknown_EPSG_proj4 *proj4_cursor = NULL;
	        		for ( proj4_cursor = EPSG_proj4; proj4_cursor != NULL; proj4_cursor = proj4_cursor->next) {
					if ( OSRIsSame(proj4_cursor->hSRS, hSRS) == TRUE ) { epgs_out = proj = proj4_cursor->epsg; break; }
				}
			} else 	epgs_out = proj = atoi( OSRGetAttrValue(hSRS, "AUTHORITY",1 ) ); 



			if (( proj  >= 32601 ) && ( proj 	<= 32660 ) 	) 	label_index = 1;
			else								label_index = 0;





			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Output projection is force by user input as (EPSG: %d) %.80s ...", proj, pszResult );
		
		}

		OSRDestroySpatialReference( hSRS );
	}

	//------------------------------------------------------------------------------------------------
	// Mars stuff on epsg:4326
	if ( epgs_out == 4326 )  {
		double sphe_src, sphe_trg; const char *sphe_src_str = NULL, *sphe_trg_str = NULL;
		if ( hSourceSRS == NULL ) {
			hSourceSRS = OSRNewSpatialReference(NULL);
			#if GDAL_VERSION >= 304
			OSRSetAxisMappingStrategy(hSourceSRS, OAMS_TRADITIONAL_GIS_ORDER); // GDAL 3.0 Fix
			#endif
			if (head->proj != NULL ) OSRImportFromProj4(hSourceSRS, head->proj);
			else 			 ImportFromEPSG(&hSourceSRS, 	head->epsg );
		}

		sphe_src_str = OSRGetAttrValue( hSourceSRS, "SPHEROID", 1 ); sphe_src = ( sphe_src_str != NULL ) ? atof( sphe_src_str ) : 6378137.0;
		sphe_trg_str = OSRGetAttrValue( hTargetSRS, "SPHEROID", 1 ); sphe_trg = ( sphe_trg_str != NULL ) ? atof( sphe_trg_str ) : 6378137.0;

		if ( sphe_src != sphe_trg ) {
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "WARN: Equirectangular spheroid dimension outside of Earth (%d != %d) ...", (int)sphe_src, (int)sphe_trg );
			char *SPHEROID = malloc(50); bzero(SPHEROID, 49); sprintf(SPHEROID, "+proj=latlong +R=%d", (int)sphe_src); 
			OSRDestroySpatialReference(hTargetSRS); hTargetSRS = OSRNewSpatialReference(NULL); 
			
			#if GDAL_VERSION >= 304
			OSRSetAxisMappingStrategy(hTargetSRS, OAMS_TRADITIONAL_GIS_ORDER); // GDAL 3.0 Fix
			#endif		
			OSRSetFromUserInput(hTargetSRS, SPHEROID ); free(wktTargetSRS); OSRExportToWkt( hTargetSRS, &wktTargetSRS );
		}
	}

	//------------------------------------------------------------------------------------------------


	if ( zoom > 0 ) {
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Output with GZoom %d (%f meters / %f degress) ...", zoom, resByZoom[zoom], resByZoom[zoom] / 111132.92 ); 
		for( cursor = head; cursor != NULL; cursor = cursor->next){
			dfXRes = dfYRes = resByZoom[zoom];
			if ( cursor->epsg == 4326 ) { dfXRes = dfYRes = resByZoom[zoom] / 111132.92; }
			if ( cursor->GeoTransform[1] >  dfXRes ) continue;

			dfMinX = cursor->GeoTransform[0];
			dfMaxX = cursor->GeoTransform[0] + cursor->GeoTransform[1] * cursor->sizeX;
			dfMaxY = cursor->GeoTransform[3];
			dfMinY = cursor->GeoTransform[3] + cursor->GeoTransform[5] * cursor->sizeY;

			cursor->sizeX 		= (int) ((dfMaxX - dfMinX + (dfXRes/2.0)) / dfXRes);
			cursor->sizeY  		= (int) ((dfMaxY - dfMinY + (dfYRes/2.0)) / dfYRes);
	        	cursor->GeoTransform[1]	=  dfXRes;
	        	cursor->GeoTransform[5]	= -dfYRes;
		}
	} else if ( outscale != 1.0 ) {
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Output using scale factor %f ...", outscale ); 
		for( cursor = head; cursor != NULL; cursor = cursor->next){
			dfXRes = cursor->GeoTransform[1] / outscale;
			dfYRes = cursor->GeoTransform[5] / outscale * -1.0;

			dfMinX = cursor->GeoTransform[0];
			dfMaxX = cursor->GeoTransform[0] + cursor->GeoTransform[1] * cursor->sizeX;
			dfMaxY = cursor->GeoTransform[3];
			dfMinY = cursor->GeoTransform[3] + cursor->GeoTransform[5] * cursor->sizeY;

			cursor->sizeX 		= (int) ((dfMaxX - dfMinX + (dfXRes/2.0)) / dfXRes);
			cursor->sizeY  		= (int) ((dfMaxY - dfMinY + (dfYRes/2.0)) / dfYRes);

			if ( cursor->sizeX <= 0 ) cursor->sizeX = 1;
			if ( cursor->sizeY <= 0 ) cursor->sizeY = 1;


	        	cursor->GeoTransform[1]	=  dfXRes;
	        	cursor->GeoTransform[5]	= -dfYRes;

		}
	}

 
	//------------------------------------------------------------------------------------------------
	// Init variables	

	hVRTSrcDS = GDALCreate( hVRTDriver, "", head->sizeX, head->sizeY, 1, GDT_Unknown, NULL ); if ( hVRTSrcDS == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: GDALCreate hVRTSrcDS"); return 500; }	
	GDALSetProjection(	hVRTSrcDS, head->wkt);
	GDALSetGeoTransform(	hVRTSrcDS, head->GeoTransform);

	if ( head->epsg != 0  ) { 
		if ( strcmp (head->wkt, wktTargetSRS ) ){
			hTransformArg = NULL;
			hTransformArg = GDALCreateGenImgProjTransformer( hVRTSrcDS, head->wkt, NULL, wktTargetSRS, FALSE, 0, 1 );

			if ( hTransformArg == NULL ) 	{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: GDALCreateGenImgProjTransformer hTransformArg");  /* GoodbyeMessage(info, "ERROR: %s", CPLGetLastErrorMsg() );*/ return 500; }
			if ( GDALSuggestedWarpOutput2(hVRTSrcDS, GDALGenImgProjTransform, hTransformArg, head->adfDstGeoTransform, &head->nPixels, &head->nLines, adfExtent, 0 ) != CE_None  ){ 
						  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: GDALSuggestedWarpOutput2 hVRTSrcDS"); return 500; }
			GDALClose(hVRTSrcDS);   hVRTSrcDS = NULL; GDALDestroyGenImgProjTransformer(hTransformArg);
		} else  { memcpy(head->adfDstGeoTransform, head->GeoTransform, sizeof(double) * 6 ); }
	} else	{ 	  memcpy(head->adfDstGeoTransform, head->GeoTransform, sizeof(double) * 6 ); epgs_out = 0; }



	x_min_res 	= head->adfDstGeoTransform[1];
	y_min_res 	= head->adfDstGeoTransform[5];
	x_min_res_ori 	= head->GeoTransform[1];
	y_min_res_ori 	= head->GeoTransform[5];

	GeoX_ul = Xp_ul = head->adfDstGeoTransform[0];
	GeoY_ul = Yp_ul = head->adfDstGeoTransform[3];
        GeoX_lr = Xp_lr = GeoX_ul + x_min_res;
        GeoY_lr = Yp_lr = GeoY_ul + y_min_res;

	if ( 	( X_RANGE_DEFINED == FALSE  ) &&
		( Y_RANGE_DEFINED == FALSE  ) ){
		x_input_ul = GeoX_ul; y_input_ul = GeoY_ul;
		x_input_lr = GeoX_ul + ( pxAOISizeX * x_min_res ); y_input_lr = GeoY_ul + ( pxAOISizeY * y_min_res );;

	}

	//------------------------------------------------------------------------------------------------


	if ( 	( x_input_ul == x_input_lr )	&&	// Pixel history case
		( y_input_ul == y_input_lr )	&&
		( Y_RANGE_DEFINED == TRUE  )	&& 
		( Y_RANGE_DEFINED == TRUE  )	){

		for( cursor = head; cursor != NULL; cursor = cursor->next){
			cursor->nPixels = cursor->sizeX;
			cursor->nLines	= cursor->sizeY;
			memcpy(cursor->adfDstGeoTransform, cursor->GeoTransform, sizeof(double) * 6);
		}

	} else for( cursor = head; cursor != NULL; cursor = cursor->next){ // General Case
			// fprintf(stderr, "%s\n", cursor->file); fflush(stderr);
			if ( cursor->epsg == epgs_out ){
				cursor->nPixels = cursor->sizeX;
				cursor->nLines	= cursor->sizeY;
				memcpy(cursor->adfDstGeoTransform, cursor->GeoTransform, sizeof(double) * 6);
				cursor->warp = FALSE;
			} else {
				hVRTSrcDS = GDALCreate( hVRTDriver, "", cursor->sizeX, cursor->sizeY, 1, GDT_Unknown, NULL ); if ( hVRTSrcDS == NULL ) continue;
				GDALSetProjection(	hVRTSrcDS, cursor->wkt);
				GDALSetGeoTransform(	hVRTSrcDS, cursor->GeoTransform);

				hTransformArg = NULL;
				hTransformArg = GDALCreateGenImgProjTransformer( hVRTSrcDS, cursor->wkt, NULL, wktTargetSRS, FALSE, 0, 1 ); 					if ( hTransformArg == NULL ) 	continue;
				if ( GDALSuggestedWarpOutput2(hVRTSrcDS, GDALGenImgProjTransform, hTransformArg, cursor->adfDstGeoTransform, &cursor->nPixels, &cursor->nLines, adfExtent, 0 ) != CE_None  ) 	continue;
				GDALClose(hVRTSrcDS);   hVRTSrcDS = NULL; GDALDestroyGenImgProjTransformer(hTransformArg);
				cursor->warp = TRUE;

			}
			
			// UL------UR
			// |	    |
			// |	    |
			// LL------LR

			Xp_ul = cursor->adfDstGeoTransform[0];
			Yp_ul = cursor->adfDstGeoTransform[3];
			Xp_lr = cursor->adfDstGeoTransform[0] + (double)cursor->nPixels * cursor->adfDstGeoTransform[1] + (double)cursor->nLines * cursor->adfDstGeoTransform[2];
			Yp_lr = cursor->adfDstGeoTransform[3] + (double)cursor->nPixels * cursor->adfDstGeoTransform[4] + (double)cursor->nLines * cursor->adfDstGeoTransform[5];

			if ( fabs(cursor->adfDstGeoTransform[1]) < fabs(x_min_res) 	) 	x_min_res	= cursor->adfDstGeoTransform[1];
			if ( fabs(cursor->adfDstGeoTransform[5]) < fabs(y_min_res) 	) 	y_min_res	= cursor->adfDstGeoTransform[5];
			if ( fabs(cursor->GeoTransform[1]) 	 < fabs(x_min_res_ori)  ) 	x_min_res_ori	= cursor->GeoTransform[1];
			if ( fabs(cursor->GeoTransform[5]) 	 < fabs(y_min_res_ori)  ) 	y_min_res_ori	= cursor->GeoTransform[5];
			if ( Xp_ul < GeoX_ul )							GeoX_ul		= Xp_ul;
			if ( Xp_lr > GeoX_lr ) 							GeoX_lr		= Xp_lr;
			if ( Yp_ul > GeoY_ul )							GeoY_ul		= Yp_ul;
			if ( Yp_lr < GeoY_lr ) 							GeoY_lr		= Yp_lr;

	}


	// Use same resolution for every warp image... Why? Try to merge image with different resolution, dude ...
	dfXRes =  x_min_res;
	dfYRes = -y_min_res;

	for( cursor = head; cursor != NULL; cursor = cursor->next){
		cursor->nPixels 		= (int)( ( ( cursor->adfDstGeoTransform[0] + cursor->adfDstGeoTransform[1] * (double)cursor->nPixels ) - cursor->adfDstGeoTransform[0]  + ( x_min_res/2.0) ) /  x_min_res );
		cursor->nLines 			= (int)( (   cursor->adfDstGeoTransform[3] - (cursor->adfDstGeoTransform[3] + cursor->adfDstGeoTransform[5] * (double)cursor->nLines )  + (-y_min_res/2.0) ) / -y_min_res );

		cursor->adfDstGeoTransform[1] 	= x_min_res;
		cursor->adfDstGeoTransform[5] 	= y_min_res;


		// Resize original image ... Yo man! 
		cursor->sizeX 			= (int)( ( ( cursor->GeoTransform[0] + cursor->GeoTransform[1] * (double)cursor->sizeX ) - cursor->GeoTransform[0]  + ( x_min_res_ori/2.0) ) /  x_min_res_ori );
		cursor->sizeY 			= (int)( (   cursor->GeoTransform[3] - (cursor->GeoTransform[3] + cursor->GeoTransform[5] * (double)cursor->sizeY ) + (-y_min_res_ori/2.0) ) / -y_min_res_ori );

		cursor->GeoTransform[1] 	= x_min_res_ori;
		cursor->GeoTransform[5] 	= y_min_res_ori;
	}


	//-------------	 
	// Defined a common area for every image
	if 	( X_RANGE_DEFINED == FALSE  ) { x_input_ul = GeoX_ul; x_input_lr = GeoX_lr; }
	if 	( Y_RANGE_DEFINED == FALSE  ) { y_input_ul = GeoY_ul; y_input_lr = GeoY_lr; }


	if 	( ( x_input_ul == x_input_lr ) && ( X_RANGE_DEFINED == TRUE  ) )	{ pxAOISizeX = 1; 	GeoX_lr = GeoX_ul +  x_min_res; 			} 
	else 										{ pxAOISizeX = (int)( ( GeoX_lr - GeoX_ul +  (x_min_res/2.0) ) / x_min_res  );	}

	if 	( ( y_input_ul == y_input_lr ) && ( Y_RANGE_DEFINED == TRUE  ) )	{ pxAOISizeY = 1;	GeoY_lr = GeoY_ul +  y_min_res; 			}
	else 										{ pxAOISizeY = (int)( ( GeoY_lr - GeoY_ul +  (y_min_res/2.0) ) / y_min_res  );	}	


	if ( pxAOISizeX <= 0 ) pxAOISizeX = 1;
	if ( pxAOISizeY <= 0 ) pxAOISizeY = 1;

	if ( CROP == FALSE ) {
		if ( INPUT_GEOM == TRUE ) {
			if ( WMTS_MODE == FALSE ) {
				OGRGeometryH hGeomTmp;
				OGREnvelope sEnvelope;	
	
				hGeomTmp = OGR_G_Clone(hGeom);
				if ( epgs_out != 4326 ){
			 		OGRSpatialReferenceH geoSRS = OSRNewSpatialReference(NULL);
					if ( ImportFromEPSG(&geoSRS,    epgs_out ) != OGRERR_NONE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: OSRImportFromEPSG geometry proj incorrect %d", 	epgs_out); return 500; }
					if ( OGR_G_TransformTo(hGeomTmp, geoSRS)   != OGRERR_NONE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: OGR_G_TransformTo to %d", 				epgs_out); return 500; }
					OSRDestroySpatialReference(geoSRS);
				}

				OGR_G_GetEnvelope( hGeomTmp, &sEnvelope );
				OGR_G_DestroyGeometry(hGeomTmp);
				x_input_ul = GeoX_ul = (double)((int)(sEnvelope.MinX / x_min_res)) * x_min_res;
				x_input_lr = GeoX_lr = (double)((int)(sEnvelope.MaxX / x_min_res)) * x_min_res;
				y_input_lr = GeoY_lr = (double)((int)(sEnvelope.MinY / y_min_res)) * y_min_res;
				y_input_ul = GeoY_ul = (double)((int)(sEnvelope.MaxY / y_min_res)) * y_min_res;

				ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Skip croping input gemotry ( UL: %fx%f LR: %fx%f ) ...",  GeoX_ul, GeoY_ul, GeoX_lr, GeoY_lr );

				pxAOISizeX = (int)( ( GeoX_lr - GeoX_ul + (x_min_res/2.0) ) /  x_min_res );
				pxAOISizeY = (int)( ( GeoY_lr - GeoY_ul + (y_min_res/2.0) ) /  y_min_res );

				for( cursor = head; cursor != NULL; cursor = cursor->next){
					cursor->adfDstGeoTransform[0] 	= GeoX_ul;
					cursor->adfDstGeoTransform[3] 	= GeoY_ul;
					cursor->adfDstGeoTransform[1]	= x_min_res;
				        cursor->adfDstGeoTransform[5]	= y_min_res;
					cursor->nPixels			= pxAOISizeX; 
					cursor->nLines			= pxAOISizeY; 
				}
			} else {
				if ( epgs_out != 4326 ){
					OGRSpatialReferenceH iSRS = OSRNewSpatialReference(NULL); OGRSpatialReferenceH oSRS = OSRNewSpatialReference(NULL); 
                                        #if GDAL_VERSION >= 304
                                        OSRSetAxisMappingStrategy(iSRS, OAMS_TRADITIONAL_GIS_ORDER); // GDAL 3.0 Fix
                                        OSRSetAxisMappingStrategy(oSRS, OAMS_TRADITIONAL_GIS_ORDER); // GDAL 3.0 Fix
                                        #endif
					OSRImportFromEPSG( oSRS, epgs_out); OSRImportFromEPSG( iSRS, 4326);
					OGRCoordinateTransformationH hCT = OCTNewCoordinateTransformation( iSRS, oSRS );
					OCTTransform(hCT, 1, &x_input_ul, &y_input_ul, NULL);
					OCTTransform(hCT, 1, &x_input_lr, &y_input_lr, NULL);

					OCTDestroyCoordinateTransformation(hCT);
					OSRDestroySpatialReference(iSRS); OSRDestroySpatialReference(oSRS);

				}

				GeoX_ul = x_input_ul; GeoX_lr = x_input_lr;  GeoY_lr = y_input_lr; GeoY_ul = y_input_ul;


				pxAOISizeX = (int)( ( GeoX_lr - GeoX_ul + (x_min_res/2.0) ) /  x_min_res );
				pxAOISizeY = (int)( ( GeoY_lr - GeoY_ul + (y_min_res/2.0) ) /  y_min_res );

				for( cursor = head; cursor != NULL; cursor = cursor->next){
					cursor->adfDstGeoTransform[0] 	= GeoX_ul;
					cursor->adfDstGeoTransform[3] 	= GeoY_ul;
					cursor->adfDstGeoTransform[1]	= x_min_res;
				        cursor->adfDstGeoTransform[5]	= y_min_res;
					cursor->nPixels			= pxAOISizeX; 
					cursor->nLines			= pxAOISizeY; 
				}			
			}
		}
	}
	


	double x_min_res_ori_bkp = x_min_res_ori;

	//-------------	 
	// Force output size
	if ( ( outsize_x > 0 ) || ( outsize_y > 0 ) || ( outresolution > 0 ) ) {
		int pxAOISizeX_ori = pxAOISizeX;
		int pxAOISizeY_ori = pxAOISizeY;


		if ( outresolution > 0 ) { 
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Force output resolution at %f  ...", outresolution ); 
			dfXRes = dfYRes = outresolution;
			pxAOISizeX = (int)( ( GeoX_lr - GeoX_ul + (dfXRes/2.0) ) / dfXRes );
			pxAOISizeY = (int)( ( GeoY_lr - GeoY_ul + (dfYRes/2.0) ) / dfYRes ); 
		} else {


			if ( outsize_x > 0 ) pxAOISizeX = outsize_x;
			if ( outsize_y > 0 ) pxAOISizeY = outsize_y;
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Output using custom size %dx%d ...", outsize_x, outsize_y ); 

			dfXRes = ( GeoX_lr - GeoX_ul ) / (double)pxAOISizeX;
			dfYRes = ( GeoY_ul - GeoY_lr ) / (double)pxAOISizeY;
		}


		x_min_res =  dfXRes; y_min_res = -dfYRes;
		x_min_res_ori =  ( (double)pxAOISizeX_ori / (double)pxAOISizeX ) *  x_min_res_ori;
		y_min_res_ori =  ( (double)pxAOISizeY_ori / (double)pxAOISizeY ) * -y_min_res_ori;
		for( cursor = head; cursor != NULL; cursor = cursor->next){
			dfMinX = cursor->adfDstGeoTransform[0];
			dfMaxX = cursor->adfDstGeoTransform[0] + cursor->adfDstGeoTransform[1] * cursor->nPixels;
			dfMaxY = cursor->adfDstGeoTransform[3];
			dfMinY = cursor->adfDstGeoTransform[3] + cursor->adfDstGeoTransform[5] * cursor->nLines;
	
			// Resize warped image
			cursor->nPixels 		= (int)( ( (dfMaxX - dfMinX) + (dfXRes * (double)pxAOISizeX / (double)pxAOISizeX_ori ) ) / dfXRes); if ( cursor->nPixels <= 0 ) { cursor->file[0] = '\0'; continue; } 
			cursor->nLines  		= (int)( ( (dfMaxY - dfMinY) + (dfYRes * (double)pxAOISizeY / (double)pxAOISizeY_ori ) ) / dfYRes); if ( cursor->nLines  <= 0 ) { cursor->file[0] = '\0'; continue; } 
			cursor->adfDstGeoTransform[1]	=  dfXRes;
		        cursor->adfDstGeoTransform[5]	= -dfYRes;

			// Resize original image ... Yo man! 
			cursor->sizeX 			= (int)( ( ( cursor->GeoTransform[0] +  cursor->GeoTransform[1] * cursor->sizeX ) - cursor->GeoTransform[0]  + (x_min_res_ori/2.0) ) / x_min_res_ori );
							if ( cursor->sizeX <= 0 ) { cursor->file[0] = '\0'; continue; }
			cursor->sizeY 			= (int)( (   cursor->GeoTransform[3] - (cursor->GeoTransform[3] + cursor->GeoTransform[5] * cursor->sizeY  ) + (y_min_res_ori/2.0) ) / y_min_res_ori );
							if ( cursor->sizeY <= 0 ) { cursor->file[0] = '\0'; continue; }

			cursor->GeoTransform[1] 	=  x_min_res_ori;
			cursor->GeoTransform[5] 	= -y_min_res_ori;	

		}
	}


	//-------------	 
	// Check for WMTS/WMS the requested tile in the real file is too huge
	if ( ( info->uri != NULL ) && ( ! strncmp(info->uri, "/wmts", 5 ) ) && ( WMTSMaxTileRatio != 0 ) ) {
		if ( ( x_min_res_ori  / x_min_res_ori_bkp  ) > WMTSMaxTileRatio ){
			struct stat tile_info; 
			stat(ZOOM_IN_TILE, &tile_info);
			realRaster = malloc(tile_info.st_size); if ( realRaster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc realRaster");   return 500; }
			FILE *fp = fopen(ZOOM_IN_TILE, "rb");
			if (fp != NULL) {
				size_t blocks_read = fread(realRaster, tile_info.st_size, 1, fp);
				if (blocks_read == 1) {
					ap_set_content_type(r, "image/png");
					i = ap_rwrite( (const void *)realRaster, tile_info.st_size, r );
					fclose(fp); free(realRaster);

					clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;			
					ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "WARN: [WMTS] Source image memory usage to big for a tile, return ZoomIn in %.3f sec, Total Time %.3f", 
						(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total );
					clock_gettime(CLOCK_REALTIME, &time_before);
					cleanList(head, r); // Clean
					return 0;
				}
			}
		}	
	}




	ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Defined common output resoution: %f x %f for common output predicted size %dx%d...", dfXRes, dfYRes, pxAOISizeX, pxAOISizeY ); 

	//-------------	 
	// I dont' rember i've added this line ... mah ... commented
	//if ( epgs_out == 4326 ) GeoX_ul += x_min_res; 

	lowerCorner[0] = GeoY_lr;
	lowerCorner[1] = GeoX_ul;
	upperCorner[0] = GeoY_ul;
	upperCorner[1] = GeoX_lr;

	clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;

	ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Finish in GetCoverage Init Variale in %.3f sec, Total Time %.3f",
			(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
	clock_gettime(CLOCK_REALTIME, &time_before);



	//------------------------------------------------------------------------------------------------

	if ( Request == GetList ){	
		clock_gettime(CLOCK_REALTIME, &time_before); ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Enter in GetList");

		// In case og GetList and Math for virtual collection i aply a different file list
		if ( ( VIRTUAL_COLLECTION == TRUE ) && ( mathChain != NULL ) ){
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: GetList modified for Virtual Collection and Math ...");
			int 	collection_need 	= 0;
			int 	hit_number_for_time	= 0;
			int 	time_serie_number 	= hit_number;
			char	**single_math_list	= NULL;

			single_math_list = (char **)malloc(sizeof(char *) * (hit_number + 1) ); for (i = 0; i < (hit_number + 1 ); i++) single_math_list[i] = NULL;

			for( cursor = head; cursor != NULL; cursor = cursor->next ){
				for (i = 0; single_math_list[i] != NULL; i++ ) if ( ! strcmp(single_math_list[i], cursor->math_id ) ) break;
				if ( i == hit_number ) continue;
				single_math_list[i] = malloc(strlen(cursor->math_id) + 1); strcpy(single_math_list[i], cursor->math_id);
			}

			for (i = 0; single_math_list[i] != NULL; i++, collection_need++ );

			// Clean if exists something allocated
			if ( vrt_head != NULL ) while ((vrt_cursor = vrt_head) != NULL) { vrt_head = vrt_head->next; free(vrt_cursor); }

        		vrt_head        = NULL;
			vrt_cursor      = NULL;

			time_serie = (time_t *)malloc(sizeof(time_t) * hit_number);
			for( cursor = head, i = 0; cursor != NULL; cursor = cursor->next, i++) time_serie[i] = cursor->time;
			qsort(time_serie, hit_number, sizeof(time_t), timecmpfunc);
			for (i = 0, j = 0; i < hit_number; i++) if( time_serie[i] != time_serie[j] ) { j++; time_serie[j] = time_serie[i]; }
			for (i = j + 1	 ; i < hit_number; i++) time_serie[i] = -1;

			for ( time_serie_index = 0, hit_number = 0; time_serie_index < time_serie_number ; time_serie_index++ ){
				if ( time_serie[time_serie_index] < 0 ) break;


				for( cursor = head, hit_number_for_time = 0; cursor != NULL; cursor = cursor->next ) {
					if ( cursor->time != time_serie[time_serie_index] ) 	continue;
					else							hit_number_for_time++; 
				}

				if ( hit_number_for_time < collection_need ) continue;
				for( cursor = head ; cursor != NULL; cursor = cursor->next ) if ( cursor->time == time_serie[time_serie_index] ) break;
				if ( cursor == NULL ) continue;

				if ( vrt_head == NULL )	{ vrt_head = vrt_cursor = (block)malloc(sizeof(struct sblock)); vrt_cursor->prev = NULL;				  	     }
				else			{ vrt_cursor->next 	= (block)malloc(sizeof(struct sblock)); vrt_cursor->next->prev = vrt_cursor; vrt_cursor = vrt_cursor->next;  }

				initBlock(vrt_cursor); memcpy(vrt_cursor, cursor, sizeof(struct sblock));

				bzero(vrt_cursor->file, MAX_PATH_LEN - 1); strcpy(vrt_cursor->file,"VIRTUAL_OUTPUT");
				vrt_cursor->next = NULL;
				vrt_cursor->type = 6;
				hit_number++;
			}

			free(time_serie);
			while ((cursor = head) != NULL) { head = head->next; free(cursor); }
			head = vrt_head; 
			if ( hit_number <= 0 ) {  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: No images found ..."); GoodbyeMessage(info, "No images found"); return 404; }
			
		}



		if ( outType == JSON ){
			json_object     	*jobj  	= json_object_new_object();
			json_object     	*jgeo  	= json_object_new_array();
			json_object     	*jtime 	= json_object_new_array();
			json_object     	*jsize 	= json_object_new_array();

			OGRSpatialReferenceH	hSRS	= OSRNewSpatialReference(NULL);

			
			for (i = 0; i < 6; i++ ) adfGeoTransform[i] = 0.0;
			adfGeoTransform[0] = lowerCorner[1]; adfGeoTransform[3] = upperCorner[0]; adfGeoTransform[1] = x_min_res; adfGeoTransform[5] = y_min_res;
			for (i = 0; i < 6; i++ ) json_object_array_add(jgeo, json_object_new_double(adfGeoTransform[i]));
	

			json_object_array_add(jtime, json_object_new_int(t_min));
			json_object_array_add(jtime, json_object_new_int(t_max));

			json_object_array_add(jsize, json_object_new_int(hit_number));
			json_object_array_add(jsize, json_object_new_int(head->nband));
			json_object_array_add(jsize, json_object_new_int(pxAOISizeY));
			json_object_array_add(jsize, json_object_new_int(pxAOISizeX));
	

			json_object_object_add(jobj, "datasetId", 	json_object_new_string(prod));
			json_object_object_add(jobj, "timeRange", 	jtime);
			json_object_object_add(jobj, "dimensions", 	jsize);
			json_object_object_add(jobj, "geoTransform", 	jgeo);
			OSRImportFromWkt(hSRS, &wktTargetSRS);
			OSRExportToProj4(hSRS, &wktTargetSRS);
			json_object_object_add(jobj, "projection", 	json_object_new_string(wktTargetSRS));
			OSRDestroySpatialReference(hSRS);
	
			json_object *jdata = json_object_new_array(); // Main data array
	
			for( cursor = head; cursor != NULL; cursor = cursor->next){
				json_object *jcur  = json_object_new_object();
				json_object_object_add(jcur, "unixtime", json_object_new_int(	cursor->time) );
				json_object_object_add(jcur, "datetime", json_object_new_string(datatimeStringFromUNIX( gmtime(&cursor->time)) ));
				json_object_object_add(jcur, "type", 	 json_object_new_string(GDALGetDataTypeName(cursor->type)));
				json_object_object_add(jcur, "nband", 	 json_object_new_int(	cursor->nband) );
				if ( cursor->high != DEFAULT_NODATA )
				json_object_object_add(jcur, "high", 	 json_object_new_double(cursor->high));

				if (( cursor->epsg <= 0 ) &&  (cursor->proj != NULL ))  { bzero( epsg_res_tmp, 255 ); sprintf(epsg_res_tmp, "%s", 	cursor->proj); }
				else 			 			  	{ bzero( epsg_res_tmp, 255 ); sprintf(epsg_res_tmp, "EPSG:%d", 	cursor->epsg); }
				json_object_object_add(jcur, "proj", 	 json_object_new_string(epsg_res_tmp));

				json_object *bbox    = json_object_new_array();
				json_object *point1  = json_object_new_array(); json_object_array_add(point1, json_object_new_double(cursor->GeoTransform[0])); 
										json_object_array_add(point1, json_object_new_double(cursor->GeoTransform[3])); 
										json_object_array_add(bbox, point1);

				json_object *point2  = json_object_new_array(); json_object_array_add(point2, json_object_new_double(cursor->GeoTransform[0] + (double)cursor->sizeX * cursor->GeoTransform[1])); 
										json_object_array_add(point2, json_object_new_double(cursor->GeoTransform[3])); 
										json_object_array_add(bbox, point2);

				json_object *point3  = json_object_new_array(); json_object_array_add(point3, json_object_new_double(cursor->GeoTransform[0] + (double)cursor->sizeX * cursor->GeoTransform[1])); 
										json_object_array_add(point3, json_object_new_double(cursor->GeoTransform[3] + (double)cursor->sizeY * cursor->GeoTransform[5])); 
										json_object_array_add(bbox, point3);

				json_object *point4  = json_object_new_array(); json_object_array_add(point4, json_object_new_double(cursor->GeoTransform[0])); 
										json_object_array_add(point4, json_object_new_double(cursor->GeoTransform[3] + (double)cursor->sizeY * cursor->GeoTransform[5])); 
										json_object_array_add(bbox, point4);
										json_object_array_add(bbox, point1);

				json_object_object_add(jcur, "bbox", bbox);

				json_object *prodgeo = json_object_new_array(); for (i = 0; i < 6; i++ ) json_object_array_add(prodgeo, json_object_new_double(cursor->GeoTransform[i]));
				json_object_object_add(jcur, "geotransform", prodgeo);

				json_object_array_add(jdata, jcur);

	
			}
			json_object_object_add(jobj, "prods", jdata);
	
			if ( output_name != NULL ) { bzero(final_path, MAX_PATH_LEN - 1); sprintf(final_path, "attachment; filename=\"%s\"", output_name ); apr_table_add(r->headers_out, "Content-disposition", final_path); }
		        ap_set_content_type(r, "text/json");
			ap_rprintf(r, "%s", json_object_to_json_string(jobj));

			


			while ((cursor = head) != NULL) { head = head->next; free(cursor); }
			clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;			
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Finish in GetList JSON in %.3f sec, Total Time %.3f", 
					(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
			clock_gettime(CLOCK_REALTIME, &time_before);

			bzero(log_tmp_str, MAX_STR_LEN - 1); sprintf(log_tmp_str, "%f", time_total); addStatToPush(info, "exec_time",  log_tmp_str, GDT_Float32 ); // STATS


			return 0;
		}
		

		// proj, time_URL[time_index], dimenstion_Label[i][0], dimenstion_Label[i][1], time_Label[time_index], dimenstion_unit[i], dimenstion_unit[i], time_unit[time_index]);
		if ( output_name != NULL ) { bzero(final_path, MAX_PATH_LEN - 1); sprintf(final_path, "attachment; filename=\"%s\"", output_name ); apr_table_add(r->headers_out, "Content-disposition", final_path); }
	        ap_set_content_type(r, "text/xml");

		ap_rprintf(r, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
	
		ap_rprintf(r, "<gmlcov:ReferenceableGridCoverage\n");
		ap_rprintf(r, "    xmlns='http://www.opengis.net/gml/3.2'\n");
		ap_rprintf(r, "    xmlns:gml='http://www.opengis.net/gml/3.2'\n");
		ap_rprintf(r, "    xmlns:gmlcov='http://www.opengis.net/gmlcov/1.0'\n");
		ap_rprintf(r, "    xmlns:swe='http://www.opengis.net/swe/2.0'\n");
		ap_rprintf(r, "    xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'\n");
		ap_rprintf(r, "    xmlns:wcs='http://www.opengis.net/wcs/2.0' gml:id='%s'\n", prod );
		ap_rprintf(r, "    xsi:schemaLocation='http://www.opengis.net/wcs/2.0 http://schemas.opengis.net/wcs/2.0/wcsAll.xsd'>\n\n");
		ap_rprintf(r, "  <boundedBy>\n");
		if (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA ))
		ap_rprintf(r, "     <Envelope srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s h\" uomLabels=\"%s %s %s h\" srsDimension=\"4\">\n", 
				proj, time_URL[time_index], dimenstion_Label[label_index][0], dimenstion_Label[label_index][1], time_Label[time_index], dimenstion_unit[label_index], dimenstion_unit[label_index], time_unit[time_index]);
		else
		ap_rprintf(r, "     <Envelope srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s\" uomLabels=\"%s %s %s\" srsDimension=\"3\">\n", 
				proj, time_URL[time_index], dimenstion_Label[label_index][0], dimenstion_Label[label_index][1], time_Label[time_index], dimenstion_unit[label_index], dimenstion_unit[label_index], time_unit[time_index]);
		ap_rprintf(r, "      <lowerCorner>%f %f %ld", lowerCorner[0], lowerCorner[1], t_min); if (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA ))   ap_rprintf(r, " %f", high_min);	ap_rprintf(r, "</lowerCorner>\n");
		ap_rprintf(r, "      <upperCorner>%f %f %ld", upperCorner[0], upperCorner[1], t_max); if (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA ))   ap_rprintf(r, " %f", high_max);	ap_rprintf(r, "</upperCorner>\n");
		ap_rprintf(r, "    </Envelope>\n");
		ap_rprintf(r, "  </boundedBy>\n\n");
		ap_rprintf(r, "  <domainSet>\n");
		ap_rprintf(r, "    <gmlrgrid:ReferenceableGridByVectors dimension=\"3\" gml:id=\"%s-grid\"\n",  prod);
		ap_rprintf(r, "		xmlns:gmlrgrid=\"http://www.opengis.net/gml/3.3/rgrid\"\n");
		ap_rprintf(r, "		xsi:schemaLocation=\"http://www.opengis.net/gml/3.3/rgrid http://schemas.opengis.net/gml/3.3/referenceableGrid.xsd\">\n");
		ap_rprintf(r, "      <limits>\n");
		ap_rprintf(r, "         <GridEnvelope>\n");
		ap_rprintf(r, "           <low>0 0 0</low>\n");
		ap_rprintf(r, "           <high>%d %d %d</high>\n", pxAOISizeX - 1, pxAOISizeY - 1, hit_number - 1);
		ap_rprintf(r, "         </GridEnvelope>\n");
		ap_rprintf(r, "      </limits>\n");
		ap_rprintf(r, "      <axisLabels>%s %s %s</axisLabels>\n", dimenstion_Label[label_index][1], dimenstion_Label[label_index][0], time_Label[time_index]);
		ap_rprintf(r, "      <gmlrgrid:origin>\n");
		ap_rprintf(r, "        <Point gml:id=\"%s-origin\" srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s\" uomLabels=\"%s %s %s\" srsDimension=\"3\">\n", 
				prod, proj, time_URL[time_index], dimenstion_Label[label_index][0], dimenstion_Label[label_index][1], time_Label[time_index], dimenstion_unit[label_index], dimenstion_unit[label_index], time_unit[time_index]);
		ap_rprintf(r, "          <pos>%f %f %ld</pos>\n", upperCorner[0] - (y_min_res/2), lowerCorner[1] - (x_min_res/2), t_min);
		ap_rprintf(r, "        </Point>\n");
		ap_rprintf(r, "      </gmlrgrid:origin>\n");
		ap_rprintf(r, "      <gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "        <gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "          <gmlrgrid:offsetVector srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s\" uomLabels=\"%s %s %s\" srsDimension=\"3\">0 %f 0</gmlrgrid:offsetVector>\n", 
			proj, time_URL[time_index], dimenstion_Label[label_index][0], dimenstion_Label[label_index][1], time_Label[time_index], dimenstion_unit[label_index], dimenstion_unit[label_index], time_unit[time_index], x_min_res);
		ap_rprintf(r, "          <gmlrgrid:coefficients />\n");
		ap_rprintf(r, "          <gmlrgrid:gridAxesSpanned>%s</gmlrgrid:gridAxesSpanned>\n", dimenstion_Label[label_index][1]);
		ap_rprintf(r, "          <gmlrgrid:sequenceRule axisOrder=\"+1\">Linear</gmlrgrid:sequenceRule>\n");
		ap_rprintf(r, "        </gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "      </gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "      <gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "        <gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "          <gmlrgrid:offsetVector srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s\" uomLabels=\"%s %s %s\" srsDimension=\"3\">%f 0 0</gmlrgrid:offsetVector>\n", 
			proj, time_URL[time_index], dimenstion_Label[label_index][0], dimenstion_Label[label_index][1], time_Label[time_index], dimenstion_unit[label_index], dimenstion_unit[label_index], time_unit[time_index], y_min_res);
		ap_rprintf(r, "          <gmlrgrid:coefficients />\n");
		ap_rprintf(r, "          <gmlrgrid:gridAxesSpanned>%s</gmlrgrid:gridAxesSpanned>\n", dimenstion_Label[label_index][0]);
		ap_rprintf(r, "          <gmlrgrid:sequenceRule axisOrder=\"+1\">Linear</gmlrgrid:sequenceRule>\n");
		ap_rprintf(r, "        </gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "      </gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "      <gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "        <gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "          <gmlrgrid:offsetVector srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s\" uomLabels=\"%s %s %s\" srsDimension=\"3\">0 0 1</gmlrgrid:offsetVector>\n",
			proj, time_URL[time_index], dimenstion_Label[label_index][0], dimenstion_Label[label_index][1], time_Label[time_index], dimenstion_unit[label_index], dimenstion_unit[label_index], time_unit[time_index]);
		ap_rprintf(r, "          <gmlrgrid:coefficients>\n");
		for( cursor = head; cursor != NULL; cursor = cursor->next) ap_rprintf(r, "%ld ", cursor->time - t_min); ap_rprintf(r, "\n");
		ap_rprintf(r, "          </gmlrgrid:coefficients>\n");
		ap_rprintf(r, "          <gmlrgrid:gridAxesSpanned>t</gmlrgrid:gridAxesSpanned>\n");
		ap_rprintf(r, "        <gmlrgrid:sequenceRule axisOrder=\"+1\">Linear</gmlrgrid:sequenceRule>\n");
		ap_rprintf(r, "      </gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "    </gmlrgrid:generalGridAxis>\n");

		if (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA )){
		ap_rprintf(r, "      <gmlrgrid:generalGridAxis>\n");
		ap_rprintf(r, "        <gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "          <gmlrgrid:offsetVector srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s h\" uomLabels=\"%s %s %s h\" srsDimension=\"4\">0 0 0 1</gmlrgrid:offsetVector>\n",
			proj, time_URL[time_index], dimenstion_Label[label_index][0], dimenstion_Label[label_index][1], time_Label[time_index], dimenstion_unit[label_index], dimenstion_unit[label_index], time_unit[time_index]);
		ap_rprintf(r, "          <gmlrgrid:coefficients>\n");
		for( cursor = head; cursor != NULL; cursor = cursor->next) ap_rprintf(r, "%f ", cursor->high - high_min); ap_rprintf(r, "\n");
		ap_rprintf(r, "          </gmlrgrid:coefficients>\n");
		ap_rprintf(r, "          <gmlrgrid:gridAxesSpanned>t</gmlrgrid:gridAxesSpanned>\n");
		ap_rprintf(r, "        <gmlrgrid:sequenceRule axisOrder=\"+1\">Linear</gmlrgrid:sequenceRule>\n");
		ap_rprintf(r, "      </gmlrgrid:GeneralGridAxis>\n");
		ap_rprintf(r, "    </gmlrgrid:generalGridAxis>\n");
		}


		ap_rprintf(r, "  </gmlrgrid:ReferenceableGridByVectors>\n");
		ap_rprintf(r, "</domainSet>\n");
		ap_rprintf(r, "<rangeSet>\n");
		ap_rprintf(r, "  <DataBlock>\n");
		ap_rprintf(r, "    <rangeParameters />\n");
		//ap_rprintf(r, "      <fileList ts=\";\" cs=\",\">\n");
		ap_rprintf(r, "      <fileList>\n");
	
	
		//------------------------------------------------------------------------------------------------

		if (head != NULL ) for( cursor = head; cursor != NULL; cursor = cursor->next){
		ap_rprintf(r, "       <file id=\"%s\">\n", 		basename(trim_gdal_path(cursor->file)) );
		ap_rprintf(r, "        <path>");
					for (i = 0; i < strlen(cursor->file); i++){ 
						if 	( cursor->file[i] == '"' ) 	ap_rprintf(r, "&quot;"	);
						else if ( cursor->file[i] == '&' ) 	ap_rprintf(r, "&amp;"	);
						else if ( cursor->file[i] == '<' ) 	ap_rprintf(r, "&lt;"	);
						else if ( cursor->file[i] == '>' ) 	ap_rprintf(r, "&gt;"	);
						else if ( cursor->file[i] == '\'' ) 	ap_rprintf(r, "&apos;"	);
						else 					ap_rprintf(r, "%c", cursor->file[i]);
					
					}ap_rprintf(r, "</path>\n");
		ap_rprintf(r, "        <nband>%d</nband>\n", 		cursor->nband );

		if (( cursor->epsg <= 0 ) &&  (cursor->proj != NULL ))  ap_rprintf(r, "        <proj>%s</proj>\n",      cursor->proj );
		else 			 			  	ap_rprintf(r, "        <proj>EPSG:%d</proj>\n",	cursor->epsg );


		ap_rprintf(r, "        <geotransform>"); for(i = 0; i < 5; i++) ap_rprintf(r, "%f ", cursor->GeoTransform[i]); ap_rprintf(r, "%f</geotransform>\n", cursor->GeoTransform[i]);
		ap_rprintf(r, "        <bbox>%f %f, %f %f, %f %f, %f %f, %f %f</bbox>\n",
					cursor->GeoTransform[0], 							cursor->GeoTransform[3],
					cursor->GeoTransform[0] + (double)cursor->sizeX * cursor->GeoTransform[1],	cursor->GeoTransform[3],
					cursor->GeoTransform[0] + (double)cursor->sizeX * cursor->GeoTransform[1],	cursor->GeoTransform[3] + (double)cursor->sizeY * cursor->GeoTransform[5],
					cursor->GeoTransform[0],							cursor->GeoTransform[3] + (double)cursor->sizeY * cursor->GeoTransform[5],
					cursor->GeoTransform[0],							cursor->GeoTransform[3]);

		if ( cursor->high != DEFAULT_NODATA )
		ap_rprintf(r, "        <high>%f</high>\n",		cursor->high );
		ap_rprintf(r, "        <datatype>%d</datatype>\n",	cursor->type );
		ap_rprintf(r, "        <unixtime>%ld</unixtime>\n",	cursor->time );
		ap_rprintf(r, "        <datetime>%s</datetime>\n",	datatimeStringFromUNIX( gmtime(&cursor->time) ) );
		ap_rprintf(r, "       </file>\n");
		}

		//------------------------------------------------------------------------------------------------
	
		ap_rprintf(r, "      </fileList>\n");
		ap_rprintf(r, "   </DataBlock>\n");
		ap_rprintf(r, "  </rangeSet>\n");
		ap_rprintf(r, "  <coverageFunction>\n");
		ap_rprintf(r, "   <GridFunction>\n");
		ap_rprintf(r, "    <sequenceRule axisOrder=\"+1 +2 +3\">Linear</sequenceRule>\n");
		ap_rprintf(r, "    <startPoint>0 0 0</startPoint>\n");
		ap_rprintf(r, "   </GridFunction>\n");
		ap_rprintf(r, "  </coverageFunction>\n");
		ap_rprintf(r, "  <gmlcov:rangeType>\n");
		ap_rprintf(r, "    <swe:DataRecord>\n");
		ap_rprintf(r, "      <swe:field name=\"value\">\n");
		ap_rprintf(r, "        <swe:Quantity xmlns:swe=\"http://www.opengis.net/swe/2.0\" definition=\"%s\">\n", OGC_dataTypes[head->type]);
		ap_rprintf(r, "          <swe:label>float</swe:label>\n");
		ap_rprintf(r, "          <swe:description>primitive</swe:description>\n");
		ap_rprintf(r, "          <swe:uom code=\"10^0\" />\n");
		ap_rprintf(r, "          <swe:constraint>\n");
		ap_rprintf(r, "            <swe:AllowedValues>\n");
		ap_rprintf(r, "              <swe:interval>-3.4028234E+38 3.4028234E+38</swe:interval>\n");
		ap_rprintf(r, "            </swe:AllowedValues>\n");
		ap_rprintf(r, "          </swe:constraint>\n");
		ap_rprintf(r, "        </swe:Quantity>\n");
		ap_rprintf(r, "      </swe:field>\n");
		ap_rprintf(r, "    </swe:DataRecord>\n");
		ap_rprintf(r, "  </gmlcov:rangeType>\n");
		ap_rprintf(r, "</gmlcov:ReferenceableGridCoverage>\n");
		




		clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;		
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Finish in GetList in %.3f sec, Total Time %.3f", 
				(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
		clock_gettime(CLOCK_REALTIME, &time_before);

		bzero(log_tmp_str, MAX_STR_LEN - 1); sprintf(log_tmp_str, "%f", time_total); addStatToPush(info, "exec_time",  log_tmp_str, GDT_Float32 ); // STATS
		return 0;
	}

	//------------------------------------------------------------------------------------------------

	if ( Request == GetFile ){	
		clock_gettime(CLOCK_REALTIME, &time_before); ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Enter in GetFile");

		if ( head 		== NULL ) {  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: No images found ..."); GoodbyeMessage(info, "No images found"); return 404; }
		if ( head->file 	== NULL ) {  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: No images found ..."); GoodbyeMessage(info, "No images found"); return 404; }
		if ( head->file[0] 	== '\0' ) {  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: No images found ..."); GoodbyeMessage(info, "No images found"); return 404; }
		if ( hit_number 	>  1    ) {  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: More than one image found ..."); GoodbyeMessage(info, "More than one image found"); return 404; }

		// ap_set_content_type(r, "text/xml");
	
        	if ( ( info->ENVIRONMENT_VARIABLE[0] != NULL ) && ( info->ENVIRONMENT_VARIABLE[1] != NULL ) )
			for ( i = 0; i < MAX_D; i++) if ( ( info->ENVIRONMENT_VARIABLE[0][i] != NULL ) && ( info->ENVIRONMENT_VARIABLE[1][i] != NULL ) ) setenv(info->ENVIRONMENT_VARIABLE[0][i], info->ENVIRONMENT_VARIABLE[1][i], 1);


		if ( output_name != NULL ) { bzero(final_path, MAX_PATH_LEN - 1); sprintf(final_path, "attachment; filename=\"%s\"", output_name ); apr_table_add(r->headers_out, "Content-disposition", final_path); }
		VSILFILE* fp = NULL;

		
		if ( head->vrt != NULL ) { 
			bzero(final_path, MAX_PATH_LEN - 1); snprintf(final_path, MAX_PATH_LEN, "/vsimem/live_long_and_prosper-%d.vrt", r->connection->client_addr->port );
			bzero(imgs_path,  MAX_PATH_LEN - 1); snprintf(imgs_path,  MAX_PATH_LEN, "/vsimem/live_long_and_prosper-%d.tif", r->connection->client_addr->port );
	       		fp = VSIFileFromMemBuffer(final_path, head->vrt, strlen( head->vrt ), FALSE ); VSIFCloseL(fp);
	
			if ( ( hDstDS = GDALOpen(final_path, GA_ReadOnly )) == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: GDALOpen error on VRT from memory %s %s", final_path, CPLGetLastErrorMsg() ); return 500;  }
			papszOptions = (char **)CSLSetNameValue( papszOptions, "COMPRESS", "LZW" );
	        	realImg = GDALCreateCopy( hOutDriver, imgs_path, hDstDS, FALSE, papszOptions, NULL, NULL ); GDALClose(realImg); GDALClose(hDstDS); VSIUnlink(final_path);
			fp = VSIFOpenL(imgs_path, "rb");

		} else fp = VSIFOpenL(head->file, "rb"); if (fp == NULL) {  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: No images found, unable to open \"%s\" ...", head->file ); GoodbyeMessage(info, "No images found"); return 404; }



		i = 1024 * 1024; // 1 MByte Buffer
		realRaster = malloc(i); 
		size_t bytes_read;
		while( ! VSIFEofL(fp) ){
			bytes_read = VSIFReadL(realRaster, 1, i, fp);
			ap_rwrite( (const void *)realRaster, bytes_read, r );
		}

		VSIFCloseL(fp);

		clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;		
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Finish in GetFile in %.3f sec, Total Time %.3f", 
				(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
		clock_gettime(CLOCK_REALTIME, &time_before);

		bzero(log_tmp_str, MAX_STR_LEN - 1); sprintf(log_tmp_str, "%f", time_total); addStatToPush(info, "exec_time",  log_tmp_str, GDT_Float32 ); // STATS
		return 0;

	}




	//------------------------------------------------------------------------------------------------
	// Check if the memory limit is set but no catalog is used
        if ( L_MaxMemoryUse > 0 ) {
		int 		mem_info	 = 0;
		int 		mem_proc	 = 0;
		unsigned long 	wcs_memory_info  = 0;
		unsigned long 	wcs_memory_sys   = 0;
		struct sysinfo 	myinfo; 
		unsigned long	memory_needed;
		sem_t		*sem_id;

		// Create a semaphore and waiting
		sem_id = sem_open(SEM_PATH, O_CREAT, S_IRUSR | S_IWUSR, 1);
		sem_wait(sem_id);

		// Open common memory usage
		mem_info = shm_open(SHMOBJ_PATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
		if ( ftruncate(mem_info, sizeof(unsigned long) )  != 0 ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: ftruncate for MaxMemoryUse without Database!" ); sem_post(sem_id); sem_close(sem_id); sem_unlink(SEM_PATH); return 500; }
		memcpy( &wcs_memory_info,  mmap(NULL, sizeof(unsigned long), PROT_READ|PROT_WRITE, MAP_SHARED, mem_info, 0), sizeof(unsigned long) );

		// Init if it's zero
		sysinfo(&myinfo);
		if ( wcs_memory_info <= 0 ) { wcs_memory_info = ( L_MaxMemoryUse > 0 ) ? (unsigned long )L_MaxMemoryUse : (unsigned long)myinfo.mem_unit * myinfo.freeram; }

		// Open local memory usage 
		bzero(imgs_path, MAX_STR_LEN - 1); sprintf(imgs_path, "%s_%d", SHMOBJ_PATH,  r->connection->client_addr->port );
		mem_proc = shm_open(imgs_path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
		if (  ftruncate(mem_proc, sizeof(unsigned long) ) != 0 ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: ftruncate for MaxMemoryUse without Database!" ); sem_post(sem_id); sem_close(sem_id); sem_unlink(SEM_PATH); return 500; }

		// Calculate memory needed for this processing
		memory_needed = (unsigned long)( pxAOISizeX * pxAOISizeY  * hit_number * 8.0 * 2.0 * MemoryCalcFactor + ( math_memory_factor * pxAOISizeX * pxAOISizeY  * 8 ) );

		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Free Memory available: %.2f MByte, processing needed %.2f MByte ...", (double)wcs_memory_info / 1024.0 / 1024.0, (double)memory_needed / 1024.0 / 1024.0 );
		wcs_memory_sys = ( L_MaxMemoryUse > 0 ) ? (unsigned long )L_MaxMemoryUse : (unsigned long)( myinfo.mem_unit * myinfo.totalram );

		// This query is possibile?
		if ( memory_needed >= wcs_memory_sys ) { // FUCK NO!
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Impossibile Query!  Memory requested for this operation is insane for this system!"); 
			close(mem_info); 	close(mem_proc); 				// Close the memories
			sem_post(sem_id);	sem_close(sem_id);	sem_unlink(SEM_PATH); 	// Release the semaphore
			GoodbyeMessage(info, "Impossibile Query!  Memory requested for this operation is insane for this system!"); return 413;
		}

		// We have enough memory?
		if ( memory_needed >= wcs_memory_info ) { // FUCK NO!
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Memory requested for this operation is too huge!"); 
			close(mem_info); 	close(mem_proc); 				// Close the memories
			sem_post(sem_id);	sem_close(sem_id);	sem_unlink(SEM_PATH); 	// Release the semaphore
			GoodbyeMessage(info, "Memory requested for this operation is too huge!"); return 429;
		}

		// Check if the memory is used from another process
		if (( L_MaxMemoryUse > 0 ) && ( memory_needed >= (unsigned long)myinfo.mem_unit * myinfo.freeram ) ) {
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Memory requested for this operation could be satisfied, but physical memory is not enough."); 
			close(mem_info); 	close(mem_proc); 				// Close the memories
			sem_post(sem_id);	sem_close(sem_id);	sem_unlink(SEM_PATH); 	// Release the semaphore
			GoodbyeMessage(info, "Memory requested for this operation could be satisfied, but physical memory is not enough.");
			return 413;
		}

		// Update total memory and close
		wcs_memory_info -= memory_needed;
		memcpy( mmap(NULL, sizeof(unsigned long), PROT_READ|PROT_WRITE, MAP_SHARED, mem_info, 0), &wcs_memory_info, sizeof(unsigned long) ); 
		close(mem_info);

		// Update local memory usage and close
		memcpy( mmap(NULL, sizeof(unsigned long), PROT_READ|PROT_WRITE, MAP_SHARED, mem_proc, 0), &memory_needed,   sizeof(unsigned long) ); 
		close(mem_proc);

		// Release lock on semaphore	
		sem_post(sem_id);
		sem_close(sem_id);
		sem_unlink(SEM_PATH);
	}

	if ( FILTER == FALSE ) ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Disabled empty image filter ...");

	// Cleaning before 
	for( cursor = head; cursor != NULL; cursor = cursor->next){
		if (cursor->file[0] == '\0' ){
			tmp = cursor;
			if 	( cursor 	== head )	{ head			= cursor->next; if (head != NULL) 	head->prev   		= NULL;		}
			else if ( cursor->next 	== NULL )	{ cursor		= cursor->prev;				cursor->next 		= NULL;		}
			else 					{ cursor->next->prev 	= cursor->prev;				cursor->prev->next 	= cursor->next;	}
			if ( head == NULL ) { hit_number = 0; break; }
			free(tmp); hit_number--;
		}
	}

	//------------------------------------------------------------------------------------------------
	if ( ( head != NULL ) && ( head->shp_head != NULL ) ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Data from shapefile, force to filter FALSE ..."); FILTER = FALSE; 	}


	//------------------------------------------------------------------------------------------------
	// From here start GetCoverage
	//------------------------------------------------------------------------------------------------
	clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;	
	ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Ready to start GetCoverage of %d dataset using %d threads in %.3f sec, Total Time %.3f", hit_number, MAX_THREAD,
			(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
	clock_gettime(CLOCK_REALTIME, &time_before);

	int hungry 	= FALSE;
	int 		*status		 = malloc(sizeof(int)       	  * MAX_THREAD );
	pthread_t  	*thread_mule 	 = malloc(sizeof(pthread_t) 	  * MAX_THREAD ); 
	int		*thread_mule_use = malloc(sizeof(int)       	  * MAX_THREAD ); for( i = 0; i < MAX_THREAD; i++) { thread_mule[i] = 0; thread_mule_use[i] = FALSE; }
	struct loadmule	*packmule 	 = malloc(sizeof(struct loadmule) * MAX_THREAD ); for( i = 0; i < MAX_THREAD; i++) {	
									// Copy common infos to struct mule job
									packmule[i].id		= i;
									packmule[i].info	= info;
									packmule[i].x_input_ul	= x_input_ul;
									packmule[i].y_input_ul	= y_input_ul;
									packmule[i].x_input_lr	= x_input_lr;
									packmule[i].y_input_lr	= y_input_lr;
									packmule[i].epgs_out	= epgs_out;
									packmule[i].GeoX_ul	= GeoX_ul;
									packmule[i].GeoY_ul	= GeoY_ul;
									packmule[i].GeoX_lr	= GeoX_lr;
									packmule[i].GeoY_lr	= GeoY_lr;
									packmule[i].pxAOISizeX	= pxAOISizeX;
									packmule[i].pxAOISizeY	= pxAOISizeY;
									packmule[i].wktTargetSRS = (char *)malloc( strlen(wktTargetSRS) + 1 ); strcpy(packmule[i].wktTargetSRS, wktTargetSRS);
									packmule[i].FILTER	= FILTER;
									packmule[i].WMTS_MODE	= WMTS_MODE;
									packmule[i].subDataSet	= subDataSet;
									packmule[i].outType	= outType;
								}

//	pthread_mutex_init(&gate, NULL);
	for( cursor = head; cursor != NULL; ){
		for( i = 0; i < MAX_THREAD; i++) if ( thread_mule_use[i] == FALSE ) {
//			fprintf(stderr, "start %d\n", i); fflush(stderr);
			packmule[i].cursor = cursor; 	// assign block to thread
			thread_mule_use[i] = TRUE;	// slot in use
			hungry 		   = FALSE;
			if ( pthread_create(&(thread_mule[i]), NULL, imagepackmule, &(packmule[i]) ) != 0 ){ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: No threads for mule."); return 500; }
			cursor = cursor->next; if ( cursor == NULL ) break;
			
		}		
		while ( hungry == FALSE ) {
			for( i = 0; i < MAX_THREAD; i++) if ( thread_mule_use[i] == TRUE ) {
				if ( pthread_kill( thread_mule[i], 0 ) == 0 ) continue; // Process still running ?
				pthread_join(thread_mule[i], (void **) &(status[i])); thread_mule_use[i] = FALSE; thread_mule[i] = 0; // Join thread and free silot
				if( status[i] == 2 ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: System error during processing."); return 500; }
//				fprintf(stderr, "stop %d status %d\n", i, status ); fflush(stderr);
				hungry = TRUE;
			}
		}		
	}
	

	// Waiting last running thread
	for( i = 0; i < MAX_THREAD; i++) if ( thread_mule_use[i] == TRUE ) {
		pthread_join(thread_mule[i], (void **) &(status[i])); thread_mule_use[i] = FALSE;
		if( status[i] == 2 ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: System error during processing."); return 500; }
		
		//fprintf(stderr, "stop %d status %d\n", i, status ); fflush(stderr);
	}

	// aaaaaaaand free ..
	free(packmule); free(thread_mule); free(thread_mule_use); free(status);
//	pthread_mutex_destroy(&gate);

	for( cursor = head; cursor != NULL; cursor = cursor->next){
		if (cursor->file[0] == '\0' ){
			tmp = cursor;
			if 	( cursor 	== head )	{ head			= cursor->next; if (head != NULL) 	head->prev   		= NULL;		}
			else if ( cursor->next 	== NULL )	{ cursor		= cursor->prev;				cursor->next 		= NULL;		}
			else 					{ cursor->next->prev 	= cursor->prev;				cursor->prev->next 	= cursor->next;	}
			if ( head == NULL ) { hit_number = 0; break; }
			free(tmp); hit_number--;
		}
	}



	clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;	
	time_read = (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;
	ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Finish in GetCoverage Reading and Warping in %.3f sec, Total Time %.3f for %d datasets", 
			(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total, hit_number);
	clock_gettime(CLOCK_REALTIME, &time_before);


	bzero(log_tmp_str, MAX_STR_LEN - 1); sprintf(log_tmp_str, "%d", hit_number); addStatToPush(info, "accessed_prod",  log_tmp_str, GDT_Int32 ); // STATS

	if ( hit_number <= 0 ) {  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: No images found"); GoodbyeMessage(info, "No images found"); return 404; }
	//------------------------------------------------------------------------------------------------
	// Forcign nodata values for all dataset
	if ( ( NO_DATA_DEFINED == TRUE ) || ( SCALE_DEFINED == TRUE ) || ( OFFSET_DEFINED == TRUE ) ) {
		if ( NO_DATA_DEFINED == TRUE ) ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: NODATA user defined as %f ...", nodata_user_defined ); 
		if ( SCALE_DEFINED   == TRUE ) ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: SCALE user defined as %f ...",  scale_user_defined  ); 
		if ( OFFSET_DEFINED  == TRUE ) ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: OFFSET user defined as %f ...", offset_user_defined ); 

		for( cursor = head; cursor != NULL; cursor = cursor->next) for ( i = 0; i < cursor->nband; i++) {
		       	if ( NO_DATA_DEFINED == TRUE ) cursor->nodata[i] = nodata_user_defined;
			if ( SCALE_DEFINED   == TRUE ) cursor->scale[i]	 = scale_user_defined;
			if ( OFFSET_DEFINED  == TRUE ) cursor->offset[i] = offset_user_defined;
		}
	}

	if ( LogServerURL != NULL ) { 
		long int tot_log_size = 0;
		for( cursor = head; cursor != NULL; cursor = cursor->next) tot_log_size +=  ( cursor->sizeX * cursor->sizeY * cursor->nband * ( GDALGetDataTypeSize(cursor->type)/8 ) );
		bzero(log_tmp_str, MAX_STR_LEN - 1); sprintf(log_tmp_str, "%ld", tot_log_size); addStatToPush(info, "size",  log_tmp_str, GDT_Int32 ); // STATS
	}


	//------------------------------------------------------------------------------------------------

	if ( INPUT_GEOM == TRUE ){
                char   **papszRasterizeOptions  = NULL;
                int     nTargetBand             = 1;
                double  dfBurnValue             = 1.0;
                double  *mask                   = NULL;
                hMEMDstDS                       = GDALCreate( hMEMDriver, "", pxAOISizeX, pxAOISizeY,  1, GDT_Float64, NULL );
          //    papszRasterizeOptions           = CSLSetNameValue( papszRasterizeOptions, "ALL_TOUCHED", "TRUE" );
                mask                            = (double *)malloc(sizeof(double) * pxAOISizeX * pxAOISizeY ); if ( mask   == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc mask "); return 500; }
                raster                          = (double *)malloc(sizeof(double) * pxAOISizeX * pxAOISizeY ); if ( raster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc mask "); return 500; }
                adfGeoTransform[0]              = GeoX_ul;
                adfGeoTransform[3]              = GeoY_ul;
                adfGeoTransform[1]              = x_min_res;
                adfGeoTransform[5]              = y_min_res;
                adfGeoTransform[2]              = 0.0;
                adfGeoTransform[4]              = 0.0;

                if ( epgs_out != atoi( OSRGetAttrValue (OGR_G_GetSpatialReference(hGeom) , "AUTHORITY", 1) ) ){
                        OGRSpatialReferenceH geoSRS = OSRNewSpatialReference(NULL); 
			#if GDAL_VERSION >= 304
			OSRSetAxisMappingStrategy(geoSRS, OAMS_TRADITIONAL_GIS_ORDER); // GDAL 3.0 Fix
			#endif
                        if ( ImportFromEPSG(&geoSRS, epgs_out ) != OGRERR_NONE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: OSRImportFromEPSG geometry proj incorrect %d",   epgs_out); return 500; }
                        if ( OGR_G_TransformTo(hGeom, geoSRS)   != OGRERR_NONE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: OGR_G_TransformTo to %d",                        epgs_out); return 500; }
			OSRDestroySpatialReference(geoSRS);
			// char *buff;
			// OGR_G_ExportToWkt(hGeom, &buff);
			// fprintf(stderr, "%s\n", buff);
                }

                GDALSetGeoTransform(hMEMDstDS, adfGeoTransform);

		if ( GDALRasterizeGeometries(hMEMDstDS, 1, &nTargetBand, 1, &hGeom, NULL, NULL, &dfBurnValue, papszRasterizeOptions, NULL, NULL) ) 
				{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: GDALRasterizeGeometries Fail!"); return 500; }
				
		hBandSrc = GDALGetRasterBand(hMEMDstDS, 1 ); err = GDALRasterIO( hBandSrc, GF_Read,  0, 0, pxAOISizeX, pxAOISizeY, mask, pxAOISizeX, pxAOISizeY, GDT_Float64, 0, 0);
		GDALClose(hMEMDstDS);

		
		for( cursor = head; cursor != NULL; cursor = cursor->next){	
			for ( i = 0; i < cursor->nband; i++){
				// Read
				hBandSrc = GDALGetRasterBand( cursor->dataset, i + 1); 
				err = GDALRasterIO( hBandSrc, GF_Read,  0, 0,  pxAOISizeX, pxAOISizeY, raster, pxAOISizeX, pxAOISizeY, GDT_Float64, 0, 0);
				// Mask
				for ( j = 0; j < ( pxAOISizeX * pxAOISizeY ); j++ ) if ( ( raster[j] != cursor->nodata[i] ) && ( mask[j] == 0.0 ) ) raster[j] = cursor->nodata[i];

				// Write
				err = GDALRasterIO( hBandSrc, GF_Write, 0, 0,  pxAOISizeX, pxAOISizeY, raster, pxAOISizeX, pxAOISizeY, GDT_Float64, 0, 0);
			}	
		}

		free(mask); free(raster);

		clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;	
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Finish Clipping data using geometry in %.3f sec, Total Time %.3f", 
			(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
		clock_gettime(CLOCK_REALTIME, &time_before);
	
	}

	//------------------------------------------------------------------------------------------------
	// here we have the huge step in the wcs future
	/*
	if ( python_script != NULL ) {
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Starting Python processing section ..." ); 

		i = PythonRun(python_script, info, prod, head, &hit_number, pxAOISizeX, pxAOISizeY, math_method);

		if ( i != 0 ) {
			cleanList(head, r);		
			clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;		
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Something went wrong in %.3f sec, Total Time %.3f sec",
					(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
			clock_gettime(CLOCK_REALTIME, &time_before);
			bzero(log_tmp_str, MAX_STR_LEN - 1); sprintf(log_tmp_str, "%f", time_total); addStatToPush(info, "exec_time",  log_tmp_str, GDT_Float32 ); // STATS
			return i;
		}

		clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;	
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Finish Python processing in %.3f sec, Total Time %.3f", 
			(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
		clock_gettime(CLOCK_REALTIME, &time_before);
	}
	*/
	//------------------------------------------------------------------------------------------------
	// Adding Virtual Dataset multi band ...
	// for ( cvlist = hvlist; cvlist != NULL; cvlist = cvlist->next){ fprintf(stderr, "%ld\n", cvlist->time); fflush(stderr);	} 


	// Extract times sort and remove duplicate ... so boring

	// Count how many collection are involved
	for (i_prod = 0; prod_path_array[i_prod] != NULL; i_prod++);

	if ( 	( mathChain == NULL ) && ( math_method == SINGLE  ) ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: DataSets forced to strategy squeeze, not math involved ..."); math_method = SQUEEZE; }
	else if ( mathChain != NULL ) {
		if ( math_method == SINGLE  ) ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: DataSets strategy single ..."); 
		if ( math_method == SQUEEZE ) ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: DataSets strategy squeeze ..."); 
	}

	if ( ( math_method == SINGLE ) || (
		( 	( ( ( x_input_ul      == x_input_lr ) && ( x_input_ul      == x_input_lr ) ) || ( i_prod == 1 ) ) && // Pixel history case avoid to merge dataset found or only one collection used for math
			( Y_RANGE_DEFINED == TRUE  	)	&& 
			( Y_RANGE_DEFINED == TRUE  	)	&&
			( ( outType == XML ) || ( outType == JSON ) ) ) ) ) {


		time_serie = (time_t *)malloc(sizeof(time_t) * hit_number);
		for( cursor = head, i = 0; cursor != NULL; cursor = cursor->next, i++) time_serie[i] = cursor->time;
		qsort(time_serie, hit_number, sizeof(time_t), timecmpfunc);
		for (i = 0, j = 0; i < hit_number; i++) if( time_serie[i] != time_serie[j] ) { j++; time_serie[j] = time_serie[i]; }
		for (i = j + 1	 ; i < hit_number; i++) time_serie[i] = -1;

	} else for (i_prod = 0, i = 0; prod_path_array[i_prod] != NULL; i_prod++) {
		for ( cvlist = hvlist; cvlist != NULL; cvlist = cvlist->next){

			gotcha = FALSE; tmp = NULL; raster = NULL; avg = NULL; hit_for_avg = NULL; char imgs_path_search[MAX_PATH_LEN];
			for( cursor = head; cursor != NULL; cursor = cursor->next){
				if    ( cursor->datasetId != NULL )	{ 													if ( strcmp(cvlist->name, cursor->datasetId)   ) continue; }
				else					{ bzero(imgs_path_search, MAX_PATH_LEN - 1); sprintf(imgs_path_search, "/%s/",  cvlist->name ); 	if ( ! strstr(cursor->file, imgs_path_search ) ) continue; }

				// if ( ! strstr(cursor->file, cvlist->name ) ) continue;
				// Init mergin with first element found
				if ( gotcha == FALSE ){
					tmp 			= cursor; // Get element reference for next steps
					if ( ( raster 		= (double *)malloc(sizeof(double) * pxAOISizeX * pxAOISizeY) )		  	== NULL ) 
															{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to create final output raster"); 		return 500; }
					if ( ( avg 		= (double *)malloc(sizeof(double) * pxAOISizeX * pxAOISizeY * cursor->nband))	== NULL ) 
															{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to create final output avg");  		return 500; }
					if ( ( hit_for_avg	= (int 	  *)malloc(sizeof(int)    * pxAOISizeX * pxAOISizeY * cursor->nband))   == NULL ) 
															{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to create final output hit_for_avg"); 	return 500; }

					for ( i = 0;  i < cursor->nband; i++){
						hBandSrc = GDALGetRasterBand( cursor->dataset, i + 1); 
						err = GDALRasterIO( hBandSrc, GF_Read,  0, 0,  pxAOISizeX, pxAOISizeY, &(avg[pxAOISizeX * pxAOISizeY * i]), pxAOISizeX, pxAOISizeY, GDT_Float64, 0, 0);
					}	
					for ( i = 0; i < ( pxAOISizeX * pxAOISizeY * cursor->nband ); i++ ) hit_for_avg[i] = 1;

					gotcha = TRUE;	
					continue;

				}
					
				// Summing other element
				for ( k = 0; k < cursor->nband; k++){
					hBandSrc = GDALGetRasterBand( cursor->dataset, k + 1);
					err = GDALRasterIO( hBandSrc, GF_Read,  0, 0,  pxAOISizeX, pxAOISizeY, raster, pxAOISizeX, pxAOISizeY, GDT_Float64, 0, 0);

					i = MergingFunc(r, MERGING_IMAGE, pxAOISizeX * pxAOISizeY, k, cursor, raster, avg, hit_for_avg); if ( i != 0 ) return i;

				}

			}
			
			if ( ( gotcha == TRUE ) && ( tmp != NULL ) && ( avg != NULL ) ){
				// Do avarage
				for (i = 0; i < ( pxAOISizeX * pxAOISizeY * tmp->nband ); i++ ) if ( avg[i] != tmp->nodata[ i / ( pxAOISizeX * pxAOISizeY ) ] ) avg[i] = avg[i] / (double)hit_for_avg[i];

				// List of new merged dataset			
				if ( vrt_head == NULL ) { vrt_head = vrt_cursor = (block)malloc(sizeof(struct sblock)); vrt_cursor->prev 	= NULL; }
				else			{ vrt_cursor->next 	= (block)malloc(sizeof(struct sblock)); vrt_cursor->next->prev 	= vrt_cursor; vrt_cursor = vrt_cursor->next; }

				// Copy the reference block to get all information
				initBlock(vrt_cursor); memcpy(vrt_cursor, tmp, sizeof(struct sblock));
				vrt_cursor->next = NULL;
				bzero( vrt_cursor->file, MAX_PATH_LEN - 1); sprintf(vrt_cursor->file, "%s_%s_MULTIBAND.tif", cvlist->name, basename( prod_path_array[i_prod] ) );

				// Crate brandnew dataset	
				if ( ( vrt_cursor->dataset = GDALCreate(hMEMDriver,"", pxAOISizeX, pxAOISizeY, vrt_cursor->nband, vrt_cursor->type, NULL ) ) == NULL )
						{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to create final output vrt_cursor"); return 500; }
				GDALSetGeoTransform(  	vrt_cursor->dataset, adfGeoTransform);
				GDALSetProjection(	vrt_cursor->dataset, wktTargetSRS);

	
				// Write output to DST dataset
				for ( i = 0; i < vrt_cursor->nband; i++){
					hBandSrc = GDALGetRasterBand( vrt_cursor->dataset, i + 1);
					GDALSetRasterNoDataValue(hBandSrc, vrt_cursor->nodata[i]);	 
					GDALSetRasterScale      (hBandSrc, vrt_cursor->scale[i] );
					GDALSetRasterOffset     (hBandSrc, vrt_cursor->offset[i]);

				
					bzero(imgs_path, MAX_PATH_LEN - 1); sprintf(imgs_path, "%s band=%d", cvlist->name, vrt_cursor->tband[i]+1);		
					GDALSetMetadataItem	(hBandSrc, "collection", imgs_path, NULL);
					err = GDALRasterIO( hBandSrc, GF_Write,  0, 0,  pxAOISizeX, pxAOISizeY, &(avg[pxAOISizeX * pxAOISizeY * i]), pxAOISizeX, pxAOISizeY, GDT_Float64, 0, 0);
				}
			}

			// Clean temp raster 
			if ( avg != NULL ) free(avg); if (raster != NULL ) free(raster); if (hit_for_avg != NULL ) free(hit_for_avg); tmp = NULL; raster = NULL; avg = NULL;
		}
	}

	// Switch list in case exists
	if ( ( vrt_head != NULL ) && ( mathChain == NULL ) ){
		// Cleaning memory
		for( cursor = head; cursor != NULL; cursor = cursor->next) GDALClose(cursor->dataset); 
		while ((cursor = head) != NULL) { head = head->next; free(cursor); }
	

		type 	= vrt_head->type;
		nband 	= 0;
		for( vrt_cursor = vrt_head; vrt_cursor != NULL; vrt_cursor = vrt_cursor->next) {
			// If we have different type I use GDT_Float32
			if ( vrt_head->type != vrt_cursor->type ) { 
				if ( ( outType == IMAGE ) && ( ! strcmp(outFormat, "JP2OpenJPEG") ) ) 	type = GDT_Int16; // Added special case for JP2OpenJPEG
				else									type = GDT_Float32;
			}
			nband += vrt_cursor->nband;
		}

		if ( hvorder != NULL ) { nband = 0; for ( cvorder = hvorder; cvorder != NULL; cvorder = cvorder->next) nband += cvorder->nband; }

		head 			= (block)malloc(sizeof(struct sblock)); initBlock(head);
		head->prev = head->next = NULL;
		head->type		= type;
		head->nband 		= nband;
		head->tband		= (int *)   malloc(sizeof(int) 	  * head->nband); 		
		head->nodata 		= (double *)malloc(sizeof(double) * head->nband); 	
		head->scale  		= (double *)malloc(sizeof(double) * head->nband);		
		head->offset 		= (double *)malloc(sizeof(double) * head->nband);	for (int i = 0; i < head->nband; i++) { head->nodata[i] = 0.0; head->scale[i] = 1.0; head->offset[i] = 0.0; head->tband[i] = i; }
		bzero( head->file, MAX_PATH_LEN - 1); strcpy(head->file, vrt_head->file);

		if ( ( head->dataset 	= GDALCreate(hMEMDriver,"", pxAOISizeX, pxAOISizeY, nband, type, NULL ) ) == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to create final from virtual output head"); 	return 500; }
		if ( ( raster		= (double *)malloc(sizeof(double) * pxAOISizeX * pxAOISizeY) )		  == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to create final output raster");		return 500; }


		if ( hvorder != NULL ) for ( cvorder = hvorder, k = 0; cvorder != NULL; cvorder = cvorder->next){
			for ( vrt_cursor = vrt_head; vrt_cursor != NULL; vrt_cursor = vrt_cursor->next) {
				if ( ! strstr(vrt_cursor->file, cvorder->name ) ) continue;
				const char *name = NULL;

				if   ( vrt_cursor->nband == 1 ){ hBandSrc = GDALGetRasterBand( vrt_cursor->dataset, 1);  name = GDALGetMetadataItem(hBandSrc, "collection",      NULL); i = 0; }
			       	else {
					bzero(imgs_path, MAX_PATH_LEN - 1); sprintf(imgs_path, "%s band=%d", cvorder->name, cvorder->tband[0]+1);
					for ( i = 0; i < vrt_cursor->nband; i++){ 
						hBandSrc = GDALGetRasterBand( vrt_cursor->dataset, i + 1);  name = GDALGetMetadataItem(hBandSrc, "collection",      NULL); 
						if ( ! strcmp(imgs_path, name) ) break; 
					}
				}

				
				err = GDALRasterIO( hBandSrc, GF_Read,  0, 0,  pxAOISizeX, pxAOISizeY, raster, pxAOISizeX, pxAOISizeY, GDT_Float64, 0, 0);


				hBandSrc = GDALGetRasterBand( head->dataset, 	   k + 1);	   GDALSetMetadataItem(hBandSrc, "collection", name, NULL);	
				err = GDALRasterIO( hBandSrc, GF_Write, 0, 0,  pxAOISizeX, pxAOISizeY, raster, pxAOISizeX, pxAOISizeY, GDT_Float64, 0, 0);

				GDALSetRasterNoDataValue(hBandSrc, vrt_cursor->nodata[i]);	 
				GDALSetRasterScale      (hBandSrc, vrt_cursor->scale[i] );
				GDALSetRasterOffset     (hBandSrc, vrt_cursor->offset[i]);

				head->nodata[k] = vrt_cursor->nodata[i];
				head->scale[k]  = vrt_cursor->scale[i];
				head->offset[k] = vrt_cursor->offset[i];
				
				k++; break;
						
			}
		} else for ( vrt_cursor = vrt_head, k = 0; vrt_cursor != NULL; vrt_cursor = vrt_cursor->next) {
			for ( i = 0; i < vrt_cursor->nband; i++, k++){
				const char *name = NULL;
				hBandSrc = GDALGetRasterBand( vrt_cursor->dataset, i + 1);  name = GDALGetMetadataItem(hBandSrc, "collection",      NULL); 	
				err = GDALRasterIO( hBandSrc, GF_Read,  0, 0,  pxAOISizeX, pxAOISizeY, raster, pxAOISizeX, pxAOISizeY, GDT_Float64, 0, 0);
				
				hBandSrc = GDALGetRasterBand( head->dataset, 	   k + 1);	   GDALSetMetadataItem(hBandSrc, "collection", name, NULL);	
				err = GDALRasterIO( hBandSrc, GF_Write, 0, 0,  pxAOISizeX, pxAOISizeY, raster, pxAOISizeX, pxAOISizeY, GDT_Float64, 0, 0);
	
				GDALSetRasterNoDataValue(hBandSrc, vrt_cursor->nodata[i]);	 
				GDALSetRasterScale      (hBandSrc, vrt_cursor->scale[i] );
				GDALSetRasterOffset     (hBandSrc, vrt_cursor->offset[i]);

				head->nodata[k] = vrt_cursor->nodata[i];
				head->scale[k]  = vrt_cursor->scale[i];
				head->offset[k] = vrt_cursor->offset[i];
			}			
		}
				
		free(raster); raster = NULL;
		for( vrt_cursor = vrt_head; vrt_cursor != NULL; vrt_cursor = vrt_cursor->next) GDALClose(vrt_cursor->dataset); 
		while ((vrt_cursor = vrt_head) != NULL) { vrt_head = vrt_head->next; free(vrt_cursor); }
	
		clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;		
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Finish to Merging Vritual Collection (%d bands) in %.3f sec, Total Time %.3f", nband,
			(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
		clock_gettime(CLOCK_REALTIME, &time_before);

	
	} else { if ((time_serie == NULL ) && ( mathChain != NULL )) if (vrt_head != NULL ) head = vrt_head; }




	//------------------------------------------------------------------------------------------------
	
	if ( mathChain != NULL ){
		for ( mathcur = mathChain, nband = 0; mathcur != NULL; mathcur = mathcur->next ) nband++;

		if  (( layers != NULL) && ( layer_index > 0 ) ) nband = layer_index; // In this case the stupid user ar select band in multiple band math, so it can duplicate band
	
		if  ( nband <= 0 )  { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: No band found for math section"); GoodbyeMessage(info, "No band found for math section"); return 400; }

		vrt_head  		= (block)malloc(sizeof(struct sblock)); initBlock(vrt_head); memcpy(vrt_head, head,    sizeof(struct sblock));	
		vrt_head->Metadata	= NULL;
		vrt_head->MetadataCount	= 0;
		vrt_head->next 		= NULL;
		vrt_head->prev 		= NULL; 
		vrt_head->nband		= nband;
		vrt_head->type		= GDT_Float32;
		vrt_head->tband		= (int *)   malloc(sizeof(int) 	  * vrt_head->nband); 		
		vrt_head->nodata 	= (double *)malloc(sizeof(double) * vrt_head->nband); 	
		vrt_head->scale  	= (double *)malloc(sizeof(double) * vrt_head->nband);		
		vrt_head->offset 	= (double *)malloc(sizeof(double) * vrt_head->nband);	for (int i = 0; i < vrt_head->nband; i++) { 
													vrt_head->nodata[i] 	= head->nodata[0];
													vrt_head->scale[i] 	= head->scale[0];
													vrt_head->offset[i] 	= head->offset[0];
													vrt_head->tband[i] 	= i; }

		bzero( vrt_head->file, MAX_PATH_LEN - 1); sprintf(vrt_head->file, "RESULT_MATH.tif");
		// Crate brandnew dataset	
		if ( ( vrt_head->dataset = GDALCreate(hMEMDriver,"", pxAOISizeX, pxAOISizeY, vrt_head->nband, vrt_head->type, NULL ) ) == NULL ) 
				{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to create final output vrt_head"); return 500; }


		GDALSetGeoTransform(  	vrt_head->dataset, adfGeoTransform);
		GDALSetProjection(	vrt_head->dataset, wktTargetSRS);

		if ( time_serie == NULL ) { // if the data was merged only one time ad it's zero!
			time(&chronometer);
			time_serie  = (time_t *)malloc(sizeof(time_t) * 2 ); time_serie[0] = chronometer; time_serie[1] = -1;
			for( cursor = head; cursor != NULL; cursor = cursor->next) cursor->time = chronometer;
			hit_number = 1;
		} else {
			for( vrt_cursor = vrt_head, time_serie_index = 1; time_serie_index < hit_number && time_serie[time_serie_index] > 0 ; time_serie_index++ ) {
				vrt_cursor->next 	= (block)malloc(sizeof(struct sblock)); initBlock(vrt_cursor->next); memcpy(vrt_cursor->next, head, sizeof(struct sblock));
				vrt_cursor->next->prev 	= vrt_cursor;
				vrt_cursor 		= vrt_cursor->next;
				vrt_cursor->next 	= NULL;
				vrt_cursor->nband	= nband;
				vrt_cursor->tband	= (int *)   malloc(sizeof(int) 	  * vrt_cursor->nband); 		
				vrt_cursor->nodata 	= (double *)malloc(sizeof(double) * vrt_cursor->nband); 	
				vrt_cursor->scale  	= (double *)malloc(sizeof(double) * vrt_cursor->nband);		
				vrt_cursor->offset 	= (double *)malloc(sizeof(double) * vrt_cursor->nband);	for (int i = 0; i < vrt_cursor->nband; i++) { 
															vrt_cursor->nodata[i] = (  NO_DATA_DEFINED == TRUE ) ? nodata_user_defined : head->nodata[0];
															vrt_cursor->scale[i] = 1.0; vrt_cursor->offset[i] = 0.0; vrt_cursor->tband[i] = i; }
				vrt_cursor->type		= GDT_Float32;
				vrt_cursor->MetadataCount 	= 0; 
				vrt_cursor->Metadata		= NULL;

				bzero( vrt_cursor->file, MAX_PATH_LEN - 1); sprintf(vrt_cursor->file, "RESULT_MATH.tif");
				// Crate brandnew dataset	
				if ( ( vrt_cursor->dataset = GDALCreate(hMEMDriver,"", pxAOISizeX, pxAOISizeY, vrt_cursor->nband, vrt_cursor->type, NULL ) ) == NULL ) 
						{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to create final output vrt_cursor"); return 500; }
				GDALSetGeoTransform(  	vrt_cursor->dataset, adfGeoTransform);
				GDALSetProjection(	vrt_cursor->dataset, wktTargetSRS);
			
			}

		}

	}

	//------------------------------------------------------------------------------------------------

	if ( mathChain != NULL ) { // Init multithread math
		hungry = FALSE;
		status          = malloc(sizeof(int)             		  	* MAX_THREAD );
		pthread_t       *thread_math     = malloc(sizeof(pthread_t)       	* MAX_THREAD );
	        int             *thread_math_use = malloc(sizeof(int)             	* MAX_THREAD ); for( i = 0; i < MAX_THREAD; i++) { thread_math[i] = 0; thread_math_use[i] = FALSE; }
	        struct mathParamters *packmath   = malloc(sizeof(struct mathParamters) 	* MAX_THREAD ); for( i = 0; i < MAX_THREAD; i++) {
										packmath[i].info		= info; 
										packmath[i].pxAOISizeX		= pxAOISizeX;
										packmath[i].pxAOISizeY		= pxAOISizeY;
										packmath[i].time_serie		= time_serie;
										packmath[i].head		= head;
										packmath[i].layers		= layers;
										packmath[i].MERGING_IMAGE	= MERGING_IMAGE;
										packmath[i].layer_index		= layer_index;
									}

		for ( mathcur = mathChain, nband = 0; mathcur != NULL; mathcur = mathcur->next, nband++ ) { 
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Computation of Math \"%s\"", mathcur->coveragemath );
			for ( time_serie_index = 0, vrt_cursor = vrt_head; time_serie_index < hit_number && time_serie[time_serie_index] > 0 && vrt_cursor != NULL ;  ) {
				for( i = 0; i < MAX_THREAD; i++) if ( thread_math_use[i] == FALSE ) {
					packmath[i].mathcur		= mathcur;
					packmath[i].vrt_cursor		= vrt_cursor;                           
					packmath[i].time_serie_index	= time_serie_index;
					packmath[i].nband		= nband;
					hungry             		= FALSE;
					thread_math_use[i]		= TRUE;
					if ( pthread_create(&(thread_math[i]), NULL, MathComputationUnit, &(packmath[i]) ) != 0 ){ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: No threads for math."); return 500; }
					time_serie_index++, vrt_cursor = vrt_cursor->next;
					if ( time_serie_index >= hit_number   ) break;
					if ( time_serie[time_serie_index] < 0 ) break;
					if ( vrt_cursor == NULL )		break;	

				}

		                while ( hungry == FALSE ) {
		                        for( i = 0; i < MAX_THREAD; i++) if ( thread_math_use[i] == TRUE ) {
		                                if ( pthread_kill( thread_math[i], 0 ) == 0 ) continue; // Process still running ?
		                                pthread_join(thread_math[i], (void **) &(status[i])); thread_math_use[i] = FALSE; thread_math[i] = 0; // Join thread and free silot
		                                if( status[i] == 2 ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: System error during processing math."); return 500; }
		                                hungry = TRUE;
		                        }
		                }
			}
	        	// Waiting last running thread
	        	for( i = 0; i < MAX_THREAD; i++) if ( thread_math_use[i] == TRUE ) {
	        	        pthread_join(thread_math[i], (void **) &(status[i])); thread_math_use[i] = FALSE;
	        	        if( status[i] == 2 ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: System error during processing math."); return 500; }
	        	}
		}
	}
	
	if ( mathChain != NULL ){
		cleanList(head , r); // Clean
		head = vrt_head;
		
                clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Finish in GetCoverage Math Section in %.3f sec, Total Time %.3f", 
				(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
		clock_gettime(CLOCK_REALTIME, &time_before);


	} // coveragemath end



	//------------------------------------------------------------------------------------------------
	

	if ( Request == GetInfo ){	
		clock_gettime(CLOCK_REALTIME, &time_before); ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Enter in GetInfo");

		double dfMin	= 0.0;
		double dfMax 	= 0.0;
		double dfMean 	= 0.0;
		double dfStdDev = 0.0;

		json_object *jobj   = json_object_new_object();
		json_object *jdata  = json_object_new_array(); // Main data array

		for( cursor = head; cursor != NULL; cursor = cursor->next) {
			json_object *jcur   = json_object_new_object();
			json_object *jgeo   = json_object_new_array();
			json_object *jsize  = json_object_new_array();



			for (i = 0; i < 6; i++ ) json_object_array_add(jgeo, json_object_new_double(cursor->GeoTransform[i]));
			bzero( epsg_res_tmp, 255 ); sprintf(epsg_res_tmp, "EPSG:%d", cursor->epsg);

			json_object_array_add(jsize, json_object_new_int(cursor->nPixels));
                       	json_object_array_add(jsize, json_object_new_int(cursor->nLines));
                        json_object_array_add(jsize, json_object_new_int(cursor->nband));

			json_object_object_add(jcur, "time", 		json_object_new_int(cursor->time) ); 
			json_object_object_add(jcur, "proj",     	json_object_new_string(epsg_res_tmp));
			json_object_object_add(jcur, "geoTransform",    jgeo);
			json_object_object_add(jcur, "dimensions",      jsize);

			if ( ( cursor->gcps_cnt > 0 ) && ( cursor->gcps != NULL ) ) {
				json_object *jgcps  = json_object_new_array();

				for ( i = 0; i < cursor->gcps_cnt; i++){
					json_object *jgcp = json_object_new_object();
					json_object_object_add(jgcp, "P", 	json_object_new_double(cursor->gcps[i].dfGCPPixel));
					json_object_object_add(jgcp, "L", 	json_object_new_double(cursor->gcps[i].dfGCPLine));
					json_object_object_add(jgcp, "X", 	json_object_new_double(cursor->gcps[i].dfGCPX));
					json_object_object_add(jgcp, "Y", 	json_object_new_double(cursor->gcps[i].dfGCPY));
					json_object_object_add(jgcp, "Z", 	json_object_new_double(cursor->gcps[i].dfGCPZ));

					json_object_array_add(jgcps, jgcp);

				}

				json_object_object_add(jcur, "gcps", jgcps);	
			}
			
			if ( ( cursor->MetadataCount > 0 ) && ( cursor->Metadata != NULL ) ){
				json_object *jmetadata  = json_object_new_object(); 
				char            *pszKey = NULL;
				const char      *pszValue;
				for( i = 0; i <  cursor->MetadataCount ; i++ ){
					pszValue = (const char *)CPLParseNameValue( cursor->Metadata[i], &pszKey );
					json_object_object_add(jmetadata, pszKey, json_object_new_string(pszValue));
				} 
				json_object_object_add(jcur, "metadata", jmetadata);	
			}

			json_object *jbands  = json_object_new_array();
			for ( i = 0; i < cursor->nband;  i++){
				json_object *jband = json_object_new_object();

				hBandSrc = GDALGetRasterBand( cursor->dataset, 	i+1 ); GDALComputeRasterStatistics(hBandSrc, FALSE, &dfMin, &dfMax, &dfMean, &dfStdDev, NULL, NULL);
				json_object_object_add(jband, "min", 	json_object_new_double(dfMin));
				json_object_object_add(jband, "max", 	json_object_new_double(dfMax));
				json_object_object_add(jband, "mean", 	json_object_new_double(dfMean));
				json_object_object_add(jband, "stddev", json_object_new_double(dfMean));
				json_object_object_add(jband, "scale",  json_object_new_double(cursor->scale[i]));
				json_object_object_add(jband, "offset", json_object_new_double(cursor->offset[i]));
				json_object_object_add(jband, "nodata", json_object_new_double(cursor->nodata[i]));
				json_object_array_add(jbands, jband);

			} 

			json_object_object_add(jcur, "bands", jbands);
			json_object_array_add(jdata, jcur);
		}


		cleanList(head, r); // Clean

		json_object_object_add(jobj,	"prods", 	jdata);
		json_object_object_add(jobj, 	"time_search",  json_object_new_double(time_search));
		json_object_object_add(jobj, 	"time_read",    json_object_new_double(time_read));
		json_object_object_add(jobj, 	"time_total",   json_object_new_double( (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION + time_total ) );
					

		if ( output_name != NULL ) { bzero(final_path, MAX_PATH_LEN - 1); sprintf(final_path, "attachment; filename=\"%s\"", output_name ); apr_table_add(r->headers_out, "Content-disposition", final_path); }
		ap_set_content_type(r, "text/json");
		ap_rprintf(r, "%s", json_object_to_json_string(jobj));



                clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Finish in GetInfo in %.3f sec, Total Time %.3f", 
				(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
		clock_gettime(CLOCK_REALTIME, &time_before);

		bzero(log_tmp_str, MAX_STR_LEN - 1); snprintf(log_tmp_str, MAX_STR_LEN, "%f", time_total); addStatToPush(info, "exec_time",  log_tmp_str, GDT_Float32 ); // STATS
	
		return 0;
	}
	
	//------------------------------------------------------------------------------------------------
	bzero(log_tmp_str, MAX_STR_LEN - 1); snprintf(log_tmp_str, MAX_STR_LEN, "%d", pxAOISizeX *  pxAOISizeY * head->nband * ( GDALGetDataTypeSize(head->type)/8 ) ); addStatToPush(info, "size",  log_tmp_str, GDT_Int32 ); // STATS

	if ( outType == IMAGE ){
		// gdalinfo /vsicurl_streaming/"http://mwcs/wcs?service=WCS&Reques
		//------------------------------------------------------------------------------------------------------------------	
		// Preparation of output file and http header
		bzero(imgs_path,  MAX_PATH_LEN - 1);
		bzero(final_path, MAX_PATH_LEN - 1);
		bzero(buff, 99);

		if 		( ! strcmp(outFormat, "PNG") ) 		{ 
									sprintf(imgs_path, "/vsimem/%s.png", 				basename(head->file)); 
									outputType = GDT_Byte;
									sprintf(buff, "image/png"); ap_set_content_type(r, buff);
					
		} else if 	( ! strcmp(outFormat, "JPEG") ) 	{ 
									sprintf(imgs_path, "/vsimem/%s.jpg", 				basename(head->file)); 
									outputType = GDT_Byte;
									sprintf(buff, "image/jpeg"); ap_set_content_type(r, buff);
	
		} else if 	( ! strcmp(outFormat, "JP2OpenJPEG") )	{
									sprintf(imgs_path,  "/vsimem/%s.jp2",  				basename(head->file)); 
									sprintf(final_path, "attachment; filename=\"%s.jp2\"",  	basename(head->file));

									if ( head->type != GDT_Byte || head->type != GDT_Int16 || head->type != GDT_UInt16 ) outputType = GDT_Int16; 
									apr_table_add(r->headers_out, "Content-disposition", final_path);
									sprintf(buff, "video/jpm");  ap_set_content_type(r, buff);
									
		} else if 	( ! strcmp(outFormat, "XYZ") )		{
									sprintf(imgs_path,  "/vsimem/%s.txt",  				basename(head->file)); 
									sprintf(final_path, "attachment; filename=\"%s.txt\"",  	basename(head->file));
									apr_table_add(r->headers_out, "Content-disposition", final_path);
									sprintf(buff, "text/csv");  ap_set_content_type(r, buff);


		} else if 	( ! strcmp(outFormat, "ENVI") )		{
									sprintf(imgs_path,  "/vsimem/%s.img",  				remove_filename_ext( basename(head->file) )); 
									sprintf(final_path, "attachment; filename=\"%s.tar\"",  	remove_filename_ext( basename(head->file) ));
									apr_table_add(r->headers_out, "Content-disposition", final_path);
									sprintf(buff, "application/tar");  ap_set_content_type(r, buff);

		} else							{
									if ((  get_filename_ext(head->file) != NULL ) && ( ! strcmp( get_filename_ext(head->file), "tif") ) ) {
										sprintf(imgs_path, "/vsimem/%s", 			basename(head->file));
										sprintf(final_path, "attachment; filename=\"%s\"",  	basename(head->file));
									} else {
										sprintf(imgs_path, "/vsimem/%s.tif", 			basename(trim_gdal_path(head->file)));
										sprintf(final_path, "attachment; filename=\"%s.tif\"", 	basename(trim_gdal_path(head->file)));
									}

									apr_table_add(r->headers_out, "Content-disposition", final_path);
									sprintf(buff, "image/tiff");  ap_set_content_type(r, buff);
					if ( COMPRESSION == TRUE )	papszOptions = (char **)CSLSetNameValue( papszOptions, "COMPRESS", "LZW" ); // Added compression
		}

		if ( output_name != NULL ) { bzero(final_path, MAX_PATH_LEN - 1); sprintf(final_path, "attachment; filename=\"%s\"", output_name ); apr_table_add(r->headers_out, "Content-disposition", final_path); }

		//------------------------------------------------------------------------------------------------------------------	

		// Create a output defined using the first dataset
		if ( ( hDstDS 		= GDALCreate( hMEMDriver, "", pxAOISizeX,  pxAOISizeY, head->nband, head->type,  NULL ) ) == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to create final output hDstDS"); 	return 500; }
		if ( ( raster 		= (double *)malloc(sizeof(double) * pxAOISizeX * pxAOISizeY) )				  == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to create final output raster"); 	return 500; }
		if ( ( avg 		= (double *)malloc(sizeof(double) * pxAOISizeX * pxAOISizeY * head->nband) )	   	  == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to create final output avg");    	return 500; }
		if ( ( hit_for_avg	= (int 	  *)malloc(sizeof(int)    * pxAOISizeX * pxAOISizeY * head->nband) )		  == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to create final output hit_for_avg");	return 500; }

		
		if ( ( info->tile_path == NULL )  && ( head->MetadataCount > 0 ) && ( head->Metadata != NULL ) ){ // If i have to put out a tile, metadata is uselss
			char            *pszKey = NULL;
			const char      *pszValue;
			for( i = 0; i <  head->MetadataCount ; i++ ) {
				if ( head->Metadata[i] 		== NULL ) continue; 
				if ( ( pszValue = (const char *)CPLParseNameValue( head->Metadata[i], &pszKey ) ) == NULL ) continue;
				if ( GDALSetMetadataItem(hDstDS, pszKey, pszValue, NULL) != CE_None ) ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "WARN: Unable to copy Metadata info %s = %s ... ", pszKey, pszValue );
			}
		}
		
		// Set georeference to Dst dataset
		for (i = 0; i < 6; i++ ) adfGeoTransform[i] = 0.0;
		adfGeoTransform[0] = lowerCorner[1];
		adfGeoTransform[3] = upperCorner[0];
		adfGeoTransform[1] = x_min_res;
		adfGeoTransform[5] = y_min_res;

		// Special case for no projection
		if ( head->epsg == 0 ){	
			adfGeoTransform[0] 	= 0.0; 	adfGeoTransform[1] 	= 1.0; 	adfGeoTransform[2] 	= 0.0;
			adfGeoTransform[3] 	= 0.0;	adfGeoTransform[4] 	= 0.0;	adfGeoTransform[5] 	= 1.0;
		}


		GDALSetGeoTransform(  	hDstDS, adfGeoTransform);
		GDALSetProjection(	hDstDS, wktTargetSRS);

		// init avg raster using the first dataset
		for ( i = 0;  i < head->nband; i++){
			hBandSrc = GDALGetRasterBand( head->dataset, i + 1); 
			err = GDALRasterIO( hBandSrc, GF_Read,  0, 0,  pxAOISizeX, pxAOISizeY, &(avg[pxAOISizeX * pxAOISizeY * i]), pxAOISizeX, pxAOISizeY, GDT_Float64, 0, 0);
		}
		for ( i = 0; i < ( pxAOISizeX * pxAOISizeY * head->nband ); i++ ) hit_for_avg[i] = 1;

		// Sum over avg all other datasets
		for( cursor = head->next; cursor != NULL; cursor = cursor->next) {
			for ( k = 0; k < cursor->nband; k++){
				hBandSrc = GDALGetRasterBand( cursor->dataset, k + 1);
				err = GDALRasterIO( hBandSrc, GF_Read,  0, 0,  pxAOISizeX, pxAOISizeY, raster, pxAOISizeX, pxAOISizeY, GDT_Float64, 0, 0);

				i = MergingFunc(r, MERGING_IMAGE, pxAOISizeX * pxAOISizeY, k, cursor, raster, avg, hit_for_avg); if ( i != 0 ) return i;
			}	
		}

		for (i = 0; i < ( pxAOISizeX * pxAOISizeY * head->nband ); i++ ) if ( avg[i] != head->nodata[ i / ( pxAOISizeX * pxAOISizeY ) ] ) avg[i] = avg[i] / (double)hit_for_avg[i];

		// Write output to DST dataset
		for ( i = 0; i < head->nband; i++){
			const char *name = NULL;
			hBandSrc = GDALGetRasterBand( head->dataset, 	i + 1);	 		name =  GDALGetMetadataItem(hBandSrc, "collection", 	  NULL); 	
			hBandSrc = GDALGetRasterBand( hDstDS, 		i + 1);	if ( name != NULL ) 	GDALSetMetadataItem(hBandSrc, "collection", name, NULL);	

			err = GDALRasterIO( hBandSrc, GF_Write,  0, 0,  pxAOISizeX, pxAOISizeY, &(avg[pxAOISizeX * pxAOISizeY * i]), pxAOISizeX, pxAOISizeY, GDT_Float64, 0, 0);

			GDALSetRasterNoDataValue(hBandSrc, head->nodata[i]);	 
			GDALSetRasterScale      (hBandSrc, head->scale[i] );
			GDALSetRasterOffset     (hBandSrc, head->offset[i]);

		}

		// Cleaning memory
		// cleanList(head, r); free(avg); free(raster); free(hit_for_avg);

		//------------------------------------------------------------------------------------------------------------------

			
		if ( ( ! strcmp(outFormat, "PNG") || ( ! strcmp(outFormat, "JPEG") ) ) ) {
			if ( COLOR_RANGE_DEFINED == TRUE ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Color range defined from user: %f to %f", color_range_min, color_range_max ); }

			// In case of math avoid scale and offset, but keep nodata
			if ( mathChain != NULL ) for ( i = 0; i < head->nband; i++) { if ( SCALE_DEFINED == FALSE ) head->scale[i] = 1.0;  if ( OFFSET_DEFINED == FALSE ) head->offset[i] = 0.0; }

			ColorizeDataset(info, hDstDS, &hMEMSrcDS, colortable, prod_path_array[0], head->scale, head->offset, head->nodata, COLOR_RANGE_DEFINED, color_range_min, color_range_max, NODATA_AUTO, equaliz_sigma ); GDALClose(hDstDS); hDstDS = hMEMSrcDS;

			clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;
			//time(&chronometer); strftime (chronometer_str, 255, "%Y-%m-%d %H:%M:%S", localtime(&chronometer));
			
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Finish in %s creation %.3f sec, Total Time %.3f sec",outFormat,
					(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
			//time(&chronometer);
			clock_gettime(CLOCK_REALTIME, &time_before);

		}
		
		//------------------------------------------------------------------------------------------------------------------	

		realImg = GDALCreateCopy( hOutDriver, imgs_path, hDstDS, FALSE, papszOptions, NULL, NULL );
		if ( realImg == NULL ) {  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: GDALCreateCopy %s", imgs_path); fflush(stderr); return 500; }

		GDALClose(realImg);
		GDALClose(hDstDS);

		realRaster = VSIGetMemFileBuffer( imgs_path, &pnOutDataLength, TRUE ); if ( realRaster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: VSIGetMemFileBuffer"); return 500; }

		//----------------------------------------------------------------------
		// Apply Magic Filter

		if( magic_filter_func != NULL ) { i = ApplyMagicFilter(info, &realRaster, &pnOutDataLength, magic_filter_func, magic_filter_params ); if ( i != 0 ) { cleanList(head, r); return i; } }

		//----------------------------------------------------------------------
		// Adding a logo for a PNG
		if ( ( ! strcmp(outFormat, "PNG") ) && ( WaterMark != NULL ) ){

			const GByte	*blob	  = NULL;	 
			size_t		 blobSize = 0;
			
	
			MagickWandGenesis();
			MagickWand 	*mt   	 = NewMagickWand();
			MagickWand 	*logo 	 = NewMagickWand();
			MagickWand 	*water 	 = NULL; 


			MagickReadImageBlob( mt, realRaster, pnOutDataLength); 
			MagickReadImage( logo, WaterMark);
			water = MagickTextureImage(mt,logo );

			MagickSetImageFormat(water, "PNG");
			blob = MagickGetImagesBlob(water, &blobSize); 

			if ( blob == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to create PNG with logo"); cleanList(head, r); return 500; }
			if ( realRaster != NULL ) free(realRaster); realRaster = NULL;

			realRaster 	= (GByte *)malloc(blobSize); memcpy(realRaster, blob, blobSize ); if ( realRaster == NULL ){ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc realRaster "); return 500; }
			pnOutDataLength = blobSize;


			if(mt)   mt   = DestroyMagickWand(mt);
			if(logo) logo = DestroyMagickWand(logo);
			MagickWandTerminus();


		}

		//----------------------------------------------------------------------

		if ( ! strcmp(outFormat, "ENVI") ){
			// Add .img to tar
			tar_add(r, realRaster, pnOutDataLength, imgs_path);
			// Read header 
			sprintf(imgs_path,  "/vsimem/%s.hdr", remove_filename_ext(basename(head->file)) ); realRaster = VSIGetMemFileBuffer( imgs_path, &pnOutDataLength, TRUE ); 
			if ( realRaster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: VSIGetMemFileBuffer"); return 500; }

			// Add .hdr to tar
			tar_add(r, realRaster, pnOutDataLength, imgs_path);

		} else {
			i = ap_rwrite( (const void *)realRaster, pnOutDataLength, r ); // Send image to client
		}

		if ( ( WMTSCachePath != NULL ) && ( info->tile_path != NULL ) ) {
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: [WMTS] Cache enable, storing tile %s", info->tile_path); 	
			bzero(imgs_path,  MAX_PATH_LEN - 1); strcpy(imgs_path, info->tile_path ); 
			if ( mkdir_recursive(dirname(imgs_path)) == FALSE ) 				{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] Unable to create cache directory for %s !!", 	info->tile_path ); return 500; }
			FILE *tile_cache = fopen(info->tile_path, "wb");
			if ( tile_cache == NULL  ) 			    				{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] Unable to create cache file %s !!", 		info->tile_path ); return 500; }
			if ( fwrite( realRaster, pnOutDataLength, 1, tile_cache ) != 1 ) 		{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] Unable to write cache file %s !!",            	info->tile_path ); return 500; }
			if ( fclose(tile_cache)	!= 0 )							{ ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] Unable to close cache file %s !!",                 info->tile_path ); return 500; }
		}


		if ( ( info->tile_path != NULL ) && ( MEMC_memc != NULL ) ) {
			memcached_return rc;
			rc = memcached_set(MEMC_memc, info->tile_path, strlen(info->tile_path), realRaster, pnOutDataLength, (time_t)0, (uint32_t)0);	
			if (rc == MEMCACHED_SUCCESS) 	ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: [WMTS] Memcache enable, storing tile %s", 	info->tile_path); 	
			else				ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: [WMTS] Couldn't store in Memcache tile %s, %s", info->tile_path, memcached_strerror(MEMC_memc, rc) );
		
		}


		free(realRaster);
		cleanList(head, r);		

		clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;		
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Finish in GetCoverage Writeing (%dx%d) in %.3f sec, Total Time %.3f sec", pxAOISizeX, pxAOISizeY,
				(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
		clock_gettime(CLOCK_REALTIME, &time_before);

		bzero(log_tmp_str, MAX_STR_LEN - 1); sprintf(log_tmp_str, "%f", time_total); addStatToPush(info, "exec_time",  log_tmp_str, GDT_Float32 ); // STATS
		return 0;
	}


	//------------------------------------------------------------------------------------------------
	// Output in this case is a tar archive filled of all images founded

	if ( outType == TAR ){ 
		bzero(final_path, MAX_PATH_LEN - 1); 	sprintf(final_path, 	"attachment; filename=\"%s.tar\"", head->math_id); 	apr_table_add(r->headers_out, "Content-disposition", final_path);
		bzero(buff, 99); 			sprintf(buff, 		"application/tar");  					ap_set_content_type(r, buff);

		if ( output_name != NULL ) { bzero(final_path, MAX_PATH_LEN - 1); sprintf(final_path, "attachment; filename=\"%s\"", output_name ); apr_table_add(r->headers_out, "Content-disposition", final_path); }


		for (i = 0; i < 6; i++ ) adfGeoTransform[i] = 0.0;
		papszOptions 	= (char **)CSLSetNameValue( papszOptions, "COMPRESS", "LZW" ); // Added compression
		hOutDriver 	= GDALGetDriverByName("GTiff");
		double tot_size = 0.0;
		// Set georeference to Dst dataset
		adfGeoTransform[0] = lowerCorner[1];
		adfGeoTransform[3] = upperCorner[0];
		adfGeoTransform[1] = x_min_res;
		adfGeoTransform[5] = y_min_res;


		for( cursor = head, i = 0; cursor != NULL; cursor = cursor->next, i++) {
			bzero(imgs_path,  MAX_PATH_LEN - 1);

			if 	(( get_filename_ext(cursor->file) != NULL ) && ( ! strcmp( get_filename_ext(cursor->file), "tif") )) 	sprintf(imgs_path, "/vsimem/%03d_%s", 		i, basename(cursor->file));
			else 														sprintf(imgs_path, "/vsimem/%03d_%s.tif", 	i, basename(trim_gdal_path(cursor->file)));

			if 	(( get_filename_ext(cursor->file) != NULL ) && ( ! strcmp( get_filename_ext(cursor->file), "tif") )) 	sprintf(imgs_path, "/vsimem/%03d_%s", 		i, basename(cursor->file));
			else 														sprintf(imgs_path, "/vsimem/%03d_%s.tif", 	i, basename(trim_gdal_path(cursor->file)));

			GDALSetGeoTransform(  	cursor->dataset, adfGeoTransform);
			GDALSetProjection(	cursor->dataset, wktTargetSRS);
		
			realImg = GDALCreateCopy( hOutDriver, imgs_path, cursor->dataset, FALSE, papszOptions, NULL, NULL ); GDALClose(realImg); GDALClose(cursor->dataset); cursor->dataset = NULL;
			if ( realImg == NULL ) {  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r,  "ERROR: GDALCreateCopy %s", imgs_path); return 500; }

			realRaster 	= VSIGetMemFileBuffer( imgs_path, &pnOutDataLength, TRUE ); if ( realRaster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: VSIGetMemFileBuffer"); return 500; }
			tar_add(r, realRaster, pnOutDataLength, imgs_path);
			tot_size += (double)pnOutDataLength;
		}


		cleanList(head, r);

		clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;		
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Finish in GetCoverage Writeing TAR (~%.2fMbyte) archive in %.3f sec, Total Time %.3f", tot_size / 1024 / 1024,
				(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
		clock_gettime(CLOCK_REALTIME, &time_before);
		bzero(log_tmp_str, MAX_STR_LEN - 1); sprintf(log_tmp_str, "%f", time_total); addStatToPush(info, "exec_time",  log_tmp_str, GDT_Float32 ); // STATS
		return 0;
	}

	if ( outType == MAGIC ){
		bzero(buff, 99);  sprintf(buff, "image/gif"); ap_set_content_type(r, buff);
		hOutDriver      = GDALGetDriverByName("JPEG");

		MagickWand 	*mw 	 = NULL;
		const GByte	*blob	 = NULL;	 
		size_t		blobSize = 0;
		double 		tot_size = 0.0;

		MagickWandGenesis();
		mw = NewMagickWand();
		
		for( cursor = head; cursor != NULL; cursor = cursor->next) {
			// fprintf(stderr, "%ld\n", cursor->time); fflush(stderr);
			bzero(imgs_path,  MAX_PATH_LEN - 1);
			if 	((get_filename_ext(cursor->file) != NULL ) && ( ! strcmp( get_filename_ext(cursor->file), "jpg") )) 	sprintf(imgs_path, "/vsimem/%s", 	basename(cursor->file));
			else 														sprintf(imgs_path, "/vsimem/%s.jpg", 	basename(trim_gdal_path(cursor->file)));

			ColorizeDataset(info, cursor->dataset, &hMEMSrcDS, colortable, prod_path_array[0], cursor->scale, cursor->offset, cursor->nodata, COLOR_RANGE_DEFINED, color_range_min, color_range_max, NODATA_AUTO, equaliz_sigma ); GDALClose(cursor->dataset); 
			cursor->dataset= NULL; 

			realImg = GDALCreateCopy( hOutDriver, imgs_path, hMEMSrcDS, FALSE, NULL, NULL, NULL ); GDALClose(realImg); GDALClose(hMEMSrcDS);
			if ( realImg == NULL ) {  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r,  "ERROR: GDALCreateCopy %s", imgs_path); return 500; }

			realRaster 	= VSIGetMemFileBuffer( imgs_path, &pnOutDataLength, TRUE ); if ( realRaster == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: VSIGetMemFileBuffer"); return 500; }

			MagickWand *mt = NewMagickWand();
			// Read image from memory
			MagickReadImageBlob( mt, realRaster, pnOutDataLength);
			MagickSetImageFormat(mt, "gif");

			MagickSetImageDelay(mt,100);
			MagickAddImage(mw,mt);
			mt = DestroyMagickWand(mt);

			tot_size += (double)pnOutDataLength;
		}

		cleanList(head, r);

		MagickSetImageFormat(mw, "gif");
		blob = MagickGetImagesBlob(mw, &blobSize);
		if ( blob == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to create GIF"); return 500; }

		i = ap_rwrite( (const void *)blob, blobSize, r );
		
		if(mw)   mw   = DestroyMagickWand(mw);
		MagickWandTerminus();

                clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;

		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Finish in GetCoverage Writeing GIF (~%.2fMbyte) in %.3f sec, Total Time %.3f", tot_size / 1024 / 1024,
				(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
		clock_gettime(CLOCK_REALTIME, &time_before);

		bzero(log_tmp_str, MAX_STR_LEN - 1); sprintf(log_tmp_str, "%f", time_total); addStatToPush(info, "exec_time",  log_tmp_str, GDT_Float32 ); // STATS
		return 0;

	}


	//------------------------------------------------------------------------------------------------
	// Merging the dataset with same time and same type and same number of bands
	raster   	= (double *)malloc(sizeof(double) * pxAOISizeX * pxAOISizeY); if ( raster 	== NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc raster"); 	return 500; }
	avg 	 	= (double *)malloc(sizeof(double) * pxAOISizeX * pxAOISizeY); if ( avg    	== NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc avg");    	return 500; }
	hit_for_avg 	= (int    *)malloc(sizeof(int)    * pxAOISizeX * pxAOISizeY); if ( hit_for_avg 	== NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: malloc hit_for_avg");   return 500; }
	for ( j = 0; j < ( pxAOISizeX * pxAOISizeY ) ; j++ )  hit_for_avg[i] = 2; // Init for avarage for two numbers

	if ( ( x_input_ul  != x_input_lr ) && ( x_input_ul != x_input_lr ) ) for( cursor = head; cursor != NULL; cursor = cursor->next) {
		if ( cursor->next 	== NULL ) 			break;
		if ( cursor->time 	!= cursor->next->time  )	continue;
		if ( cursor->type 	!= cursor->next->type  )	continue;
		if ( cursor->nband 	!= cursor->next->nband )	continue;

		for ( i = 0; i < cursor->nband;  i++){
			hBandSrc = GDALGetRasterBand( cursor->dataset, 		i+1 ); 	err = GDALRasterIO( hBandSrc, GF_Read,  0, 0,  pxAOISizeX, pxAOISizeY, avg, 	pxAOISizeX, pxAOISizeY, GDT_Float64, 	0, 0);
			hBandSrc = GDALGetRasterBand( cursor->next->dataset, 	i+1 );	err = GDALRasterIO( hBandSrc, GF_Read,  0, 0,  pxAOISizeX, pxAOISizeY, raster, 	pxAOISizeX, pxAOISizeY, GDT_Float64, 	0, 0);

			j = MergingFunc(r, MERGING_IMAGE, pxAOISizeX * pxAOISizeY, i, cursor, raster, avg, hit_for_avg); if ( j != 0 ) return j;

			for ( j = 0; j < ( pxAOISizeX * pxAOISizeY ); j++ ) if ( avg[i] != cursor->nodata[i] ) avg[j] = avg[j] / (double)hit_for_avg[j];
			hBandSrc = GDALGetRasterBand( cursor->dataset, i+1 ); err = GDALRasterIO( hBandSrc, GF_Write, 0, 0,  pxAOISizeX, pxAOISizeY, avg, pxAOISizeX, pxAOISizeY, GDT_Float64,  0, 0);

		}
						tmp 			= cursor->next;
						cursor->next 		= cursor->next->next;
		if ( cursor->next != NULL ) 	cursor->next->prev 	= cursor;
		GDALClose(tmp->dataset); tmp->dataset = NULL;	
		hit_number--;
	}



	//------------------------------------------------------------------------------------------------


	for( cursor = head, hit_number = 0; cursor != NULL; cursor = cursor->next, hit_number++){ if ( cursor->dataset == NULL ) continue;
		cursor->raster 		= (double **)malloc(sizeof(double *) * cursor->nband );   if ( cursor->raster  == NULL ) {  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r,  "ERROR: Malloc cursor->raster"); return 500; }
		cursor->type		= GDT_Float64;
		cursor->typeSize	= GDALGetDataTypeSize(cursor->type)/8;
		for (i = 0; i < cursor->nband; i++ ){
			cursor->raster[i] = NULL; if ( ( cursor->raster[i] = (double *)malloc(cursor->typeSize * pxAOISizeX * pxAOISizeY ) ) == NULL )  {  ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r,  "ERROR: Malloc cursor->raster[%d]", i); return 500; }
		       	hBandSrc = GDALGetRasterBand( cursor->dataset, i+1 ); err = GDALRasterIO( hBandSrc, GF_Read,  0, 0,  pxAOISizeX, pxAOISizeY, cursor->raster[i], pxAOISizeX, pxAOISizeY, cursor->type,  0, 0);

			if (	( Y_RANGE_DEFINED == TRUE  	)	&& 
				( Y_RANGE_DEFINED == TRUE  	)	&&
				( x_input_ul      == x_input_lr ) 	&& 
				( x_input_ul      == x_input_lr ) 	) for (j = 0; j < ( pxAOISizeX * pxAOISizeY ); j++) if ( cursor->raster[i][j] != cursor->nodata[i] ) cursor->raster[i][j] = ( cursor->raster[i][j] - cursor->offset[i] ) * cursor->scale[i];
		}
		GDALClose(cursor->dataset); cursor->dataset = NULL;
	}



	free(raster); free(avg);

	//------------------------------------------------------------------------------------------------
	if ( outType == JSON ){
		json_object     	*jobj  	= json_object_new_object();
		json_object     	*jgeo  	= json_object_new_array();
		json_object     	*jtime 	= json_object_new_array();
		json_object     	*jsize 	= json_object_new_array();

		OGRSpatialReferenceH	hSRS	= OSRNewSpatialReference(NULL);


		for (i = 0; i < 6; i++ ) adfGeoTransform[i] = 0.0;
		adfGeoTransform[0] = lowerCorner[1]; adfGeoTransform[3] = upperCorner[0]; adfGeoTransform[1] = x_min_res; adfGeoTransform[5] = y_min_res;
		for (i = 0; i < 6; i++ ) json_object_array_add(jgeo, json_object_new_double(adfGeoTransform[i]));
	

		json_object_array_add(jtime, json_object_new_int(t_min));
		json_object_array_add(jtime, json_object_new_int(t_max));

		json_object_array_add(jsize, json_object_new_int(hit_number));
		json_object_array_add(jsize, json_object_new_int(head->nband));
		json_object_array_add(jsize, json_object_new_int(pxAOISizeY));
		json_object_array_add(jsize, json_object_new_int(pxAOISizeX));


		json_object_object_add(jobj, "datasetId", 	json_object_new_string(prod));
		json_object_object_add(jobj, "timeRange", 	jtime);
		json_object_object_add(jobj, "dimensions", 	jsize);
		json_object_object_add(jobj, "geoTransform", 	jgeo);
		OSRImportFromWkt(hSRS, &wktTargetSRS);
		OSRExportToProj4(hSRS, &wktTargetSRS);
		json_object_object_add(jobj, "projection", 	json_object_new_string(wktTargetSRS));
		OSRDestroySpatialReference(hSRS);

		json_object *jdata = json_object_new_array(); // Main data array

		for( cursor = head; cursor != NULL; cursor = cursor->next){

			struct vsblock *shp_remove;
			if ( cursor->shp_head != NULL ) {
				for ( shp_head = cursor->shp_head; shp_head != NULL; shp_head = shp_head->next ){
					json_object *jcur  = json_object_new_object();

					json_object_object_add(jcur, "geometry" , json_tokener_parse( OGR_G_ExportToJson(shp_head->hGeom) ) );
					
					json_object *jvector_feature  = json_object_new_array();	
					for ( shp_cursor = shp_head, shp_tmp = shp_cursor ; shp_cursor != NULL; shp_tmp = shp_cursor, shp_cursor = shp_cursor->next ){ if ( shp_head->feature != shp_cursor->feature ) continue;
						json_object *jvecor_value  = json_object_new_object();
						json_object_object_add(jvecor_value, "time",   json_object_new_int(	shp_cursor->time ) );
						json_object_object_add(jvecor_value, "value",  json_object_new_double(	shp_cursor->value) );
						json_object_object_add(jvecor_value, "merged", json_object_new_int(	shp_cursor->cnt  ) );

						json_object_array_add( jvector_feature, jvecor_value);

						shp_remove	= shp_cursor;
						shp_tmp->next 	= shp_cursor->next;
						shp_cursor	= shp_tmp;
						if ( shp_cursor != shp_head ) {  OGR_G_DestroyGeometry(shp_remove->hGeom); free(shp_remove); } 
						

					}
					json_object_object_add(jcur, "vector", jvector_feature);
					json_object_array_add(jdata, jcur);

				}
				continue;
			}

			json_object *jcur  = json_object_new_object();
			json_object_object_add(jcur, "time", json_object_new_int(cursor->time) );
			if (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA )) 
			json_object_object_add(jcur, "high", json_object_new_int(cursor->high) );

			if ( cursor->MinMax[0] != DBL_MAX ) {
				json_object_object_add(jcur, "min", json_object_new_double(cursor->MinMax[0]) );
				json_object_object_add(jcur, "max", json_object_new_double(cursor->MinMax[1]) );
			}

			if ( ( cursor->MetadataCount > 0 ) && ( cursor->Metadata != NULL ) ){
				json_object *jmetadata  = json_object_new_object(); 
				char            *pszKey = NULL;
				const char      *pszValue;
				for( int i = 0; i <  cursor->MetadataCount ; i++ ){
					if ( cursor->Metadata[i] == NULL ) continue;
					if (  ( pszValue = (const char *)CPLParseNameValue( cursor->Metadata[i], &pszKey ) ) == NULL ) continue;
					json_object_object_add(jmetadata, pszKey, json_object_new_string(pszValue));
				} 
				json_object_object_add(jcur, "metadata", jmetadata);	
			}
	
			json_object *jraster_band  = json_object_new_array();
			for (int i = 0; i < cursor->nband; i++ ){
				json_object *jraster_y  = json_object_new_array();
				for ( int y = 0; y < pxAOISizeY; y++ ){
					json_object *jraster_x  = json_object_new_array();
					for ( int x = 0; x < pxAOISizeX; x++ ){
						json_object_array_add(jraster_x, json_object_new_double(cursor->raster[i][x + ( pxAOISizeX * y) ] ) );
						}
					json_object_array_add(jraster_y, jraster_x);
				}
				json_object_array_add(jraster_band, jraster_y);
			}
			json_object_object_add(jcur, "raster", jraster_band);
		

			json_object_array_add(jdata, jcur);
	
		}
		json_object_object_add(jobj, "data", jdata);

	
		if ( output_name != NULL ) { bzero(final_path, MAX_PATH_LEN - 1); sprintf(final_path, "attachment; filename=\"%s\"", output_name ); apr_table_add(r->headers_out, "Content-disposition", final_path); }
	        ap_set_content_type(r, "text/json");
		ap_rprintf(r, "%s", json_object_to_json_string(jobj));


                clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Finish in GetCoverage JSON Writeing in %.3f sec, Total Time %.3f", 
				(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
		clock_gettime(CLOCK_REALTIME, &time_before);

		cleanList(head, r); // Clean

		bzero(log_tmp_str, MAX_STR_LEN - 1); sprintf(log_tmp_str, "%f", time_total); addStatToPush(info, "exec_time",  log_tmp_str, GDT_Float32 ); // STATS

		return 0;
	}

	if ( outType == CSV ){
		bzero(final_path, MAX_PATH_LEN - 1); sprintf(final_path, "attachment; filename=\"data.csv\""); apr_table_add(r->headers_out, "Content-disposition", final_path);
		if ( output_name != NULL ) { bzero(final_path, MAX_PATH_LEN - 1); sprintf(final_path, "attachment; filename=\"%s\"", output_name ); apr_table_add(r->headers_out, "Content-disposition", final_path); }
	        ap_set_content_type(r, "text/csv");

		ap_rprintf(r, "epoch;datetime;"); if (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA )) ap_rprintf(r, "high;"); for (i = 0; i < head->nband; i++ ) ap_rprintf(r, "band%d;", i ); ap_rprintf(r, "\n");

		for( cursor = head; cursor != NULL; cursor = cursor->next){
			ap_rprintf(r, "%ld;%s;", cursor->time, datatimeStringFromUNIX( gmtime(&cursor->time)) ); if (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA )) ap_rprintf(r, "%f;", cursor->high);
			for (i = 0; i < cursor->nband; i++ ) ap_rprintf(r, "%f;", ( cursor->raster[i][0] - cursor->offset[i] ) * cursor->scale[i]  ); ap_rprintf(r, "\n");
			
		}
                clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Finish in GetCoverage time series CSV Writeing in %.3f sec, Total Time %.3f", 
				(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
		clock_gettime(CLOCK_REALTIME, &time_before);
		cleanList(head, r); // Clean

		bzero(log_tmp_str, MAX_STR_LEN - 1); sprintf(log_tmp_str, "%f", time_total); addStatToPush(info, "exec_time",  log_tmp_str, GDT_Float32 ); // STATS

		return 0;


	}

	if ( outType == CHART ){
		if ( output_name != NULL ) { bzero(final_path, MAX_PATH_LEN - 1); sprintf(final_path, "attachment; filename=\"%s\"", output_name ); apr_table_add(r->headers_out, "Content-disposition", final_path); }
	        ap_set_content_type(r, "image/png");

		unsigned char 	chart_png[1024];
		char 		chart_file[1024];
		FILE *gnuplotPipe = popen ("/usr/bin/gnuplot 1>&2", "w");


		if ( gnuplotPipe == NULL ) { 	for( cursor = head; cursor != NULL; cursor = cursor->next) for ( i = 0 ; i < cursor->nband; i++ ) free(cursor->raster[i]); 
						ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r,  "ERROR: No gnuplot found!" ); return 500; }

		bzero(chart_file, 1023); sprintf(chart_file, "/tmp/%d.file", r->connection->client_addr->port );
		fprintf(gnuplotPipe, "set terminal png size %d,%d\n", 2048, 1024);	fflush(gnuplotPipe);
		fprintf(gnuplotPipe, "set title '%s' \n", prod);			fflush(gnuplotPipe);
		fprintf(gnuplotPipe, "set xdata time \n");				fflush(gnuplotPipe);
		fprintf(gnuplotPipe, "set timefmt \"%%s\" \n");				fflush(gnuplotPipe);
		fprintf(gnuplotPipe, "set format x \"%%Y-%%m-%%d\\n%%H:%%M:%%S\" \n");	fflush(gnuplotPipe);
		fprintf(gnuplotPipe, "set output '%s'\n", chart_file );			fflush(gnuplotPipe);
		fprintf(gnuplotPipe, "set grid \n");					fflush(gnuplotPipe);

		if ( COLOR_RANGE_DEFINED == TRUE ) 
		fprintf(gnuplotPipe, "set yrange [%f:%f]\n", color_range_min, color_range_max); fflush(gnuplotPipe);


		fprintf(gnuplotPipe, "plot '-' u 1:2 w lp title '%s'\n", prod);		fflush(gnuplotPipe);


		for( cursor = head; cursor != NULL; cursor = cursor->next) for (i = 0; i < cursor->nband; i++ ){ fprintf(gnuplotPipe, "%ld %lf\n", cursor->time, ( cursor->raster[i][0] - cursor->offset[i] ) * cursor->scale[i] ); fflush(gnuplotPipe); }
			
		
		fprintf(gnuplotPipe, "e\n"); 	fflush(gnuplotPipe);
		fprintf(gnuplotPipe, "quit\n");	fflush(gnuplotPipe);
		pclose(gnuplotPipe);

		gnuplotPipe = fopen(chart_file, "rb");
		if ( gnuplotPipe != NULL ) {  while ( fread(chart_png, 1, 1024, gnuplotPipe ) ) {  ap_rwrite( chart_png, 1024, r ); } fclose(gnuplotPipe); }
		if ( remove(chart_file) != 0 ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r,  "ERROR: Unable to remove %s!", chart_file ); }
		
		// ( rasterRGB[0][i] - offset[0] ) * scale[0])

                clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;
		ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Finish in GetCoverage time series Plot Writeing in %.3f sec, Total Time %.3f", 
				(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
		clock_gettime(CLOCK_REALTIME, &time_before);

		cleanList(head, r); // Clean
		bzero(log_tmp_str, MAX_STR_LEN - 1); sprintf(log_tmp_str, "%f", time_total); addStatToPush(info, "exec_time",  log_tmp_str, GDT_Float32 ); // STATS

		return 0;


	}




	if ( output_name != NULL ) { bzero(final_path, MAX_PATH_LEN - 1); sprintf(final_path, "attachment; filename=\"%s\"", output_name ); apr_table_add(r->headers_out, "Content-disposition", final_path); }
        ap_set_content_type(r, "text/xml");

	ap_rprintf(r, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");

	ap_rprintf(r, "<gmlcov:ReferenceableGridCoverage\n");
	ap_rprintf(r, "    xmlns='http://www.opengis.net/gml/3.2'\n");
	ap_rprintf(r, "    xmlns:gml='http://www.opengis.net/gml/3.2'\n");
	ap_rprintf(r, "    xmlns:gmlcov='http://www.opengis.net/gmlcov/1.0'\n");
	ap_rprintf(r, "    xmlns:swe='http://www.opengis.net/swe/2.0'\n");
	ap_rprintf(r, "    xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'\n");
	ap_rprintf(r, "    xmlns:wcs='http://www.opengis.net/wcs/2.0' gml:id='%s'\n", prod );
	ap_rprintf(r, "    xsi:schemaLocation='http://www.opengis.net/wcs/2.0 http://schemas.opengis.net/wcs/2.0/wcsAll.xsd'>\n\n");



	ap_rprintf(r, "  <boundedBy>\n");
	if (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA ))
	ap_rprintf(r, "     <Envelope srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s h\" uomLabels=\"%s %s %s h\" srsDimension=\"4\">\n", 
			proj, time_URL[time_index], dimenstion_Label[label_index][0], dimenstion_Label[label_index][1], time_Label[time_index], dimenstion_unit[label_index], dimenstion_unit[label_index], time_unit[time_index]);
	else
	ap_rprintf(r, "     <Envelope srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s\" uomLabels=\"%s %s %s\" srsDimension=\"3\">\n", 
			proj, time_URL[time_index], dimenstion_Label[label_index][0], dimenstion_Label[label_index][1], time_Label[time_index], dimenstion_unit[label_index], dimenstion_unit[label_index], time_unit[time_index]);
	ap_rprintf(r, "      <lowerCorner>%f %f %ld", lowerCorner[0], lowerCorner[1], t_min); if (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA ))   ap_rprintf(r, " %f", high_min);	ap_rprintf(r, "</lowerCorner>\n");
	ap_rprintf(r, "      <upperCorner>%f %f %ld", upperCorner[0], upperCorner[1], t_max); if (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA ))   ap_rprintf(r, " %f", high_max);	ap_rprintf(r, "</upperCorner>\n");
	ap_rprintf(r, "    </Envelope>\n");
	ap_rprintf(r, "  </boundedBy>\n\n");
	ap_rprintf(r, "  <domainSet>\n");
	ap_rprintf(r, "    <gmlrgrid:ReferenceableGridByVectors dimension=\"3\" gml:id=\"%s-grid\"\n",  prod);
	ap_rprintf(r, "		xmlns:gmlrgrid=\"http://www.opengis.net/gml/3.3/rgrid\"\n");
	ap_rprintf(r, "		xsi:schemaLocation=\"http://www.opengis.net/gml/3.3/rgrid http://schemas.opengis.net/gml/3.3/referenceableGrid.xsd\">\n");
	ap_rprintf(r, "      <limits>\n");
	ap_rprintf(r, "         <GridEnvelope>\n");
	ap_rprintf(r, "           <low>0 0 0</low>\n");
	ap_rprintf(r, "           <high>%d %d %d</high>\n", pxAOISizeX - 1, pxAOISizeY - 1, hit_number - 1);
	ap_rprintf(r, "         </GridEnvelope>\n");
	ap_rprintf(r, "      </limits>\n");
	ap_rprintf(r, "      <axisLabels>%s %s %s</axisLabels>\n", dimenstion_Label[label_index][1], dimenstion_Label[label_index][0], time_Label[time_index]);
	ap_rprintf(r, "      <gmlrgrid:origin>\n");
	ap_rprintf(r, "        <Point gml:id=\"%s-origin\" srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s\" uomLabels=\"%s %s %s\" srsDimension=\"3\">\n", 
			prod, proj, time_URL[time_index], dimenstion_Label[label_index][0], dimenstion_Label[label_index][1], time_Label[time_index], dimenstion_unit[label_index], dimenstion_unit[label_index], time_unit[time_index]);
	ap_rprintf(r, "          <pos>%f %f %ld</pos>\n", upperCorner[0] - (y_min_res/2), lowerCorner[1] - (x_min_res/2), t_min);
	ap_rprintf(r, "        </Point>\n");
	ap_rprintf(r, "      </gmlrgrid:origin>\n");
	ap_rprintf(r, "      <gmlrgrid:generalGridAxis>\n");
	ap_rprintf(r, "        <gmlrgrid:GeneralGridAxis>\n");
	ap_rprintf(r, "          <gmlrgrid:offsetVector srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s\" uomLabels=\"%s %s %s\" srsDimension=\"3\">0 %f 0</gmlrgrid:offsetVector>\n", 
		proj, time_URL[time_index], dimenstion_Label[label_index][0], dimenstion_Label[label_index][1], time_Label[time_index], dimenstion_unit[label_index], dimenstion_unit[label_index], time_unit[time_index], x_min_res);
	ap_rprintf(r, "          <gmlrgrid:coefficients />\n");
	ap_rprintf(r, "          <gmlrgrid:gridAxesSpanned>%s</gmlrgrid:gridAxesSpanned>\n", dimenstion_Label[label_index][1]);
	ap_rprintf(r, "          <gmlrgrid:sequenceRule axisOrder=\"+1\">Linear</gmlrgrid:sequenceRule>\n");
	ap_rprintf(r, "        </gmlrgrid:GeneralGridAxis>\n");
	ap_rprintf(r, "      </gmlrgrid:generalGridAxis>\n");
	ap_rprintf(r, "      <gmlrgrid:generalGridAxis>\n");
	ap_rprintf(r, "        <gmlrgrid:GeneralGridAxis>\n");
	ap_rprintf(r, "          <gmlrgrid:offsetVector srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s\" uomLabels=\"%s %s %s\" srsDimension=\"3\">%f 0 0</gmlrgrid:offsetVector>\n", 
		proj, time_URL[time_index], dimenstion_Label[label_index][0], dimenstion_Label[label_index][1], time_Label[time_index], dimenstion_unit[label_index], dimenstion_unit[label_index], time_unit[time_index], y_min_res);
	ap_rprintf(r, "          <gmlrgrid:coefficients />\n");
	ap_rprintf(r, "          <gmlrgrid:gridAxesSpanned>%s</gmlrgrid:gridAxesSpanned>\n", dimenstion_Label[label_index][0]);
	ap_rprintf(r, "          <gmlrgrid:sequenceRule axisOrder=\"+1\">Linear</gmlrgrid:sequenceRule>\n");
	ap_rprintf(r, "        </gmlrgrid:GeneralGridAxis>\n");
	ap_rprintf(r, "      </gmlrgrid:generalGridAxis>\n");
	ap_rprintf(r, "      <gmlrgrid:generalGridAxis>\n");
	ap_rprintf(r, "        <gmlrgrid:GeneralGridAxis>\n");
	ap_rprintf(r, "          <gmlrgrid:offsetVector srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s\" uomLabels=\"%s %s %s\" srsDimension=\"3\">0 0 1</gmlrgrid:offsetVector>\n",
		proj, time_URL[time_index], dimenstion_Label[label_index][0], dimenstion_Label[label_index][1], time_Label[time_index], dimenstion_unit[label_index], dimenstion_unit[label_index], time_unit[time_index]);
	ap_rprintf(r, "          <gmlrgrid:coefficients>\n");
	for( cursor = head; cursor != NULL; cursor = cursor->next) ap_rprintf(r, "%ld ", cursor->time - t_min); ap_rprintf(r, "\n");
	ap_rprintf(r, "          </gmlrgrid:coefficients>\n");
	ap_rprintf(r, "          <gmlrgrid:gridAxesSpanned>t</gmlrgrid:gridAxesSpanned>\n");
	ap_rprintf(r, "        <gmlrgrid:sequenceRule axisOrder=\"+1\">Linear</gmlrgrid:sequenceRule>\n");
	ap_rprintf(r, "      </gmlrgrid:GeneralGridAxis>\n");
	ap_rprintf(r, "    </gmlrgrid:generalGridAxis>\n");


	if (( high_min != DEFAULT_NODATA ) && ( high_max != DEFAULT_NODATA )){
	ap_rprintf(r, "      <gmlrgrid:generalGridAxis>\n");
	ap_rprintf(r, "        <gmlrgrid:GeneralGridAxis>\n");
	ap_rprintf(r, "          <gmlrgrid:offsetVector srsName=\"http://www.opengis.net/def/crs-compound?1=http://www.opengis.net/def/crs/EPSG/0/%d&amp;2=%s\" axisLabels=\"%s %s %s h\" uomLabels=\"%s %s %s h\" srsDimension=\"4\">0 0 0 1</gmlrgrid:offsetVector>\n",
		proj, time_URL[time_index], dimenstion_Label[label_index][0], dimenstion_Label[label_index][1], time_Label[time_index], dimenstion_unit[label_index], dimenstion_unit[label_index], time_unit[time_index]);
	ap_rprintf(r, "          <gmlrgrid:coefficients>\n");
	for( cursor = head; cursor != NULL; cursor = cursor->next) ap_rprintf(r, "%f ", cursor->high - high_min); ap_rprintf(r, "\n");
	ap_rprintf(r, "          </gmlrgrid:coefficients>\n");
	ap_rprintf(r, "          <gmlrgrid:gridAxesSpanned>t</gmlrgrid:gridAxesSpanned>\n");
	ap_rprintf(r, "        <gmlrgrid:sequenceRule axisOrder=\"+1\">Linear</gmlrgrid:sequenceRule>\n");
	ap_rprintf(r, "      </gmlrgrid:GeneralGridAxis>\n");
	ap_rprintf(r, "    </gmlrgrid:generalGridAxis>\n");
	}



	ap_rprintf(r, "  </gmlrgrid:ReferenceableGridByVectors>\n");
	ap_rprintf(r, "</domainSet>\n");
	ap_rprintf(r, "<rangeSet>\n");
	ap_rprintf(r, "  <DataBlock>\n");
	ap_rprintf(r, "    <rangeParameters />\n");
	ap_rprintf(r, "      <tupleList ts=\",\" cs=\"\">\n");



	//------------------------------------------------------------------------------------------------
	
	printTupleList(r, head, pxAOISizeX, pxAOISizeY );

	//------------------------------------------------------------------------------------------------
	
	ap_rprintf(r, "\n");
	ap_rprintf(r, "       </tupleList>\n");
	ap_rprintf(r, "   </DataBlock>\n");
	ap_rprintf(r, "  </rangeSet>\n");
	ap_rprintf(r, "  <coverageFunction>\n");
	ap_rprintf(r, "   <GridFunction>\n");
	ap_rprintf(r, "    <sequenceRule axisOrder=\"+1 +2 +3\">Linear</sequenceRule>\n");
	ap_rprintf(r, "    <startPoint>0 0 0</startPoint>\n");
	ap_rprintf(r, "   </GridFunction>\n");
	ap_rprintf(r, "  </coverageFunction>\n");
	ap_rprintf(r, "  <gmlcov:rangeType>\n");
	ap_rprintf(r, "    <swe:DataRecord>\n");
	ap_rprintf(r, "      <swe:field name=\"value\">\n");
	ap_rprintf(r, "        <swe:Quantity xmlns:swe=\"http://www.opengis.net/swe/2.0\" definition=\"%s\">\n", OGC_dataTypes[head->type]);
	ap_rprintf(r, "          <swe:label>float</swe:label>\n");
	ap_rprintf(r, "          <swe:description>primitive</swe:description>\n");
	ap_rprintf(r, "          <swe:uom code=\"10^0\" />\n");
	ap_rprintf(r, "          <swe:constraint>\n");
	ap_rprintf(r, "            <swe:AllowedValues>\n");
	ap_rprintf(r, "              <swe:interval>-3.4028234E+38 3.4028234E+38</swe:interval>\n");
	ap_rprintf(r, "            </swe:AllowedValues>\n");
	ap_rprintf(r, "          </swe:constraint>\n");
	ap_rprintf(r, "        </swe:Quantity>\n");
	ap_rprintf(r, "      </swe:field>\n");
	ap_rprintf(r, "    </swe:DataRecord>\n");
	ap_rprintf(r, "  </gmlcov:rangeType>\n");
	ap_rprintf(r, "</gmlcov:ReferenceableGridCoverage>\n");


	clock_gettime(CLOCK_REALTIME, &time_after); time_total += (double)( time_after.tv_sec - time_before.tv_sec )  + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION;
	ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "INFO: Finish in GetCoverage XML Writeing in %.3f sec, Total Time %.3f",
			(double)( time_after.tv_sec - time_before.tv_sec ) + ( time_after.tv_nsec - time_before.tv_nsec ) / BILLION, time_total);
	clock_gettime(CLOCK_REALTIME, &time_before);

	cleanList(head, r); // Clean

	bzero(log_tmp_str, MAX_STR_LEN - 1); sprintf(log_tmp_str, "%f", time_total); addStatToPush(info, "exec_time",  log_tmp_str, GDT_Float32 ); // STATS
	return 0;
}

