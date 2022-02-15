#ifndef DATATYPES_H
#define DATATYPES_H

#define BILLION 	1E9
#define POLLING_CLIENT  1
#define MAX_D           256
#define DELIMS          "&"
#define MAX_PATH_LEN    8192
#define MAX_STR_LEN     1024
#define DEFAULT_NODATA  -999.0
#define LATLON          0
#define UTM             1
#define AVG             0
#define SUM             1
#define STD             2
#define DEF_MGRS_TILES	50
#define MAX_UPLOAD_SIZE 4096

#define XML             0
#define IMAGE           1
#define TAR             2
#define MAGIC           3
#define JSON           	4
#define CSV           	5
#define CHART          	6

#define NODATA_MIN	2
#define NODATA_MAX	3

#define SQUEEZE		0
#define	SINGLE		1

#define WCS	 	2
#define WMTS	 	3
#define WMS		4

#define OVERVIEW_NO	0
#define OVERVIEW_YES	1
#define	OVERVIEW_MISS	2


#define AVERAGE		0
#define OVERLAP		1
#define MOSTRECENT	2
#define LEASTRECENT	3
#define MINVALUE	4
#define MAXVALUE	5

#define OperationNotSupported 0
#define MissingParameterValue 1
#define InvalidParameterValue 3


#define GetCoverage             0
#define GetCapabilities         1
#define DescribeCoverage        2
#define GetList                 3
#define GetInfo                 4
#define GetMap			5
#define GetTile			6
#define GetFile			7
#define Status                  10

#define SHMOBJ_PATH		"/wcs_memory_info"
#define SHTOBJ_PATH		"/wcs_task_info"
#define SEM_PATH		"/wcs_semaphores"


#define ColorTablesPath         "/opt/mea/dar/mwcs/colorTable"
#define ModulesPath             "/opt/mea/dar/mwcs/modules"
#define DefaultColorTable       "NCV_bright"

#define Sentinel_2_grid_shape	"/opt/mea/dar/mwcs/shapes/mgrs/sentinel_2_index_shapefile.shp"
#define Landsat_grid_shape	"/opt/mea/dar/mwcs/shapes/pathrow/wrs2.shp"
#define earth4h9		"/opt/mea/dar/mwcs/masks/earth4h9.tif"
#define ZOOM_IN_TILE		"/opt/mea/dar/mwcs/zoom_tile.png"
#define MODULES_PATH		"/opt/mea/dar/mwcs/modules"

#define DEBUG_TOKEN		"appeso_ai_miei_tarzanelli"




#ifndef HTTP_UNAVAILABLE_FOR_LEGAL_REASONS
#define HTTP_UNAVAILABLE_FOR_LEGAL_REASONS 451
#endif

struct Unknown_EPSG_block {
        int                             epsg;
        OGRSpatialReferenceH            hSRS;
        struct Unknown_EPSG_block       *next;
};
typedef struct Unknown_EPSG_block Unknown_EPSG_proj4;

struct vsblock {
	time_t  	time;
	char		*layer;
	int		feature;
	char		*field;
	double		value;
	int		cnt;
	OGRGeometryH 	hGeom; 
	struct vsblock	*next;
};

struct sblock {
        char    	file[MAX_PATH_LEN];
        char    	math_id[MAX_STR_LEN];
        int     	sizeX;
        int     	sizeY;
        int     	pxSizeX;
        int     	pxSizeY;
        int     	srcSizeX;
        int     	srcSizeY;
        int     	upX;
        int     	upY;
        int     	offsetX;
        int     	offsetY;
        char    	*proj;
	int		epsg;
        int     	nPixels;
        int     	nLines;
        int     	nband;
        int     	*tband;
        int     	type;
        int     	typeSize;
        time_t  	time;
        double  	GeoTransform[6];
        double  	adfDstGeoTransform[6];
        double  	srcGeoTransform[6];
	double		high;
        char    	*wkt;
        double  	 **raster;
        double  	*nodata;
        double  	*scale;
        double  	*offset;
	char 		*vrt;
	int		warp;
        GDALWarpOptions *psWarpOptions;
        GDALDatasetH 	dataset;
	char		*datasetId;	
        int    		infoflag[6];
	char    	**Metadata;
	int    		MetadataCount;
	double		MinMax[2];
	GDAL_GCP	*gcps;
	int		gcps_cnt;

	struct vsblock *shp_head;
	struct vsblock *shp_cursor;
	int		shp_num;	

        struct  sblock *next;
        struct  sblock *prev;
};

typedef struct sblock * block;

struct sDescCoverage {
	int	pxAOISizeX;
	int	pxAOISizeY;
        double  x_res;
        double  y_res;
        double  GeoX_ul;
        double  GeoX_lr;
        double  GeoY_ul;
        double  GeoY_lr;
        time_t  t_min;
        time_t  t_max;
	double  high_min;
	double	high_max;
        int     type;
        int     nband;
        int     epsg;
        char    *proj;
        int     hit_number;
        char    *label;
        int     infoflag[6];
        struct  sDescCoverage *next;
};

typedef struct  sDescCoverage * DescCoverage;


struct virtualList{
	char 	*name;
	char	*prod_path_array;
	int 	nband;
	int	*tband;
	struct virtualList *next;
};



#define fail(s1, s2) {fprintf(stderr, "ERROR: Math [Error %s] '%s'\n", s1, s2); fflush(stderr);return 0;}
#define qpush(x) unit->queue[  (unit->l_queue)++] = x
#define spush(x) unit->stack[  (unit->l_stack)++] = x
#define spop()   unit->stack[--(unit->l_stack)  ]

struct ProjAndRes{
	int 	proj; 
	double 	res[MAX_D];
	double  lowerCorner[2];
	double	upperCorner[2];
};



typedef struct {
        char    *s;
        int     len, prec, assoc;
} str_tok_t;

typedef struct {
        char    *str;
        int     assoc, prec;
        regex_t re;
} pat_t;


typedef struct urlList {
	char *url;
	struct urlList *next;

} urlList;


struct VirtualContent {
	int 			band;
	int			num;
	char			*src[MAX_D];
	char			*collection[MAX_D];
	int			size;
	struct VirtualContent 	*next;
};


struct mathUnit {
	int 		prec_booster;
	int 		l_queue;
	int		l_stack;
	int		l_stack_max;
	str_tok_t       stack[MAX_D];
	str_tok_t       queue[MAX_D];
	char		*coveragemath;
	int		band;
	struct mathUnit	*next;
	struct mathUnit	*prev;
};

struct mathParamters {
        struct info      *info;
        int              pxAOISizeX;
        int              pxAOISizeY;
	struct  mathUnit *mathcur;
	time_t           *time_serie;
	block		 head;
	block            vrt_cursor;
	int		 time_serie_index;
	int     	 *layers;
	int		 layer_index;
	int		 nband;
	int              MERGING_IMAGE;
};



struct log_stats {
	char		 *key;
	char		 *value;
	int		 type;
	struct log_stats *next;
};


struct info {
        char                    *query_string;
	char			*content_type;
        char                    *uri;
        request_rec     	*r;
        char                    *token;
        char                    *module;
        int                     killable;
        struct log_stats        *STATS_TO_PUSH;
	CURLSH 			*share;
	pthread_mutex_t  	connlock;
	void			*func;
	char			*coverage;
	char			*version;
	char			*error_msg;
	char			*tile_path;
	int			exit_code;
	struct 	nfs_mounts 	*nfs_mount_points;
	int			passthrough;
	double			x_input_ul;
	double			x_input_lr;
	double			y_input_ul;
	double			y_input_lr;
	char    		**ENVIRONMENT_VARIABLE[2];
	int			cache;
	apr_array_header_t 	*pairs;

};

struct MemoryStruct {
	char 	*memory;
	size_t 	size;
};

struct ex_dirent {
	char    d_name[256];
	char    *line;
	struct 	ex_dirent *next;

};

struct loadmule {
	int		id;
        block           cursor;
	struct  info 	*info;
	char            **envs;
        double          x_input_ul; 
        double          y_input_ul; 
        double          x_input_lr; 
        double          y_input_lr;
        int             epgs_out;   
        double          GeoX_ul;    
        double          GeoY_ul;    
        double          GeoX_lr;    
        double          GeoY_lr;  
        int             pxAOISizeX;
        int             pxAOISizeY;
	char		*wktTargetSRS;
	int		FILTER;
	int		WMTS_MODE;
	int		subDataSet;
	int		outType;

};

struct Data_Set_SMOS {
        unsigned int    Grid_Point_ID;
        float           Latitude;
        float           Longitude;
        float           data;
        unsigned int    ScienceFlags;
	unsigned char	Chi_2;
        int             Seqnum;
};

typedef struct Data_Set_SMOS DSMOS;



struct nfs_mounts {
	char *src;
	char *dst;
	struct nfs_mounts *next;
};


struct module_info {
	void 	*handle;
	int 	(*mod)( GDALDatasetH  *, struct loadmule *);
	char	*name;
	char 	*version;
	regex_t *regex;

	struct module_info *next;
};


struct sExchangeInput {
        struct tm       t_time;
        time_t          t_start;
        time_t          t_finish;
        int             X_RANGE_DEFINED;
        int             Y_RANGE_DEFINED;
        double          x_input_ul;
        double          y_input_ul;
        double          x_input_lr;
        double          y_input_lr;
        OGRSpatialReferenceH hSourceSRS;
};

typedef struct sExchangeInput * ExchangeInput;


#endif
