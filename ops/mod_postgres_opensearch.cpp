
#include "httpd.h"
#include "http_config.h"
#include "http_protocol.h"
#include "ap_config.h"
#include "http_core.h"
#include <mod_postgres_opensearch.h>
#include <memory>

//global variable definitions
char* ConnectionString = NULL;
char* DescibeDataset = NULL;
char* Http_Schema = NULL;
char* DescribeFilePath = NULL;
char* SearchRecords = NULL;
char* LimitRequestBody = NULL;
char* ItemPerPage = NULL;
char* MaxItemPerPage = NULL;
char* StartIndex = NULL;


auto postres_opensearch = std::make_unique<PostgresOpensearch>();

//apache bindings functionalities for directives
static const char* set_postgresconnectionstring(cmd_parms *cmd,void *cfg,const char *arg1){
    ConnectionString=(char*)malloc(strlen(arg1)+1);strcpy(ConnectionString,arg1);
    return NULL;
}

static const char* set_descibedataset(cmd_parms *cmd, void *cfg, const char *arg1){
    DescibeDataset = (char*)malloc(strlen(arg1)+1);strcpy(DescibeDataset,arg1);
    return NULL;
}

static const char* set_http_schema(cmd_parms *cmd, void *cfg, const char *arg1){
    Http_Schema = (char*)malloc(strlen(arg1)+1);strcpy(Http_Schema,arg1);
    return NULL;
}

static const char* set_describejson_path(cmd_parms *cmd,void *cfg,const char *arg1){
    DescribeFilePath=(char*)malloc(strlen(arg1)+1);strcpy(DescribeFilePath,arg1);
    return NULL;
}

static const char* set_search_uri(cmd_parms *cmd,void *cfg,const char *arg1){
    SearchRecords=(char*)malloc(strlen(arg1)+1);strcpy(SearchRecords,arg1);
    return NULL;
}

static const char* set_limit_request_body(cmd_parms *cmd,void *cfg,const char* args1){
	LimitRequestBody=(char*)malloc(strlen(args1)+1);strcpy(LimitRequestBody,args1);
	return NULL;
}

static const char* set_itemperpage(cmd_parms *cmd,void *cfg,const char *args1,const char *args2){
    ItemPerPage=(char*)malloc(strlen(args1)+1); strcpy(ItemPerPage,args1);
    MaxItemPerPage=(char*)malloc(strlen(args2)+1);strcpy(MaxItemPerPage,args2);
    return NULL;
}

static const char* set_startindex(cmd_parms *cmd,void *cfg,const char *args1){
    StartIndex=(char*)malloc(strlen(args1)+1); strcpy(StartIndex,args1);
    return NULL;
}

extern "C"{
void postgres_opensearch_hooks(apr_pool_t *p)
{
    ap_hook_handler([](request_rec *req){
        string module_name = "postgres_opensearch";
        if(!req->handler || module_name != req->handler){
            return DECLINED;
        }else{
            if (strcmp(req->uri,DescibeDataset) == 0){
                return postres_opensearch->DescribeDatasets(req,ConnectionString,Http_Schema,DescibeDataset,DescribeFilePath);
            }else if(strcmp(req->uri,SearchRecords) == 0){
                return postres_opensearch->SearchRecords(req,ConnectionString,Http_Schema,DescibeDataset,LimitRequestBody,ItemPerPage,StartIndex,DescribeFilePath,MaxItemPerPage);
            }else{
                ap_custom_response(req,HTTP_NOT_FOUND,"{\"message\":\"Wrong Uri to call an api\",\"status_code\":\"404\",\"title\":\"OpenSerach api not found\"}");
                return HTTP_NOT_FOUND;
            }
        
    }
    }, NULL, NULL, APR_HOOK_MIDDLE);
}

static const command_rec directives[] = {
    AP_INIT_TAKE1("PostGresConnectionString",(const char *(*)())set_postgresconnectionstring,NULL,RSRC_CONF,"set connection string params"),
    AP_INIT_TAKE1("DescribeDatasetUrl",(const char *(*)())set_descibedataset,NULL,RSRC_CONF,"set describe dataset uri"),
    AP_INIT_TAKE1("HttpRequestSchema",(const char *(*)())set_http_schema,NULL,RSRC_CONF,"set http schema"),
    AP_INIT_TAKE1("DescribeFilePath",(const char *(*)())set_describejson_path,NULL,RSRC_CONF,"set describe.json file path"),
    AP_INIT_TAKE1("SearchUrl",(const char *(*)())set_search_uri,NULL,RSRC_CONF,"set search api url"),
    AP_INIT_TAKE1("LimitRequestBody",(const char *(*)())set_limit_request_body,NULL,RSRC_CONF,"set request body limit"),
    AP_INIT_TAKE2("ItemPerPage", (const char *(*)())set_itemperpage, NULL, RSRC_CONF, "set default item per page"),
    AP_INIT_TAKE1("StartIndex", (const char *(*)())set_startindex, NULL, RSRC_CONF, "set default start index"),
    {NULL}
};
/* Dispatch list for API hooks */

    void postgres_opensearch_hooks(apr_pool_t *p);
module AP_MODULE_DECLARE_DATA postgres_opensearch_module = {
    STANDARD20_MODULE_STUFF, 
    NULL,                  /* create per-dir    config structures */
    NULL,                  /* merge  per-dir    config structures */
    NULL,                  /* create per-server config structures */
    NULL,                  /* merge  per-server config structures */
    directives,                  /* table of config file commands       */
    postgres_opensearch_hooks  /* register hooks                      */
};
}
