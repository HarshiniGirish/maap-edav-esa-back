#pragma once 
//apache include
#include <apr_uri.h>
#include <apr_strings.h>
#include "http_core.h"
#include "http_protocol.h"
#include "http_log.h"
#include "util_script.h"
//sys include 
#include <string>
#include <sstream>
#include <fstream>
#include <vector>
#include <iostream>
#include <stdlib.h>
//lib include
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <boost/foreach.hpp>
#include <pqxx/pqxx>
#include <gdal/gdal.h>
#include <gdal/ogr_geometry.h>
#include <gdal/ogr_api.h>
#include <json-c/json.h>


using std::string;
using std::vector;
using std::ostringstream;
using namespace std;



class ElementError{
        private:
        char* schema;
        char* desc;
        string mess="Incorrect Query Parameter. See the link to learn about possible query parameters.";

        public:
        ElementError(char* sc,char* des){
                schema=sc;
                desc=des;
        }
        void printError(vector<string>list_err,request_rec *req){
            string m;
            string list;
            for(int i=0;i<list_err.size();i++){
                list.append("{\"p\":\""+list_err.at(i)+"\"}");
                if(i<list_err.size()-1){
                    list.append(",");
                }
            }
            ap_set_content_type(req,"application/opensearch+json");
            m.append("{\"message\":\""+mess+"\",\"Error_code\":\"400\",\"parameters\":["+list.c_str()+"],\"link\":\""+schema+req->hostname+desc+"\"}");
            ap_rputs(m.c_str(),req);
        }

        bool checkError(vector<string>list_param){
                if(!list_param.empty()){
                        return true;
                }else{
                        return false;
                }
        }

        void printException(request_rec *req,const char* msg,string code){
            ap_rputs("{\"message\":\"",req);
            ap_rputs(msg,req);
            ap_rputs("\",\"Error_code\":\"",req);
            ap_rputs(code.c_str(),req);
            ap_rputs("\"}",req);
        }

        void printExceprion(request_rec *req,string msg,string code){
            ap_rputs("{\"message\":\"",req);
            ap_rputs(msg.c_str(),req);
            ap_rputs("\",\"Error_code\":\"",req);
            ap_rputs(code.c_str(),req);
            ap_rputs("\"}",req);
        }
        void printSuccess(request_rec *req,const char* msg,string code){
            ap_rputs("{\"message\":\"",req);
            ap_rputs(msg,req);
            ap_rputs("\",\"Status_code\":\"",req);
            ap_rputs(code.c_str(),req);
            ap_rputs("\"}",req);
        }

        void log_exception(string log_msg){
            fprintf(stderr,"%s\n",log_msg.c_str());
            fflush(stderr);

        }
        void log_exception(string log_msg,const char *param){
            fprintf(stderr,"%s:%s\n",log_msg.c_str(),param);
            fflush(stderr);
        }
    
    int return400(request_rec *req,string msg,string title){
		string output_msg;
		output_msg.append("{\"message\":\""+msg+"\",\"title\":\""+title+"\",\"status_code\":401}");
		ap_custom_response(req, HTTP_BAD_REQUEST , output_msg.c_str());
		return HTTP_BAD_REQUEST;
	}

	int return401(request_rec *req,string msg,string title){
		string output_msg;
		output_msg.append("{\"message\":\""+msg+"\",\"title\":\""+title+"\",\"status_code\":401}");
		ap_custom_response(req, HTTP_UNAUTHORIZED , output_msg.c_str());
		return HTTP_UNAUTHORIZED;
	}

	int return404(request_rec *req,string msg,string title){
		string output_msg;
		output_msg.append("{\"message\":\""+msg+"\",\"title\":\""+title+"\",\"status_code\":404}");
		ap_custom_response(req, HTTP_NOT_FOUND , output_msg.c_str());
		return HTTP_NOT_FOUND;
	}

	int return405(request_rec *req,string msg,string title){
		string output_msg;
		output_msg.append("{\"message\":\""+msg+"\",\"title\":\""+title+"\",\"status_code\":405}");
		ap_custom_response(req, HTTP_METHOD_NOT_ALLOWED , output_msg.c_str());
		return HTTP_METHOD_NOT_ALLOWED;
	}

	int return422(request_rec *req,string msg,string title){
		string output_msg;
		output_msg.append("{\"message\":\""+msg+"\",\"title\":\""+title+"\",\"status_code\":422}");
		ap_custom_response(req, HTTP_UNPROCESSABLE_ENTITY , output_msg.c_str());
		return HTTP_UNPROCESSABLE_ENTITY;

	}
	int return422list(vector<string>list_err,request_rec *req){
		string output_msg;
                string list;
		for(int i=0;i<list_err.size();i++){
			list.append(list_err.at(i));
			if(i<list_err.size()-1){
				list.append(",");
			}
		}
		output_msg.append("{\"message\":\"Wrong or not Allowed parameter("+list+")\",\"status_code\":422,\"title\":\"Parameter Problem\"}");
		ap_custom_response(req, HTTP_UNPROCESSABLE_ENTITY , output_msg.c_str());
		return HTTP_UNPROCESSABLE_ENTITY;

	}
	int return500(request_rec *req,string msg,string title){
		string output_msg;
		output_msg.append("{\"message\":\""+msg+"\",\"title\":\""+title+"\",\"status_code\":500}");
		ap_custom_response(req, HTTP_INTERNAL_SERVER_ERROR  , output_msg.c_str());
		return HTTP_INTERNAL_SERVER_ERROR ;

	}
	int return200(request_rec *req,string msg,string title){
		string output_msg;
		output_msg.append("{\"message\":\""+msg+"\",\"title\":\""+title+"\",\"status_code\":200}");
		ap_custom_response(req, HTTP_OK , output_msg.c_str());
		return HTTP_OK;

	}


};

template<class Container>
void split(const std::string& str,Container& cont,string delim){
    size_t current,previus=0;
    current=str.find(delim);
    while(current!=string::npos){
        cont.push_back(str.substr(previus,current-previus));
        previus=current+delim.length();
        current=str.find(delim,previus);
    }
    cont.push_back(str.substr(previus,current-previus));
}

vector<string> getkey(string delim,vector<string>kv){
    size_t find=0;
    vector<string> key;
    while(!kv.empty()){
        find=kv.back().find(delim);
        key.push_back(kv.back().substr(0,find));
        kv.pop_back();
    }
    return key;
}
boost::property_tree::ptree readDescribe(char *describepath){
    boost::property_tree::ptree root;
    boost::property_tree::read_json(describepath,root);
    return root;
};

ostringstream convertToXML(boost::property_tree::ptree pt){
    ostringstream xml;
    boost::property_tree::write_xml(xml,pt);
    return xml;
};

ostringstream writeDescribe(boost::property_tree::ptree pt){
    ostringstream json;
    boost::property_tree::write_json(json,pt);
    return json;
};

string createWKTfromJson(string geojson){
    string wkt;
    OGRGeometryH geom = OGR_G_CreateGeometryFromJson(geojson.c_str());
    OGRErr err = OGR_G_ExportToWkt(geom,(char**)wkt.c_str());
    return string(wkt);
}

string createJsonFromWkt(string wkt,ElementError error){
    char* geojson=NULL;
    char* pszwkt=(char*)wkt.c_str();
    try{
    OGRSpatialReferenceH ref=OSRNewSpatialReference(NULL);
    OGRGeometryH new_geom;
    OGRErr err=OGR_G_CreateFromWkt(&pszwkt,ref,&new_geom);
    if (err==OGRERR_NONE){
        geojson=OGR_G_ExportToJson(new_geom);
    }else{
        strcpy(geojson,"{}");
    }
    }catch(const std::exception &xcp){
        error.log_exception(xcp.what());
    }
    return geojson;
}

vector<string> findElement(string val,vector<string> array){
    if(std::find(array.begin(),array.end(),val)==array.end()){
        array.emplace_back(val);
    }
    return array;
}

class PostgresOpensearch{

    public:
            int DescribeDatasets(request_rec *req,char *ConnectionString,char* Http_Schema,char* DescibeDataset,char* DescribeFilePath){
                ElementError error(Http_Schema, DescibeDataset);
                string url;
                string url1;
                string data;
                string datasetId;
                char *generic_str = NULL;
                apr_table_t *GET;
                vector<string> kv;
                vector<string> key;
                int lenelem = 0;
                pqxx::connection c(ConnectionString);
                pqxx::work txn(c);
                apr_array_header_t *pairs = NULL;
                json_object *jobj = json_object_new_object();
                json_object *output_json = json_object_new_object();
                json_object *jarray = json_object_new_array();
                vector<string> json_keys;
                if(req->args !=NULL){
                    if(req->method_number == M_GET){
                        data = req->args;
                        ap_args_to_table(req, &GET);
                        split(data, kv, "&");
                        key = getkey("=", kv);
                        lenelem = key.size();
                        for(int z=0;z<lenelem;z++){
                            if (strcmp(key[z].c_str(),"datasetId") == 0){
                                datasetId=apr_table_get(GET,"datasetId");
                            }else{
                                error.return422(req,string("Invalid Paarameter: ").append(key[z]),"Invalid Parameter");
                            }
                        }
                    }else if (req->method_number == M_POST){
                        while (pairs && !apr_is_empty_array(pairs)){
                            apr_off_t len = 0;
			                apr_size_t size = 0;
			                char *buffer = NULL;
			                const char constkey[1024] = {};
			                string key;
			                ap_form_pair_t *pair = (ap_form_pair_t *)apr_array_pop(pairs);
			                apr_brigade_length(pair->value, 1, &len);
			                size = (apr_size_t)len;
			                buffer = (char *)apr_palloc(req->pool, size + 1);
			                apr_brigade_flatten(pair->value, buffer, &size);
                            buffer[size]='\0';
                            if(strcmp(apr_pstrdup(req->pool,pair->name),"dataset") == 0){
                                generic_str = (char *)malloc(strlen(buffer) + 1);
                                strcpy(generic_str, buffer);
                                datasetId=generic_str;
                            }else{
                                bzero((char *)constkey, 1023);
				                strcpy((char *)constkey, apr_pstrdup(req->pool, pair->name));
				                key = constkey;
                                error.return422(req,string("Invalid Paarameter: ").append(key),"Invalid Parameter");
                            }
                        }
                    }else{
                        error.return405(req,string("This request methos is not allowed: ").append(req->method),"Method not Allowed");
                    }
                    ap_set_content_type(req,"application/json");
                    try{
                        pqxx::result r=txn.exec("SELECT json_metadata FROM records WHERE json_metadata ->> 'dataset' = '"+datasetId+"' ;");
                        txn.commit();
                        c.disconnect();
                        for(auto row:r){
                            jobj = json_tokener_parse(row[0].c_str());
                            json_object_object_foreach(jobj,key,val){
                                if (std::find(json_keys.begin(),json_keys.end(),string(key))==json_keys.end()){
                                    json_keys.emplace_back(string(key));
                                    json_object_array_add(jarray,json_object_new_string(string(key).c_str()));
                                }
                            }
                            jobj = NULL;
                        }
                        json_object *jstring = json_object_new_string("FeatureCollection");
                        json_object_object_add(output_json,"type",jstring);
                        json_object_object_add(output_json,"availableSearchKeys",jarray);
                        ap_rputs(json_object_to_json_string(output_json),req);
                    }catch(const std::exception &xcp){
                        error.log_exception(xcp.what());
                    }
                    return OK;
                }else{
                    try{
                    boost::property_tree::ptree pt = readDescribe(DescribeFilePath);
                    error.log_exception("OPENSEARCH DESCRIBE requested");
                    ap_set_content_type(req, "application/xml");
                    ap_rputs("<?xml version=\"1.0\" encoding=\"UTF-8\"?>", req);
                    ap_rputs("<OpenSearchDescription xmlns=\"http://a9.com/-/spec/opensearch/1.1/\"  xmlns:parameters=\"http://a9.com/-/spec/opensearch/extensions/parameters/1.0/\" >", req);
                    ap_rputs("<ShortName>", req);
                    ap_rputs(pt.get<string>("OpenSearchDescription.ShortName").c_str(), req);
                    ap_rputs("</ShortName>", req);
                    ap_rputs("<LongName>", req);
                    ap_rputs(pt.get<string>("OpenSearchDescription.LongName").c_str(), req);
                    ap_rputs("</LongName>", req);
                    ap_rputs("<Description>", req);
                    ap_rputs(pt.get<string>("OpenSearchDescription.Description").c_str(), req);
                    ap_rputs("</Description>", req);
                    ap_rputs("<Tags>",req);
                    ap_rputs(pt.get<string>("OpenSearchDescription.Tags").c_str(),req);
                    ap_rputs("</Tags>", req);
                    ap_rputs("<Contact>",req);
                    ap_rputs(pt.get<string>("OpenSearchDescription.Contact").c_str(),req);
                    ap_rputs("</Contact>", req);
                    url.append("<Url type=\"application/xml\" rel=\"self\" template=\"");url.append(pt.get<string>("OpenSearchDescription.properties.describe.Url").c_str());url.append("?dataset={dataset?}\">");
                    ap_rputs(url.c_str(), req);
                    ap_rputs("<parameters:Parameter name=\"datasetId\" value=\"{datasetId?}\" title=\"Collection dataset\" minimum=\"0\" maximum=\"1\"/>", req);
                    ap_rputs("</Url>",req);
                    url1.append("<Url type=\"application/json\" rel=\"self\" template=\"");
                    url1.append(pt.get<string>("OpenSearchDescription.properties.search.Url").c_str());url1.append("?dataset={dataset?}&amp;timeExtent={starttime/endtime?}&amp;identifier={identifier?}&amp;geometry={geo:bbox?}&amp;maxRecords={count?}&amp;startIndex={startIndex?}\">");
                    ap_rputs(url1.c_str(), req);
                    ap_rputs("<parameters:Parameter name=\"dataset\" value=\"{dataset?}\" title=\"Collection dataset\" minimum=\"0\" maximum=\"1\"/>", req);
                    ap_rputs("<parameters:Parameter name=\"timeExtent\" value=\"{starttime/endtime?}\" title=\"Collection dataset\" minimum=\"0\" maximum=\"1\"/>", req);
                    ap_rputs("<parameters:Parameter name=\"date\" value=\"{time?}\" title=\"Collection date\" minimum=\"0\" maximum=\"1\"/>", req);
                    ap_rputs("<parameters:Parameter name=\"identifier\" value=\"{identifier?}\" title=\"Collection identifier\" minimum=\"0\" maximum=\"1\"/>", req);
                    ap_rputs("<parameters:Parameter name=\"maxRecords\" value=\"{count?}\" title=\"Maximum result count\" minimum=\"0\" maximum=\"1\" minInclusive=\"0\" maxInclusive=\"10\" pattern=\"[0-9]+\"/>", req);
                    ap_rputs("<parameters:Parameter name=\"startIndex\" value=\"{startIndex?}\" title=\"Start index of results\" minimum=\"0\" maximum=\"1\" minInclusive=\"1\" pattern=\"[0-9]+\"/>", req);
                    ap_rputs("<parameters:Parameter name=\"geometry\" value=\"{geo:bbox?}\" title=\"WKT geometry for search results\"/>", req);
                    ap_rputs("</Url>",req);
                    ap_rputs("<Developer>",req);
                    ap_rputs(pt.get<string>("OpenSearchDescription.Developer").c_str(),req);
                    ap_rputs("</Developer>", req);
                    ap_rputs("<Attribution>",req);
                    ap_rputs(pt.get<string>("OpenSearchDescription.Attribution").c_str(),req);
                    ap_rputs("</Attribution>", req);
                    ap_rputs("<SyndacationRight>",req);
                    ap_rputs(pt.get<string>("OpenSearchDescription.SyndacationRight").c_str(),req);
                    ap_rputs("</SyndacationRight>", req);
                    ap_rputs("<AdultContent>",req);
                    ap_rputs(pt.get<string>("OpenSearchDescription.AdultContent").c_str(),req);
                    ap_rputs("</AdultContent>", req);
                    ap_rputs("<Language>",req);
                    ap_rputs(pt.get<string>("OpenSearchDescription.Language").c_str(),req);
                    ap_rputs("</Language>", req);
                    ap_rputs("<OutputEncoding>",req);
                    ap_rputs(pt.get<string>("OpenSearchDescription.OutputEncoding").c_str(),req);
                    ap_rputs("</OutputEncoding>", req);
                    ap_rputs("<InputEncoding>",req);
                    ap_rputs(pt.get<string>("OpenSearchDescription.InputEncoding").c_str(),req);
                    ap_rputs("</InputEncoding>", req);
                    ap_rputs("</OpenSearchDescription>", req);
                     }catch(const std::exception &xcp){
                        error.log_exception(xcp.what());
                    }
                    return OK;
                }
            };
    
            int SearchRecords(request_rec *req,char* ConnectionString,char* Http_Schema,char* DescibeDataset,char* LimitRequestBody,char* ItemPerPage,char* StartIndex,char* DescribeFilePath,char* MaxItemPerPage){
                ElementError error(Http_Schema, DescibeDataset);
                boost::property_tree::ptree pt = readDescribe(DescribeFilePath);
                json_object *jobj = json_object_new_object();
                json_object *jqueryparam = json_object_new_object();
                json_object *jlimit = NULL;
                json_object *jindex = NULL;
                json_object *jstringparam = NULL;
                string data;
                string geom;
                string wkt_geom;
                string link_first;
                string link_next;
                string link_last;
                pqxx::connection c(ConnectionString);
                pqxx::work txn(c);
                string query;
                string count_query;
                string paging_query = "";
                string limit_get;
                string index_get;
                string genericstr_get;
                string link1;
                string link2;
                string link3;
                apr_table_t *GET;
                apr_array_header_t *pairs = NULL;
                char *post_geom = NULL;
                char *generic_str = NULL;
                char *limit = NULL;
                char *index = NULL;
                vector<string> kv;
                vector<string> key;
                int lenelem = 0;
                int next;
                int last;
                int count;
                if(req->method_number == M_GET){
                    if(req->args == NULL){
                        query = "SELECT json_metadata,source,abstract,wkt_geometry FROM records AND";
                        count_query = "SELECT COUNT(*) FROM records AND";

                    }else{
                        data = req->args;
                        query="SELECT json_metadata,source,abstract,wkt_geometry FROM records WHERE";
                        count_query = "SELECT COUNT(*) FROM records WHERE";
                        ap_args_to_table(req, &GET);
                        split(data, kv, "&");
                        key = getkey("=", kv);
                        lenelem = key.size();
                        for(int z=0;z<lenelem;z++){
                            if (strcmp(key[z].c_str(),"geometry") == 0){
                                //query con geometria
                                geom = apr_table_get(GET,"geometry");
                                if(geom.find("{") != string::npos){
                                    link_first.append("&"+key[z]+"="+geom);
                                    link_next.append("&"+key[z]+"="+geom);
                                    link_last.append("&"+key[z]+"="+geom);
                                    query.append(" ST_INTERSECTS(ST_GeomFromText('"+createWKTfromJson(geom)+"',4326),wkb_geometry) AND");
                                    count_query.append(" ST_INTERSECTS(ST_GeomFromText('"+createWKTfromJson(geom)+"',4326),wkb_geometry) AND");
                                    jstringparam=json_object_new_string(geom.c_str());
                                    json_object_object_add(jqueryparam,key[z].c_str(),jstringparam);
                                    jstringparam=NULL;
                                }else if(geom.find("(") != string::npos){
                                    link_first.append("&"+key[z]+"="+geom);
                                    link_next.append("&"+key[z]+"="+geom);
                                    link_last.append("&"+key[z]+"="+geom);
                                    query.append(" ST_INTERSECTS(ST_GeomFromText('"+geom+"',4326),wkb_geometry) AND");
                                    count_query.append(" ST_INTERSECTS(ST_GeomFromText('"+geom+"',4326),wkb_geometry) AND");
                                    jstringparam=json_object_new_string(geom.c_str());
                                    json_object_object_add(jqueryparam,key[z].c_str(),jstringparam);
                                    jstringparam=NULL;
                                }else{
                                    error.return400(req,string("Invalid Geometry: ").append(geom.c_str()),"Invalid Geometry");
                                }
                            }else if(strcmp(key[z].c_str(),"maxRecords") == 0){
                                limit_get = apr_table_get(GET,"maxRecords");
                                if (std::stoi(limit_get.c_str()) > 0 && (std::stoi(limit_get.c_str())<=std::stoi(ItemPerPage) || std::stoi(limit_get.c_str())<=std::stoi(MaxItemPerPage))){                                    
                                    paging_query.append(" LIMIT "+limit_get);
                                    jstringparam=json_object_new_string(limit_get.c_str());
                                    json_object_object_add(jqueryparam,key[z].c_str(),jstringparam);
                                    jstringparam=NULL;
                                    jlimit = json_object_new_int(stoi(limit_get));
                                }else{
                                    paging_query.append(" LIMIT "+string(ItemPerPage));
                                    jstringparam=json_object_new_string(ItemPerPage);
                                    json_object_object_add(jqueryparam,key[z].c_str(),jstringparam);
                                    jstringparam=NULL;
                                    jlimit = json_object_new_int(stoi(ItemPerPage));
                                }
                            }else if(strcmp(key[z].c_str(),"startIndex") == 0){
                                index_get = apr_table_get(GET,"startIndex");
                                if (std::stoi(index_get.c_str()) >= 0 ){
                                    link_first.append("&"+key[z]+"="+index_get);
                                    paging_query.append(" OFFSET "+index_get);
                                    jstringparam=json_object_new_string(index_get.c_str());
                                    json_object_object_add(jqueryparam,key[z].c_str(),jstringparam);
                                    jstringparam=NULL;
                                    jindex = json_object_new_int(stoi(index_get));
                                }else{
                                    link_first.append("&"+key[z]+"="+StartIndex);
                                    paging_query.append(" OFFSET "+string(StartIndex));
                                    jstringparam=json_object_new_string(StartIndex);
                                    json_object_object_add(jqueryparam,key[z].c_str(),jstringparam);
                                    jstringparam=NULL;
                                    jindex = json_object_new_int(stoi(StartIndex));
                                }
                            }else{
                                //query testuale generica
                                genericstr_get=apr_table_get(GET,key[z].c_str());
                                link_first.append("&"+key[z]+"="+genericstr_get);
                                link_next.append("&"+key[z]+"="+genericstr_get);
                                link_last.append("&"+key[z]+"="+genericstr_get);
                                query.append(" json_metadata ->> '"+key[z]+"' = '"+genericstr_get+"' AND");
                                count_query.append(" json_metadata ->> '"+key[z]+"' = '"+genericstr_get+"' AND");
                                jstringparam=json_object_new_string(genericstr_get.c_str());
                                json_object_object_add(jqueryparam,key[z].c_str(),jstringparam);
                                jstringparam=NULL;
                            }
                        }
                        
                    }
                    if (limit_get.empty()){
                            limit_get=ItemPerPage;
                            paging_query.append(" LIMIT "+string(ItemPerPage));
                            jlimit = json_object_new_int(stoi(ItemPerPage));
                        }
                        if(index_get.empty()){
                            index_get=StartIndex;
                            paging_query.append(" OFFSET "+string(StartIndex));
                            jindex = json_object_new_int(stoi(StartIndex));
                        }
                        next= stoi(limit_get)+stoi(index_get);
                        link_first.append("&maxRecords="+limit_get);
                        link_next.append("&maxRecords="+limit_get+"&startIndex="+to_string(next));
                        link_last.append("&maxRecords="+limit_get);
                        query = query.substr(0,query.size()-4);
                        count_query = count_query.substr(0,count_query.size()-4);
                        query.append(paging_query);
                        query.append(";");
                        count_query.append(";");
                }else if(req->method_number == M_POST){
                    if(req->args == NULL){
                        query = "SELECT json_metadata,source,abstract,wkt_geometry FROM records;";
                        count_query = "SELECT COUNT(*) FROM records;";
                    }else{
                        count_query = "SELECT COUNT(*) FROM records WHERE";
                        query = "SELECT json_metadata,source,abstract,wkt_geometry FROM records WHERE";
                        ap_parse_form_data(req, NULL, &pairs, -1, (apr_size_t)LimitRequestBody);
                        while (pairs && !apr_is_empty_array(pairs)){
                            apr_off_t len = 0;
			                apr_size_t size = 0;
			                char *buffer = NULL;
			                const char constkey[1024] = {};
			                string key;
			                ap_form_pair_t *pair = (ap_form_pair_t *)apr_array_pop(pairs);
			                apr_brigade_length(pair->value, 1, &len);
			                size = (apr_size_t)len;
			                buffer = (char *)apr_palloc(req->pool, size + 1);
			                apr_brigade_flatten(pair->value, buffer, &size);
                            buffer[size]='\0';
                            if(strcmp(apr_pstrdup(req->pool,pair->name),"geometry") == 0){                                
                                post_geom=(char *)malloc(strlen(buffer)+1);strcpy(post_geom,buffer);
                                geom=post_geom;
                                if(geom.find("{") != string::npos){
                                    query.append(" ST_INTERSECTS(ST_GeomFromText('"+createWKTfromJson(geom)+"',4326),wkb_geometry) AND");
                                    count_query.append(" ST_INTERSECTS(ST_GeomFromText('"+createWKTfromJson(geom)+"',4326),wkb_geometry) AND");
                                    jstringparam=json_object_new_string(geom.c_str());
                                    json_object_object_add(jqueryparam,key.c_str(),jstringparam);
                                    jstringparam=NULL;
                                }else if(geom.find("(") != string::npos){
                                    query.append(" ST_INTERSECTS(ST_GeomFromText('"+geom+"',4326),wkb_geometry) AND");
                                    count_query.append(" ST_INTERSECTS(ST_GeomFromText('"+createWKTfromJson(geom)+"',4326),wkb_geometry) AND");
                                    jstringparam=json_object_new_string(geom.c_str());
                                    json_object_object_add(jqueryparam,key.c_str(),jstringparam);
                                    jstringparam=NULL;
                                }else{
                                    error.return400(req,string("Invalid Geometry: ").append(geom.c_str()),"Invalid Geometry");
                                }
                            }else if(strcmp(apr_pstrdup(req->pool,pair->name),"maxRecords") == 0){
                                limit=(char *)malloc(strlen(buffer)+1);strcpy(limit,buffer);
                                if (std::stoi(limit) > 0 && (std::stoi(limit)<=std::stoi(ItemPerPage) || std::stoi(limit_get.c_str())<=std::stoi(MaxItemPerPage))){                                    
                                    paging_query.append(" LIMIT "+string(limit));
                                    jstringparam=json_object_new_string(limit);
                                    json_object_object_add(jqueryparam,key.c_str(),jstringparam);
                                    jstringparam=NULL;
                                    jlimit = json_object_new_int(stoi(limit));
                                }else{
                                    paging_query.append(" LIMIT "+string(ItemPerPage));
                                    jstringparam=json_object_new_string(ItemPerPage);
                                    json_object_object_add(jqueryparam,key.c_str(),jstringparam);
                                    jstringparam=NULL;
                                    jlimit = json_object_new_int(stoi(ItemPerPage));
                                }
                            }else if(strcmp(apr_pstrdup(req->pool,pair->name),"startIndex") == 0){
                                index=(char *)malloc(strlen(buffer)+1);strcpy(index,buffer);
                                if (std::stoi(index) >= 0 ){                                    
                                    paging_query.append(" OFFSET "+string(index));
                                    jstringparam=json_object_new_string(index);
                                    json_object_object_add(jqueryparam,key.c_str(),jstringparam);
                                    jstringparam=NULL;
                                    jindex = json_object_new_int(stoi(index));
                                }else{
                                    paging_query.append(" OFFSET "+string(StartIndex));
                                    jstringparam=json_object_new_string(StartIndex);
                                    json_object_object_add(jqueryparam,key.c_str(),jstringparam);
                                    jstringparam=NULL;
                                    jindex = json_object_new_int(stoi(StartIndex));
                                }
                            }else{
                                generic_str=(char *)malloc(strlen(buffer)+1);strcpy(generic_str,buffer);
                                bzero((char *)constkey, 1023);
				                strcpy((char *)constkey, apr_pstrdup(req->pool, pair->name));
				                key = constkey;
                                query.append(" json_metadata ->> '"+key+"' = '"+generic_str+"' AND");
                                count_query.append(" json_metadata ->> '"+key+"' = '"+generic_str+"' AND");
                                jstringparam=json_object_new_string(generic_str);
                                json_object_object_add(jqueryparam,key.c_str(),jstringparam);
                                jstringparam=NULL;
                                generic_str = NULL;
                            }
                        }
                        if (limit_get.empty()){                            
                            paging_query.append(" LIMIT "+string(ItemPerPage));
                            jlimit = json_object_new_int(stoi(ItemPerPage));
                        }
                        if(index_get.empty()){
                            paging_query.append(" OFFSET "+string(StartIndex));
                            jindex = json_object_new_int(stoi(StartIndex));
                        }
                        next= stoi(limit_get)+stoi(index_get);
                        query = query.substr(0,query.size()-4);
                        count_query = count_query.substr(0,count_query.size()-4);
                        query.append(paging_query);
                        query.append(";");
                        count_query.append(";");
                    }
                }else{
                    error.return405(req,string("This request methos is not allowed: ").append(req->method),"Method not Allowed");
                }

                json_object *jobjfeatures = json_object_new_array();
                json_object *jcount = NULL;
                try{
                    pqxx::result r=txn.exec(query);
                    pqxx::result  r_count=txn.exec(count_query);
                    txn.commit();
                    c.disconnect();
                    for(auto c_row:r_count){
                        jcount=json_object_new_int(stoi(c_row[0].c_str()));
                        count=stoi(c_row[0].c_str());
                    }
                    for(auto row:r){
                        json_object *jobjfeature = json_object_new_object();
                        json_object_object_add(jobjfeature,"metadata",json_tokener_parse(row["json_metadata"].c_str()));
                        if(strcmp(row["wkt_geometry"].c_str(),"") != 0){
                            wkt_geom=createJsonFromWkt(row["wkt_geometry"].c_str(),error);
                            json_object_object_add(jobjfeature,"geometry",json_tokener_parse(wkt_geom.c_str()));
                        }
                        json_object_object_add(jobjfeature,"source",json_object_new_string(row["source"].c_str()));
                        json_object_object_add(jobjfeature,"summary",json_object_new_string(row["abstract"].c_str()));
                        json_object_array_add(jobjfeatures,jobjfeature);
                    }
                }catch(const std::exception &xcp){
                    error.return500(req,string("ERROR: ").append(xcp.what()),"Internal Server Error");
                }
                last= count -1;
                link_last.append("&startIndex="+to_string(last));
                json_object *jstring = json_object_new_string("FeatureCollection");
                string id = pt.get<string>("OpenSearchDescription.properties.search.Url").c_str();
                json_object *jreqid = json_object_new_string(id.c_str());
                json_object *jobj1 = json_object_new_object();
                json_object *jobj2 = json_object_new_object();
                json_object *jarrayreq = json_object_new_array();
                json_object *jarraylink = json_object_new_array();
                json_object *jlinkfirst = json_object_new_object();
                json_object *jlinklast = json_object_new_object();
                json_object *jlinknext = json_object_new_object();
                json_object *jlinkdescribe = json_object_new_object();
                if(lenelem > 0){
                    size_t first = link_first.find("&");
                    link_first = link_first.substr(first+1,link_first.size());
                    link1=pt.get<string>("OpenSearchDescription.properties.search.Url").c_str();link1.append("?"+link_first);
                    json_object_object_add(jlinkfirst,"first",json_object_new_string(link1.c_str()));
                    json_object_array_add(jarraylink,jlinkfirst);
                    if(next<count){
                        size_t nexxt = link_next.find("&");
                        link_next = link_next.substr(nexxt+1,link_next.size());
                        link2=pt.get<string>("OpenSearchDescription.properties.search.Url").c_str();link2.append("?"+link_next);
                        json_object_object_add(jlinknext,"next",json_object_new_string(link2.c_str()));
                        json_object_array_add(jarraylink,jlinknext);
                    }
                    size_t lasst = link_last.find("&");
                    link_last = link_last.substr(lasst+1,link_last.size());
                    link3=pt.get<string>("OpenSearchDescription.properties.search.Url").c_str();link3.append("?"+link_last);
                    json_object_object_add(jlinklast,"last",json_object_new_string(link3.c_str()));
                    json_object_array_add(jarraylink,jlinklast);
                }else{
                    size_t first = link_first.find("&");
                    link_first = link_first.substr(first+1,link_first.size());
                    link1=pt.get<string>("OpenSearchDescription.properties.search.Url").c_str();link1.append("?"+link_first);
                    json_object_object_add(jlinkfirst,"first",json_object_new_string(link1.c_str()));
                    json_object_array_add(jarraylink,jlinkfirst);
                    if(next<count){
                        size_t nexxt = link_next.find("&");
                        link_next = link_next.substr(nexxt+1,link_next.size());
                        link2=pt.get<string>("OpenSearchDescription.properties.search.Url").c_str();link2.append("?"+link_next);
                        json_object_object_add(jlinknext,"next",json_object_new_string(link2.c_str()));
                        json_object_array_add(jarraylink,jlinknext);
                    }
                    size_t lasst = link_last.find("&");
                    link_last = link_last.substr(lasst+1,link_last.size());
                    link3=pt.get<string>("OpenSearchDescription.properties.search.Url").c_str();link3.append("?"+link_last);
                    json_object_object_add(jlinklast,"last",json_object_new_string(link3.c_str()));
                    json_object_array_add(jarraylink,jlinklast);
                }
                json_object_object_add(jlinkdescribe,"describe",json_object_new_string(pt.get<string>("OpenSearchDescription.properties.describe.Url").c_str()));
                json_object_array_add(jarraylink,jlinkdescribe);
                json_object_array_add(jarrayreq,jqueryparam);
                json_object_object_add(jobj2,"request",jarrayreq);
                json_object_object_add(jobj1,"id",jreqid);
                json_object_object_add(jobj1,"query",jobj2);
                json_object_object_add(jobj1,"itemsPerPage",jlimit);
                json_object_object_add(jobj1,"startIndex",jindex);
                json_object_object_add(jobj1,"totalResults",jcount);
                json_object_object_add(jobj1,"links",jarraylink);
                json_object_object_add(jobj,"type",jstring);
                json_object_object_add(jobj,"properties",jobj1);
                json_object_object_add(jobj,"features",jobjfeatures);
                ap_set_content_type(req,"application/json");
                ap_rputs(json_object_to_json_string(jobj),req);

                jobj =NULL; jobj1=NULL; jobj2=NULL; jarraylink=NULL;
                jarrayreq=NULL;jlinkdescribe=NULL;jlinklast=NULL;jlinknext=NULL;
                jlinkfirst=NULL;jreqid=NULL;jreqid=NULL;
                jcount=NULL;jobjfeatures=NULL;jindex=NULL;jlimit=NULL;
                return OK;
            };
};
