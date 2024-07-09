print("init database")
var database = _getEnv('MONGO_INITDB_DATABASE');
var user = _getEnv('MONGO_INITDB_ROOT_USERNAME');
var pw = _getEnv('MONGO_INITDB_ROOT_PASSWORD');

if( db.getMongo().getDBNames().indexOf(database) < 0 ){
    print("create")
    db=db.getSiblingDB(database)

    db.createCollection("datasets",{
      validator : {
        $jsonSchema : {
          bsonType : "object",
          required : [ "datasetId", "subDatasets" ],
          properties : {
            datasetId : {
              bsonType : "string",
              description : "It must be a string and is required. It specifies dataset names for ingested product"
            },
            subDatasets : {
              bsonType : "object",
              description : "this is an object containing the high level description for each subdataset"
            }
          }
        }
    }})

    db.createCollection("temporalbar",{
      validator:{
        $jsonSchema:{
          bsonType:"object",
          required:["datasetId","presenceDate"],
          properties:{
            datasetId:{
              bsonType:"string",
              description:"It must be a string and is required. It specifies dataset names for ingested product"
            },
            presenceDate:{
              bsonType:"date",
              description:"It must be a string.It specifies the date of presence of this dataset"
            }
          }
        }
    }})
    db.createCollection("accessAnalytics",{
      validator:{
        $jsonSchema:{
          bsonType:"object",
          required:["subDatasetId","user_id","requestDate"],
          properties:{
            subDatasetId:{
              bsonType:"string",
              description:"It must be a string and is required. It specifies subdataset names for ingested product"
            },
            user_id:{
              bsonType:"string",
              description:"It must be a string and is required. It specifies when this datasetId has been ingested"
            },
            requestDate:{
              bsonType:"string",
              description:"It must be a string and is required. it specifies the date of the request"
            }
          }
        }
    }})

    db.createCollection("describe",{
      validator:{
        $jsonSchema:{
          bsonType:"object",
          required:["datasetId","filters"],
          properties:{
            datasetId:{
              bsonType:"string",
              description:"It must be a string and is required. It specifies dataset names for ingested product"
            },
            filters:{
              bsonType:"object",
              description:"It must be an object.It specifies the basic and specific filters for each dataset"
            }
          }
        }
    }})

    db.createCollection("ingestionAnalytics",{
      validator:{
        $jsonSchema:{
          bsonType:"object",
          required:["datasetId","subDatasetId","ingestionDate"],
          properties:{
            datasetId:{
              bsonType:"string",
              description:"It must be a string and is required. It specifies dataset names for ingested product"
            },
            subDatasetId:{
              bsonType:"string",
              description:"It must be a string and is required. It specifies subdataset names for ingested product"
            },
            ingestionDate:{
              bsonType:"date",
              description:"It must be a string and is required. It specifies when this datasetId has been ingested"
            }
          }
        }
    }})
    db.datasets.createIndex( { "datasetId" : 1 }, { unique: true } );
    db.temporalbar.createIndex( { "datasetId": 1, "presenceDate" : 1 }, { unique: true } );
    db.describe.createIndex({ "datasetId": 1 }, { unique: true } )
    db.accessAnalytics.createIndex({"subDatasetId" : 1,"user_id" : 1,"requestDate" : 1}, { unique: true } )
    db.ingestionAnalytics.createIndex({"subDatasetId" : 1,"datasetId" : 1,"ingestionDate" : 1},{unique:true})
    db.createUser({user: user,pwd:pw,roles: [ { role: "readWrite", db: database }]})
    print("Done")
}else{
    print("database already present")
}
























