rint("init database")
var database = _getEnv('MONGO_INITDB_DATABASE');
var user = _getEnv('MONGO_INITDB_ROOT_USERNAME');
var pw = _getEnv('MONGO_INITDB_ROOT_PASSWORD');

if( db.getMongo().getDBNames().indexOf(database) < 0 ){
    print("create")
    db=db.getSiblingDB(database)
    db.createCollection("datasets", {
      validator: {
        $jsonSchema: {
          bsonType: "object",
          required: ["datasetId"],
          properties:{
            datasetId : {
              bsonType : "string",
              description : "It must be a string and is required. It specifies dataset names for ingested product"
            }
          }
        }
      }
    })
    db.createCollection("records", {
      validator : {
        $jsonSchema : {
          bsonType : "object",
          required : ["datasetId", "catalogueId", "subDatasetId"],
          properties : {
            /* START REQUIRED FIELDS */
            datasetId : {
              bsonType : "string",
                      description : "It must be a string and is required. It is an unique product identifier (i.e filename)"
            },
            subDatasetId : {
                      bsonType : "string",
                      description : "must be a string and is required. It specifies subdataset names for ingested product (i.e. MODIS:LST, S2A_RGB)"
            },
            productDate : {
                      bsonType : "date",
                      description : "must be a date and is required - product date"
            },
              catalogueId:{
                      bsonType : "string",
                      description : "It must be a string and is required. It is an unique product catalogue identifier (i.e filename)"
              },
              source:{
                      bsonType : "string",
                      description : "It must be a string and is required. link or refernce to the object"
              },
              geolocated:{
                      bsonType : "string",
                      description : "true if the product has the geolocation"
              }
          }
        }
      }
    })
    db.createCollection("temporalbar",{
      validator:{
      $jsonSchema:{
        bsonType: "object",
        required:["datasetId", "subDatasetId" ,"presenceDate" ],
        properties:{
          datasetId:{
            bsonType:"string",
            description:"It must be a string and is required. It specifies dataset names for ingested product"
          },
          subDatasetId:{
            bsonType:"string",
            description:"It must be a string and is required. It specifies subdataset names for ingested product"
          },
          presenceDate:{
            bsonType:"string",
            description:"It must be a string.It specifies the date of presence of this dataset"
          }
        }
      }
      }
    })
    db.records.createIndex( { subDatasetId: 1, catalogueId:1, datasetId: 1 }, { unique: true } );
    db.records.createIndex( { geometry: "2dsphere"} );
    db.datasets.createIndex( { datasetId : 1 }, { unique: true } );
    db.datasets.createIndex( { geometry: "2dsphere"} );
    db.temporalbar.createIndex( { subDatasetId:1,datasetId: 1, presenceDate : 1 }, { unique: true } );
    db.createUser({user: user,pwd:pw,roles: [ { role: "readWrite", db: database }]})
    print("Done")
}else{
    print("database already present")
}

