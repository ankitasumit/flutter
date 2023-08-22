package anthem.ehub.bds.utils

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.ReadPreference
import com.typesafe.config.Config

object MongoBDSConnection {
  
  def MongoDB(envType:String, appConf:Config)={
    val uri = appConf.getString("bds.mongodb.mongodb_uri_"+envType.toLowerCase) 
    //+ "." + appConf.getString("bds.mongodb.config_collection")
    val mongoClient = new MongoClient( new MongoClientURI(uri+"?authMechanism=SCRAM-SHA-1"))
    mongoClient.setReadPreference(ReadPreference.secondaryPreferred())
    val database = mongoClient.getDatabase(appConf.getString("bds.mongodb.mongodb_"+envType.toLowerCase))
//    val collection = database.getCollection(appConf.getString("bds.mongodb.config_collection")) 
    database
  }
}