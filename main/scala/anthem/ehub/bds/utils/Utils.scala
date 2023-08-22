package anthem.ehub.bds.utils

import java.net.URI
import java.util.Date

import scala.collection.mutable.ListBuffer

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.bson.Document

import com.anthem.utils.CustomFileUtil
import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import scala.collection.immutable.ListMap

case class Parameter(Source: String, Loadtype: String, target: String, serialization: String, compressiontechniques: String, delta: String,
                     primarykey: String, fieldsseparator: String, lastupdated_by: String, dbType: String)
object Utils {
  def validate(input: String) = {
    println("input   " + input)
    val line = input.split("\\|", -1)
    println("line   " + line.size)
    val obj = Parameter(line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7), line(8), line(9))
    println("obj   " + obj)
    println("obj_load  " + obj.Loadtype)
    obj

  }

  def validate(input: Document) =
    {
      println("in validatin")
      val obj = Parameter(input.getString("Source"), input.getString("Loadtype"), input.getString("Target"), input.getString("Serialization"), input.getString("Comperssion"), input.getString("Delta"), input.getString("Primarykey"), input.getString("Fields_Separator"), input.getString("partionkey"), input.getString("DBType"))
      println(obj.Loadtype)
      obj
    }
  def writeDataToCSVWithoutLocal(dataFrame: DataFrame, sparkContext: SparkContext, sourceLocationPath: String, finalMergedfileName: String, header: String, trailer:String) = {

    checkAndDeleteFolder(sparkContext, sourceLocationPath)
    dataFrame.write.mode("append").format("com.databricks.spark.csv").option("header", "false").save(sourceLocationPath)
    val destinationPath = sourceLocationPath + "/mergedFile"
    merge(sparkContext, sourceLocationPath, destinationPath, finalMergedfileName, header,trailer)

  }
  def writeDataToCSV(dataFrame: DataFrame, sparkContext: SparkContext, sourceLocationPath: String, mergeLocation: String, finalMergedfileName: String,
                     toCopyLocal: Boolean, localPath: String, header: String, delimiter: String, trailer:String) = {

    checkAndDeleteFolder(sparkContext, sourceLocationPath)
    dataFrame.write.mode("append").format("com.databricks.spark.csv").option("header", "false").option("delimiter", delimiter).save(sourceLocationPath)
    var destinationPath: String = null
    if (mergeLocation != null) {
      destinationPath = mergeLocation
    } else {
      destinationPath = sourceLocationPath + "/mergedFile"
    }
    if (toCopyLocal) {
      copyFromHDFSToLocalFile(sparkContext: SparkContext, sourceLocationPath, localPath + "/" + finalMergedfileName, header,trailer)
    } else {
      merge(sparkContext, sourceLocationPath, destinationPath, finalMergedfileName, header,trailer)
    }

  }
  /**
   * @param sc
   * @param srcPath
   * @param dstPath
   */
  def merge(sparkContext: SparkContext, srcPath: String, destinationPath: String, finalMergedfileName: String, header: String, trailer:String): Unit = {
    println("Before Merging the files " + new Date())
    val srcFileSystem = FileSystem.get(new URI(srcPath), sparkContext.hadoopConfiguration)
    val dstFileSystem = FileSystem.get(new URI(destinationPath), sparkContext.hadoopConfiguration)
    checkAndDeleteFolder(sparkContext, destinationPath + "/" + finalMergedfileName)
    CustomFileUtil.copyMergeWithHeader(srcFileSystem, new Path(srcPath), dstFileSystem, new Path(destinationPath + "/" + finalMergedfileName), false, sparkContext.hadoopConfiguration, header, trailer)
    println("After Merging the files " + new Date())
  }

  /**
   * @param sparkContext
   * @param path
   */
  def checkAndDeleteFolder(sparkContext: SparkContext, path: String) = {
    val hadoopConfig = sparkContext.hadoopConfiguration
    val fileSystem = org.apache.hadoop.fs.FileSystem.get(hadoopConfig)
    val exists = fileSystem.exists(new org.apache.hadoop.fs.Path(path))
    if (exists) {
      println("Path already exists " + path)
      fileSystem.delete(new org.apache.hadoop.fs.Path(path), true)
      println("Deleted the folders and sub folders of existing path")
    }
    println("Path where the files will be written is " + path)
  }

  
  def checkAndCreateFolder(sparkContext: SparkContext, path: String) = {
    val hadoopConfig = sparkContext.hadoopConfiguration
    val fileSystem = org.apache.hadoop.fs.FileSystem.get(hadoopConfig)
    val exists = fileSystem.exists(new org.apache.hadoop.fs.Path(path))
    if (!exists) {
      println("Path Does not exists " + path)
      fileSystem.mkdirs(new Path(path))
      println("Creating the folders and sub folders of existing path")
    }
    println("Path where the files will be written is " + path)
  }
  /**
   * @param hiveContext
   * @param uri
   * @param sourceCode
   */
  def executeHQLQueries(hiveContext: SparkSession, dbName: String, uri: String, sourceCode: String, collectionName: String) = {
    /*   val finalURI = uri+"."+collectionName
      val readConfig = ReadConfig(Map("uri" -> finalURI))
      val mongoRecords = MongoSpark.load(sparkContext, readConfig).toDF*/
    println(uri)
    val mongoClient = new MongoClient(new MongoClientURI(uri))
    val database = mongoClient.getDatabase(dbName)
    val collection = database.getCollection(collectionName)
    val searchQuery = new BasicDBObject();
    searchQuery.put("name", sourceCode);
    println("Source code is " + sourceCode)
    val cursor = collection.find(searchQuery);
    val iterator = cursor.iterator();
    var document: Document = null;
    while (iterator.hasNext()) {
      document = iterator.next();
    }
    println(document)
    if (document != null && document.getBoolean("executeQuery")) {
      val query = document.getString("query").split(";")
      query.foreach(query => {
        if (query != null && query.trim().length() > 0) {
          println("Before executing the Hive query " + query + new Date())
          hiveContext.sql(query)
          println("After executing the Hive query " + query + new Date())
        }
      })
    } else {
      println("No Queries are executed")
    }
  }

  /**
   * @param sourcePath
   * @param destinationPath
   */
  def copyFromHDFSToLocalFile(sparkContext: SparkContext, sourceLocation: String, localDestinationLocation: String, header: String, trailer:String) = {

    val sourcePath = new Path(sourceLocation)
    println(sourceLocation)
    val destinationPath = new Path(localDestinationLocation)
    println("Dest" + localDestinationLocation)
    val hadoopFileSystem = FileSystem.get(sparkContext.hadoopConfiguration);
    // val localFileSystem = FileSystem.getLocal(new Configuration())
    val localFileSystem = FileSystem.get(sparkContext.hadoopConfiguration);
    if (localFileSystem.exists(destinationPath)) {
      println("Hadoop file system already exists, it will deleted")
      localFileSystem.delete(destinationPath, true)
    }
    CustomFileUtil.copyMergeWithHeader(hadoopFileSystem, sourcePath, localFileSystem, destinationPath, false, sparkContext.hadoopConfiguration, header, trailer)
    println(s"Merged file has been copied to ${destinationPath}")
  }

  
  def copyFromHDFSToLocalFile(sparkContext: SparkContext, sourceLocation: String, localDestinationLocation: String) = {
    val sourcePath = new Path(sourceLocation)
    val destinationPath = new Path(localDestinationLocation)
    val hadoopFileSystem = FileSystem.get(sparkContext.hadoopConfiguration);
    if(hadoopFileSystem.exists(destinationPath)){
      hadoopFileSystem.delete(destinationPath)
    }
    hadoopFileSystem.copyToLocalFile(sourcePath,destinationPath)
  }
  /**
 * Returns Mongo Collection 
 * @param env
 * @return
 */
def getMongoConfigCollection(env: String) ={
    val appConf = ConfigFactory.load()
    val collection = getMongoCollectionWithAppConf(env,appConf.getString("bds.mongodb.config_collection"),appConf)
    collection
  }

  /**
 * Returns Mongo Collection 
 * @param env
 * @return
 */
def getMongoCollectionWithoutAppConf(env: String, collectionName:String) ={
    getMongoCollectionWithAppConf(env, collectionName,ConfigFactory.load())
  }

  /**
 * Returns Mongo Collection 
 * @param env
 * @return
 */
def getMongoCollectionWithAppConf(env: String, collectionName:String, appConf:Config) ={
    val uri = appConf.getString("bds.mongodb.mongodb_uri_" + env.toLowerCase())
    val mongoClient = new MongoClient(new MongoClientURI(uri + "?authMechanism=SCRAM-SHA-1"))
    val database = mongoClient.getDatabase(appConf.getString("bds.mongodb.mongodb_" + env.toLowerCase()))
    val collection = database.getCollection(collectionName)
    collection
  }


/**
 * Fetches the document from collection based on field and vlaue
 * @param field
 * @param value
 * @param collection
 * @return
 */
def getDataFromCollection(field:String, value:String, collection:MongoCollection[Document]) ={ //source, EHUB_MBR_DOMN.EHUB_MBR_ID, dail_load
    val searchQuery = new BasicDBObject();
    searchQuery.put(field, value);
    val cursor = collection.find(searchQuery);
    val ite = cursor.iterator();
    var document:Document = null;
    var jobs:List[String] = null;
    if(ite.hasNext()) {
     document= ite.next();
    }
    document
}

/**
 * Fetches the document from collection based on field and vlaue
 * @param field
 * @param value
 * @param collection
 *  regexQuery.put("name",new BasicDBObject("$regex", "ops411_parallel_").append("$options", "i"));
 * @return
 */
def getDataFromCollectionUsingLike(field:String, value:String, ignoreCase:Boolean,collection:MongoCollection[Document]) ={
    val regexQuery = new BasicDBObject();
    if (ignoreCase) {
      regexQuery.put(field, new BasicDBObject("$regex", value).append("$options", "i"))
    } else {
      regexQuery.put(field, new BasicDBObject("$regex", value))
    }
    val cursor = collection.find(regexQuery);
    val ite = cursor.iterator();
    val documents: ListBuffer[Document] = new ListBuffer[Document]
    while (ite.hasNext()) {
      var document: Document = ite.next(); ;
      documents.append(document)
    }
  }

  import org.apache.spark.sql.Row
  def mkJson(r: Row, schema: List[String]): String =
    {
      val t = schema zip r.mkString("~").split("~").toList

      val p = t.filter(k => { k._1.contains("date") })
      val q = t.filter(k => { k._1.contains("pharmacy") }).map(a => a._2.split(",").map(l => (a._1, l)))
      val s = t.filter(k => { k._1.contains("source_code") })
      val u = t.filter(k => { k._1.contains("hpcc_code") })
      val v = t.filter(k => { k._1.contains("contract_code") })
      val x = t.filter(k => { k._1.contains("program") }).map(a => a._2.split(",").map(l => (a._1, l)))
      val y = t.filter(k => { k._1.contains("product") }).map(a => a._2.split(",").map(l => (a._1, l)))
      val z = t.filter(k => { k._1.contains("castlight") }).map(k => (k._1, """["""" + (k._2.replaceAll(",+",",").replaceAll(",", """","""")+ """"]""").replaceAll(""",""]""","]")))
   //   val m = t.filter(k => { !(k._1.contains("Program") || k._1.contains("Product") || k._1.contains("castlight_Ind")) })

      val proStr = y.transpose.distinct.map(k => scala.util.parsing.json.JSONObject(ListMap(k.toMap.toSeq.sortBy(_._1):_*)))
      val pgmStr = x.transpose.distinct.map(k => scala.util.parsing.json.JSONObject(ListMap(k.toMap.toSeq.sortBy(_._1):_*)))
      val phar =  q.transpose.distinct.map(k => scala.util.parsing.json.JSONObject(k.toMap))

      val combineStr = proStr zip pgmStr
      var str = ""
      if(phar.length == 2){
         str = "\"pharmacy_indicator\":\"Y\""
      }else{
        str = phar.toString().substring(6,phar.toString.length-2)
      }
      
      val fstr = combineStr.map(k => {
        val xStr = k._1.toString()
        val yStr = k._2.toString()
        xStr.substring(0, xStr.length - 1) + "," + """ "Program" : [ """ + yStr + """] }"""
      }).toString()

      val zstr = scala.util.parsing.json.JSONObject(z.toMap).toString().replaceAll("\"\\[", "\\[").replaceAll("\\]\"", "\\]").replaceAll("\\\\\"", "\"")
     // val mstr = scala.util.parsing.json.JSONObject(m.toMap).toString()
      val cstr = scala.util.parsing.json.JSONObject(v.toMap).toString()
      val pstr = scala.util.parsing.json.JSONObject(p.toMap).toString()
      val hstr = scala.util.parsing.json.JSONObject(u.toMap).toString()
      //val qstr = scala.util.parsing.json.JSONObject(q.toMap).toString()
      val sstr = scala.util.parsing.json.JSONObject(s.toMap).toString()

      
      val str1 = Array(cstr.substring(0, cstr.length - 1), pstr.substring(1, pstr.length - 1),zstr.substring(1, zstr.length - 1),str,hstr.substring(1, hstr.length - 1),sstr.substring(1, sstr.length - 1), fstr.replaceAll("List\\(",""""Product": [""").replaceAll("\\)","""]""")).mkString(",")
      str1 + "}"
    }
}