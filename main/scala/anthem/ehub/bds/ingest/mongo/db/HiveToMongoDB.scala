package anthem.ehub.bds.ingest.mongo.db

import java.util.Date

import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.bson.Document

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConcernConfig
import com.mongodb.spark.config.WriteConfig
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.model.IndexOptions
import anthem.ehub.bds.utils.Utils
import anthem.ehub.bds.utils.MongoBDSConfig
import org.apache.spark.sql.SparkSession
import java.io.File

/**
 * This scala code reads the data from Hive Tables and writes the data to Mongo DB
 * @author AF55641
 *
 */
object HiveToMongoDB {

  val APPEND_BATCH_ID_STRING_TEMPLATE = "\"<BATCH_ID>\" as batch_id, \"I\" as Indicator, cast('<BATCH_DATE>' as timestamp) as batch_date"

  def main(args: Array[String]): Unit = {

    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)
    val sparkConf = new SparkConf().setAppName("HiveToMongoDB").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.ui.port", "3012").set("spark.rdd.compress", "true").set("spark.scheduler.mode", "FAIR")
      .set("spark.dynamicAllocation.enabled", "true").set("spark.shuffle.service.enabled", "true")

    sparkConf.registerKryoClasses(Array(Class.forName("anthem.ehub.bds.ingest.mongo.db.HiveToMongoDB"), Class.forName("anthem.ehub.bds.utils.Utils")))

    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val sparkContext = new SparkContext(sparkConf)
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val sqlContext = SparkSession.builder().appName("Spark Hive Example").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
    val envType = args(0)

    val appConf = ConfigFactory.load()
    val URL = appConf.getString("bds.mongodb.mongodb_uri_" + envType.toLowerCase)
    //				val HIVE_DATABASE = appConf.getString("bds.hive.hive_db_"+envType.toLowerCase)
    val MONGO_DATABASE = appConf.getString("bds.mongodb.mongodb_" + envType.toLowerCase)
    val BATCH_COLLECTION_NAME = appConf.getString("bds.batch.batch_collection_name")
    //				sqlContext.sql(s"use $HIVE_DATABASE")
    val template = APPEND_BATCH_ID_STRING_TEMPLATE.replace("<BATCH_ID>", sparkContext.applicationId).replace("<BATCH_DATE>", dateFormat.format(new java.util.Date()))
    val format = new java.text.SimpleDateFormat("dd-MM-yyyy")
    //        val typeList = appConf.getStringList("bds.hiveToMongoDB.sourceTableNames") ///Here
    val typeList: Array[Document] = MongoBDSConfig.getConfigDetails(envType, "hiveToMongoDB", appConf)

    //        println(s"Hive Database $HIVE_DATABASE")
    println(s"Mongo Database $MONGO_DATABASE")
    println("Mongo Database " + appConf.getString("bds.mongodb.mongodb_uri_print_" + envType.toLowerCase))

    //        println(s"Hive Database $HIVE_DATABASE")

    if ("All".equalsIgnoreCase(args(1))) {
      import scala.collection.JavaConversions._
      typeList.foreach(memberObj => {
        val HIVE_DATABASE = memberObj.getString("hive_DB")
        sqlContext.sql(s"use $HIVE_DATABASE")
        println(s"Hive Database $HIVE_DATABASE")
        //              	   Utils.executeHQLQueries(sqlContext,MONGO_DATABASE,URL+"?authMechanism=SCRAM-SHA-1",memberObj.getString("code"),"HQL")
        val documentRDD = getHiveDataFrame(getQueryString(appConf, memberObj.getString("query"), template), sqlContext)
        println("*****************Got Data for Member ************************************************************************************* " + memberObj.getString("hiveToMongoCode"))
        val collectionName = memberObj.getString("collection_name")
        writeToMongoDB(documentRDD, getWriteConfig(MONGO_DATABASE, collectionName, sparkConf, URL), URL, collectionName, appConf, envType)
        println("****** Saving of ************** " + memberObj.getString("collection_name") + " is done")
      })
    } else {
      val doc = MongoBDSConfig.getConfDetailKey(envType, "hiveToMongoCode", args(1), appConf)
      val HIVE_DATABASE = doc.getString("hive_DB")
      sqlContext.sql(s"use $HIVE_DATABASE")
      println(s"Hive Database $HIVE_DATABASE")
      //                    Utils.executeHQLQueries(sqlContext,MONGO_DATABASE,URL+"?authMechanism=SCRAM-SHA-1",doc.getString("code"),"HQL")
      val documentRDD = getHiveDataFrame(getQueryString(appConf, doc.getString("query"), template), sqlContext)
      println("*****************Got Data for Member ************************************************************************************* " + args(1))
      val collectionName = doc.getString("collection_name")
      writeToMongoDB(documentRDD, getWriteConfig(MONGO_DATABASE, collectionName, sparkConf, URL), URL, collectionName, appConf, envType)
      println(s"****** Saving of ************** $args(1) is done")
    }

    val document = createBatchDocument(sparkContext.applicationId, new Date(), new Date(), new Date(), new Date())
    updateBatchCollection(MONGO_DATABASE, document, BATCH_COLLECTION_NAME, sparkConf, sparkContext, URL)
    println("****** Saving of ************** Udpate Batch is done  **************")
  }

  /**
   * Drops the collection if exists
   * @param dataFrame
   * @param writeConfig
   */
def writeToMongoDB(dataFrame:DataFrame, writeConfig:WriteConfig, url:String, collectionName:String, appConf:Config, envType:String) = {
  
        val mongoClient = new MongoClient( new MongoClientURI(url+"?authMechanism=SCRAM-SHA-1"))
        val database = mongoClient.getDatabase(appConf.getString("bds.mongodb.mongodb_"+envType.toLowerCase))
        val collection = database.getCollection(collectionName) 
        println("Collection count - "+ collection.count())
         if(collection.count() > 0){
          collection.drop()
          println(s"Collection Dropped ${collectionName}")
        }
        MongoSpark.save(dataFrame, writeConfig)
        println("Data has written to Mongo DB")
 }

  /**
   * @param config
   * @param param
   * @return
   */
  def getQueryString(config: Config, query: String, commonString: String) = {
    query.replace("<COMMON_STR>", commonString)
  }

  /**
   * Returns the RDD[Document] from passed collectionName
   * @param collectionName
   * @param sqlContext
   * @return
   */
  def getHiveDataFrame(query: String, sqlContext: SparkSession) = {
    val dataFrame = sqlContext.sql(query) //Caling Hive Select 
    dataFrame
  }

  /**
   * @param dbase
   * @param collection
   * @param conf1
   * @return
   */
  def getWriteConfig(dbase: String, collection: String, conf1: SparkConf, url: String) = {
    import scala.collection.JavaConversions._
    import scala.collection.mutable._
    val opt: java.util.Map[String, String] = HashMap("writeConcern.w" -> "majority", "writeConcern.journal" -> "false", "maxBatchSize" -> "1000")
    WriteConfig.create(dbase, collection, url, 1, WriteConcernConfig.create(conf1).withOptions(opt).writeConcern)
  }

  /**
   * @param document
   * @param sparkConf
   * @param sparkContext
   */
  def updateBatchCollection(mongoDataBase: String, document: Document, batchMemberCollectionName: String, sparkConf: SparkConf, sparkContext: SparkContext, url: String) {

    val writeConfig = getWriteConfig(mongoDataBase, batchMemberCollectionName, sparkConf, url)
    val documents = sparkContext.parallelize(Seq(document))
    MongoSpark.save(documents, writeConfig)
  }

  /**
   * @param batchId
   * @param batchExecStartDate
   * @param batchExecEndDate
   * @param startUpdateDate
   * @param endLastUpdateDate
   * @return
   */
  def createBatchDocument(batchId: String, batchExecStartDate: Date, batchExecEndDate: Date, startUpdateDate: Date, endLastUpdateDate: Date): Document = {

    val batchCollectionDocument: Document = new Document
    batchCollectionDocument.append("batch_id", batchId)
    batchCollectionDocument.append("batch_start_date", batchExecStartDate)
    batchCollectionDocument.append("batch_end_date", batchExecEndDate)
    batchCollectionDocument.append("last_update_start_date", startUpdateDate);
    batchCollectionDocument.append("last_update_end_date", endLastUpdateDate);
    batchCollectionDocument;
  }
}