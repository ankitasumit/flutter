package anthem.ehub.bds.ingest.delta



import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.bson.Document

import com.mongodb.MongoClient
import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.DataFrame
import com.mongodb.spark.config.WriteConcernConfig
import com.mongodb.spark.config.WriteConfig
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.functions.lit
import com.typesafe.config.ConfigObject
import java.util.Map.Entry
import com.typesafe.config.ConfigValue
import scala.collection.JavaConverters._
import org.apache.spark.io.SnappyCompressionCodec
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import anthem.ehub.bds.utils.Utils
import anthem.ehub.bds.utils.MongoBDSConfig
import org.apache.spark.sql.SparkSession

/**
 * 
 * @author AF31935
 *
 */

 // spark-submit --conf spark.ui.port=10101 --master yarn --executor-cores 5 --executor-memory 10G --num-executors 5 --conf spark.driver.memory=4G 
//--conf spark.driver.cores=10 --conf spark.broadcast.compress=true --jars mongo-java-driver-3.2.2.jar,mongo-spark-connector_2.10-1.1.0.jar 
//--conf spark.driver.extraClassPath=/home/af31935/Ingestion/* --conf spark.executor.extraClassPath=/home/af31935/Ingestion/* 

final case class UpdateDeltaInfo(collectionName:String, hiveTable:String, sparkContext:SparkContext, 
                                  sqlContext:SparkSession, sparkConfig:SparkConf, appConfig:Config,batchDate:String, envType: String, memberObj:Document)

/**
 * This class is used to perform the delta ingestion between Hive and Delta Tables
 * 
 *
 */
object ProcessDelta {

  @transient lazy val log = org.apache.log4j.LogManager.getLogger("anthem.ehub.bds.transform.ProcessDelta")

  val APPEND_BATCH_ID_STRING_TEMPLATE = "\"<BATCH_ID>\" as batch_id, \"<INDICATOR>\" as Indicator, cast('<BATCH_DATE>' as timestamp) as batch_date"

  def main(args: Array[String]): Unit = {

    val log = LogManager.getRootLogger
    log.setLevel(Level.ERROR)

    val sparkConf = new SparkConf().setAppName("ProcessDelta").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.ui.port", "3012").set("spark.rdd.compress", "true").set("spark.scheduler.mode", "FAIR")
      /*.set("spark.dynamicAllocation.enabled", "true")*/.set("spark.shuffle.service.enabled", "true")
      .set("spark.shuffle.compress", "true")
      
    
    sparkConf.registerKryoClasses(Array(classOf[Process],classOf[UpdateDeltaInfo]))
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .getOrCreate()
    
    val appConf = ConfigFactory.load()
    val envType = args(0).toLowerCase()
    val BDS_TYPES = "bds.types."
    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val HIVE_DATABASE = appConf.getString("bds.hive.hive_db_"+envType)
    val batchDate = dateFormat.format(new java.util.Date())
    
//    println(s"Hive Database $HIVE_DATABASE")
    println(s"Mongo Url "+appConf.getString("bds.mongodb.mongodb_uri_print_"+envType))
    
//    sqlContext.sql(s"use $HIVE_DATABASE")
    
    if("All".equalsIgnoreCase(args(1))) {
        println("Delta works on bds.delta.delta_collections of configuration");
//        val typeList = appConf.getStringList("bds.delta.delta_collections")
        val typeList = MongoBDSConfig.getConfigDetails(envType, "deltaToHive", appConf)
        import scala.collection.JavaConversions._
        typeList.foreach(memberObj => {
            val HIVE_DATABASE = memberObj.getString("hive_DB")
  	        sqlContext.sql(s"use $HIVE_DATABASE")
  	        println(s"Hive Database $HIVE_DATABASE")
            println("Processing of "+ memberObj.getString("hiveDeltaKey"))
            processDelta(UpdateDeltaInfo(memberObj.getString("collection_name"),memberObj.getString("hiveTable"),sparkContext,sqlContext,sparkConf,appConf,batchDate,envType,memberObj))  	  
        })
        
    } else {
        println("Collection name is " +args(1))
        val memberObj = MongoBDSConfig.getConfDetailKey(envType, "hiveDeltaKey", args(1), appConf)
        val HIVE_DATABASE = memberObj.getString("hive_DB")
        sqlContext.sql(s"use $HIVE_DATABASE")
        println(s"Hive Database $HIVE_DATABASE")
        println("Processing of "+ memberObj.getString("hiveDeltaKey"))
        processDelta(UpdateDeltaInfo(memberObj.getString("collection_name"),memberObj.getString("hiveTable"),sparkContext,sqlContext,sparkConf,appConf,batchDate,envType,memberObj))
    }
  }
  /**
 * @param deltaInfo
 */
def processDelta (deltaInfo:UpdateDeltaInfo) = {
        val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
        val todayDate = format.format(new java.util.Date())+" 00:00:00.0"
        val query = deltaInfo.memberObj.getString("query").replace("<HIVE_TABLE>", deltaInfo.hiveTable)
        val mongoDatabase = deltaInfo.appConfig.getString("bds.mongodb.mongodb_"+deltaInfo.envType)
        Utils.executeHQLQueries(deltaInfo.sqlContext,mongoDatabase,deltaInfo.appConfig.getString("bds.mongodb.mongodb_uri_"+deltaInfo.envType)+"?authMechanism=SCRAM-SHA-1", 
            deltaInfo.memberObj.getString("code"),"HQL")
        val hiveData = getHiveData(query, deltaInfo.sqlContext)
        val uri = deltaInfo.appConfig.getString("bds.mongodb.mongodb_uri_"+deltaInfo.envType)+"."+deltaInfo.collectionName
        val readConfig = ReadConfig(Map("uri" -> uri))
        println("Data is about to read from Mongo DB "+new Date())
        val mongoData = MongoSpark.load(deltaInfo.sparkContext, readConfig).toDF()
       
        println("Data read from Mongo DB "+new Date())
        val mongoDataCol = mongoData.drop("_id").drop("Indicator").drop("batch_id").drop("batch_date")
        println("Dropped columns batch Id, indicator and id")
        
        val template = APPEND_BATCH_ID_STRING_TEMPLATE.replace("<BATCH_ID>", deltaInfo.sparkContext.applicationId).replace("<BATCH_DATE>", deltaInfo.batchDate)
       
//        lazy val mapValues : Map[String, String] = getColumnsToBeRenamed(deltaInfo.appConfig , "column_rename_map" )
        lazy val mapValues : Map[String, String] = getColumnsToBeRenamed(deltaInfo.memberObj.getString("column_rename") )
        val colKeys  = mapValues.keySet.seq.toList.toArray
       var mongoDatawithArrangedCol = mongoDataCol.select(colKeys.head, colKeys.tail: _*)
        for ((key,value) <- mapValues) {
           mongoDatawithArrangedCol = mongoDatawithArrangedCol.withColumnRenamed(key,value)
         }
        
         mongoDatawithArrangedCol = mongoDatawithArrangedCol.select(mongoDatawithArrangedCol("mongoMemberId"), mongoDatawithArrangedCol("mongoContrctCode"));
        
        import deltaInfo.sqlContext.implicits._
        val hiveDataWithNull = hiveData.withColumn("NewContractCode", when($"cntrct_cd".isNull, "NULLVAL").otherwise($"cntrct_cd")).
                                withColumn("concatCol", concat($"member_id",$"NewContractCode"))
        
        val mongoDataWithNull = mongoDatawithArrangedCol.withColumn("NewContractCode", when($"mongoContrctCode".isNull, "NULLVAL").otherwise($"mongoContrctCode")).
                                withColumn("mtConcatCol", concat($"mongoMemberId",$"NewContractCode"))
        
        hiveDataWithNull.createOrReplaceTempView("hiveTab");
        mongoDataWithNull.createOrReplaceTempView("mongoTab");
              
        val appendDataJoinQuery = deltaInfo.memberObj.getString("join_query")
        val appendQueryString = appendDataJoinQuery. replace("<APPEND_DATA>", template).replace("<INDICATOR>","A")
        val newDF = deltaInfo.sqlContext.sql(appendQueryString).withColumn("Indicator", when($"enrlmnt_trmntn_dt"<todayDate, "T").otherwise($"Indicator"))
        //MongoSpark.save(newDF.drop("NewContractId").drop("concatCol"),getWriteConfig(deltaInfo.appConfig.getString("bds.mongodb.mongodb"), "delta_collection", deltaInfo.sparkConfig, deltaInfo.appConfig.getString("bds.mongodb.mongodb_uri")))
        //TODO Uncomment the below line comment the above line
        MongoSpark.save(newDF,getWriteConfig(deltaInfo.appConfig.getString("bds.mongodb.mongodb_"+deltaInfo.envType), deltaInfo.collectionName, deltaInfo.sparkConfig, deltaInfo.appConfig.getString("bds.mongodb.mongodb_uri_"+deltaInfo.envType)))
        
        //********************************************************Update**************************************
        val finalUpdateDF = processUpdate(hiveData,mongoDataCol, mongoData, mapValues,deltaInfo, template,deltaInfo.sqlContext,todayDate)
        if(finalUpdateDF.isDefined){
         
        	//MongoSpark.save(finalUpdateDF.get, getWriteConfig(deltaInfo.appConfig.getString("bds.mongodb.mongodb"), "delta_collection", deltaInfo.sparkConfig, deltaInfo.appConfig.getString("bds.mongodb.mongodb_uri")))
          //TODO Uncomment the below line comment the above line 
          MongoSpark.save(finalUpdateDF.get, getWriteConfig(deltaInfo.appConfig.getString("bds.mongodb.mongodb_"+deltaInfo.envType), deltaInfo.collectionName, deltaInfo.sparkConfig, deltaInfo.appConfig.getString("bds.mongodb.mongodb_uri_"+deltaInfo.envType)))
        }
        println("Done saving of Updated Data ")
  }
  
  def processUpdate (hiveData:DataFrame, mongoDataCol:DataFrame,mongoData:DataFrame, mapValues:Map[String,String], deltaInfo:UpdateDeltaInfo, template:String, sqlContext:SparkSession, todayDate:String) = {
        try {
            val hiveColumnsList = hiveData.columns.toList
            
            println("Hive columns are "+hiveColumnsList.size)
            
            val mongoColumnsList = mongoDataCol.columns.toList
            println("Mongo columns are "+mongoColumnsList.size)
            val missingColumnsList = hiveColumnsList.filterNot(mongoColumnsList.contains(_))
            println("Missing Mongo columns are "+missingColumnsList.size)
            val convertMissingCols = missingColumnsList.map(colName =>
                "null as "+colName
            )
            var convertMongoColumnList = mongoColumnsList.mkString(",")
            if(!convertMissingCols.isEmpty) {
             convertMongoColumnList = convertMongoColumnList+","+ convertMissingCols.mkString(",")
            }
            
            mongoData.createOrReplaceTempView("mongTable1")
            val queryString = "Select " + convertMongoColumnList + " from mongTable1 ";
            println(queryString)
            var mongoDatawithNewArrangedCol1 = sqlContext.sql(queryString)
            mongoDatawithNewArrangedCol1.createOrReplaceTempView("mongTable2")
            val mongoDatawithNewArrangedColData = sqlContext.sql("Select " +hiveColumnsList.mkString(",")+ " from mongTable2 ")
            println("Before Except is done "+ new Date())		
            

           //If Hive has date fields and Mongo has time stamp fields, then Mongo Time Stamp fields need to convert to date, to make the schemas consistent
           //Commented the below code because in Hive member info table there are no date field columns
           /*val mongoDateFields = mongoDatawithNewArrangedColData.dtypes.filter(colName => colName._2 == "TimestampType").map(tup => tup._1)
           
            val columnNames = hiveData.dtypes;
            
            val mongoTSColumns = columnNames.map(colName =>
            if (colName._2=="DateType" && mongoDateFields.contains(colName._1)) {
              colName._1
            })
           
            for (col <- mongoTSColumns) {
             
           }*/
            
            val updateData = hiveData.except(mongoDatawithNewArrangedColData)
            println("Except is done "+ new Date())
            
            val finalCols = "_id"+:mapValues.keySet.seq.toList.toArray
           
            var updateMongoData = mongoData.select(finalCols.head, finalCols.tail: _*)
            for ((key,value) <- mapValues) {
                   updateMongoData = updateMongoData.withColumnRenamed(key,value)
            }
            updateData.createOrReplaceTempView("hiveUpdateTable");
            updateMongoData.createOrReplaceTempView("mongoTable");
            
            val updateDataJoinQuery = deltaInfo.memberObj.getString("update_query")
            val updateQueryString = updateDataJoinQuery.replace("<UPDATE_DATA>", template).replace("<INDICATOR>", "U")
            import deltaInfo.sqlContext.implicits._
            val updatedDataWithId = deltaInfo.sqlContext.sql(updateQueryString).drop("mongoContrctCode").drop("mongoMemberId").drop("mongoEnrlmnt_trmntn_dt")
                                    .withColumn("Indicator", when($"enrlmnt_trmntn_dt"<todayDate, "T").otherwise($"Indicator"));
            
            println("End of processUpdate")
            Some(updatedDataWithId)
        } catch {
          case ex: Exception => {
            println("Error while processing the update ", ex.getCause.getMessage)
            ex.printStackTrace();
            None
          }
        } 
  }

 /* def printData(df:DataFrame) = {
      val collectedData  = df.toJSON.coalesce(1).collect().mkString("\n")
        val json = "[" + ("}\n".r replaceAllIn (collectedData, "},\n")) + "]"
        println(json)
  }
  */
  /**
   * Returns the RDD[Document] from passed collectionName
   * @param collectionName
   * @param sqlContext
   * @return
   */
  def getHiveData(query: String, sqlContext: SparkSession) = {
    val dataFrame = sqlContext.sql(query)
    dataFrame
  }
  
  
  def getColumnsToBeRenamed (value:String) ={
      import scala.collection.JavaConversions._
      var mapValues = Map[String, String]()
      
      for(valu <-value.split(",")){
        val mpval = valu.split(":")
        mapValues += (mpval(0) -> mpval(1))
        println(mpval(0) +  "\t"+ mpval(1) +"\t" + mapValues)
      }
      println(mapValues)
      
      mapValues
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
    val opt: java.util.Map[String, String] = HashMap("writeConcern.w" -> "0", "writeConcern.journal" -> "false")
    WriteConfig.create(dbase, collection, url, 1, WriteConcernConfig.create(conf1).withOptions(opt).writeConcern)
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