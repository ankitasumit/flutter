package anthem.ehub.bds.ingest.mongo.db

import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.bson.Document

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConcernConfig
import com.mongodb.spark.config.WriteConfig
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import anthem.ehub.bds.utils.MongoBDSConfig

/**
 *
 * This scala code reads the data from ELIGIBILITY_CVRG_<SRC_TYPE> where SRC_TYPE - WGS,STAR or NASCO based on the type_cnt
 * Ref: To eligibility_type_cnt_query in app.conf
 * Once the data is read based on each contract code creates Document object
 * And then finally writes the data to MongoDB
 *
 * Spark Submit command for Local
 * spark-submit --conf spark.ui.port=10101 --master local[*]  --conf spark.driver.memory=4G --conf spark.driver.cores=4 --conf
 * spark.broadcast.compress=true --jars mongo-java-driver-3.2.2.jar,mongo-spark-connector_2.10-1.1.0.jar,ojdbc6.jar
 * --conf spark.driver.extraClassPath=/home/af55641/HiveToMongoDB* --conf spark.executor.extraClassPath=/home/af55641/HiveToMongoDB*
 *  --class anthem.ehub.bds.transform.WriteToCoverageCollection if_transform2-1-SNAPSHOT.jar "" (When argument is passed as blank it creates all 3 collections)
 *
 *  Spark Submit command for cluster
 * spark-submit --conf spark.ui.port=10101 --master yarn --executor-cores 10 --executor-memory 10G --num-executors 4
 * --conf spark.driver.memory=10G --conf spark.driver.cores=6 --conf spark.broadcast.compress=true --jars
 * mongo-java-driver-3.2.2.jar,mongo-spark-connector_2.10-1.1.0.jar,ojdbc6.jar,spark-csv_2.10-1.4.0.jar,commons-csv-1.4.jar
 * --conf spark.driver.extraClassPath=/home/af55641/HiveToMongoDB* --conf spark.executor.extraClassPath=/home/af55641/HiveToMongoDB*
 * --class anthem.ehub.bds.transform.WriteToCoverageCollection if_transform2-1-SNAPSHOT.jar ""
 *
 * //TODO Get rid off the Println and add the Log Statements
 *
 *
 * @author AF55641
 *
 */
object WriteToCoverageCollection {
  @transient lazy val log1 = org.apache.log4j.LogManager.getLogger("anthem.ehub.bds.ingest.mongo.db.WriteToCoverageCollection")

  /*def main(args: Array[String]): Unit = {
    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)
    log.info("Started ***********")

    val sparkConf = new SparkConf().setAppName("HiveToMongoDB").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.ui.port", "3012").set("spark.rdd.compress", "true").set("spark.scheduler.mode", "FAIR")
      .set("spark.dynamicAllocation.enabled", "true").set("spark.shuffle.service.enabled", "true")

    sparkConf.registerKryoClasses(Array(Class.forName("anthem.ehub.bds.ingest.mongo.db.WriteToCoverageCollection")))
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sparkContext)
    val appConf = ConfigFactory.load()
    val envType = args(0).toLowerCase()
    //       val HIVE_DATABASE = appConf.getString("bds.hive.hive_db_"+envType)
    //       sqlContext.sql(s"use $HIVE_DATABASE")
    println("Mongo Database " + appConf.getString("bds.mongodb.mongodb_uri_print_" + envType))
    //       val srcList = appConf.getStringList("bds.eligibility.sources")
    val srcList = MongoBDSConfig.getConfigDetails(envType, "eligibilityHiveToMongoDB", appConf)
    if ("All".equalsIgnoreCase(args(1))) {
      import scala.collection.JavaConversions._
      srcList.foreach(srcObj => {
        val HIVE_DATABASE = srcObj.getString("hive_DB")
        sqlContext.sql(s"use $HIVE_DATABASE")
        println(s"Hive Database $HIVE_DATABASE")
        processDF(appConf, sqlContext, sparkConf, srcObj, envType)
        println("Mongo DB write is done for " + srcObj.getString("code"))
      })
    } else {
      val srcObj = MongoBDSConfig.getConfDetailKey(envType, "code", args(1), appConf)
      if (srcObj != null) {
        println("Argument passed is " + args(1))
        val HIVE_DATABASE = srcObj.getString("hive_DB")
        sqlContext.sql(s"use $HIVE_DATABASE")
        println(s"Hive Database $HIVE_DATABASE")
        processDF(appConf, sqlContext, sparkConf, srcObj, envType)
      } else {
        println("Not a valid source code");
      }
    }
  }

  *//**
   *
   * This method reads the data from CVRG Type Tables and writes the data to mongo db
   * @param appConf
   * @param sqlContext
   * @param sparkConf
   * @param srcType
   *//*
  def processDF(appConf: Config, sqlContext: HiveContext, sparkConf: SparkConf, srcObj: Document, envType: String) = {

    println("Inside processDF")
    //        val eligibilityTypeCntList = appConf.getString("bds.eligibilityQueries.eligibility_type_cnt").split(",")
    val eligibilityTypeCntList = srcObj.getString("eligibility_type_cnt").split(",")
    val srcType = srcObj.getString("code")

    var masterDocument: RDD[(String, Document)] = null
    for (elgiType <- eligibilityTypeCntList) {
      val eligibilityType = elgiType.split("~")
      //          val query = appConf.getString("bds.eligibilityQueries.eligibility_type_cnt_query").replace("<SRC_TYPE>",srcType).replace("<CVRG_TYPE>", eligibilityType(0)).replace("<CVRG_ELEMENT_ATTR>",eligibilityType(1))
      val query = srcObj.getString("query").replace("<SRC_TYPE>", srcType).replace("<CVRG_TYPE>", eligibilityType(0)).replace("<CVRG_ELEMENT_ATTR>", eligibilityType(1))
      val cvrgTypeDataFrame = sqlContext.sql(query)
      val cvrgTypeDataDocument = cvrgTypeDataFrame.map(rowData => (rowData.getString(rowData.fieldIndex("cntrct_cd")), getCVRGTypeDocument(rowData, appConf, eligibilityType(0), eligibilityType(1), srcObj)))
      if (masterDocument != null) {
        masterDocument = masterDocument.union(cvrgTypeDataDocument)
      } else {
        masterDocument = cvrgTypeDataDocument
      }
      println("Done union with " + eligibilityType)
    }

    val reducdedContrctDocuments = masterDocument.groupByKey().map(keyValuePair => merge(keyValuePair._1, keyValuePair._2))

    val URI = appConf.getString("bds.mongodb.mongodb_uri_" + envType)
    val wc = getWriteConfig(appConf.getString("bds.mongodb.mongodb_" + envType), srcObj.getString("collection_name"), sparkConf, URI)
    MongoSpark.save(reducdedContrctDocuments, wc)

    println("End of creating rdd with document")
  }

  *//**
   * @param rowData
   * @param appConf
   * @param eligibilityType
   * @return
   *//*
  def getCVRGTypeDocument(rowData: Row, appConf: Config, eligibilityType: String, elementAttribute: String, srcObj: Document) = {
    println(s"Inside getCVRGTypeDocument $eligibilityType $elementAttribute")
    val rootDocument = new Document();
    val attributeDocument = new Document();
    //     val attrList = appConf.getString("bds.eligibilityQueries.cvrg_type_attrList").split(",")
    val attrList = srcObj.getString("cvrg_type_attrList").split(",")
    for (attrName <- attrList) {
      val atName = attrName.trim()
      val value = rowData.get(rowData.fieldIndex(atName))
      if (value != null) {
        if (value.isInstanceOf[java.sql.Date]) {
          attributeDocument.append(atName, (new java.util.Date(rowData.getTimestamp(rowData.fieldIndex(atName)).getTime())).toString())
        } else if (value.isInstanceOf[java.sql.Timestamp]) {
          attributeDocument.append(atName, (new java.util.Date(rowData.getTimestamp(rowData.fieldIndex(atName)).getTime())).toString())
        } else {
          attributeDocument.append(atName, value)
        }
      }
    }
    val typeRootDocument = new Document //planTypeDocument
    val typeNameDocument = new Document //MED
    val typeAttributeValue = rowData.getString(rowData.fieldIndex(elementAttribute))
    typeNameDocument.append(typeAttributeValue, attributeDocument)
    //typeRootDocument.append(eligibilityType, typeNameDocument)
    //println("The document of eligibilityType --"+eligibilityType+" json Type is "+typeNameDocument.toJson())
    rootDocument.append(elementAttribute, typeNameDocument)
  }

  *//**
   * Creates the Document object and appends the Type Document
   * @param key
   * @param childDocument
   * @param rootDocument
   * @return
   *//*
  def appendChildDocument(key: String, childDocument: Document, rootDocument: Document) = {
    val typeDocument: Document = rootDocument.get(key).asInstanceOf[Document];
    val document1 = childDocument.get(key).asInstanceOf[Document] //plandocument
    val medDocumentKey = document1.keySet().iterator().next(); //MedKey
    val medDocument = document1.get(medDocumentKey).asInstanceOf[Document]
    typeDocument.append(medDocumentKey, medDocument)
  }

  *//**
   * Merges the contract code and other documents related to contract code
   * @param cntrctCode
   * @param documents
   * @return
   *//*
  def merge(cntrctCode: String, documents: Iterable[Document]): Document = {
    val rootDocument = new Document
    rootDocument.append("cntrct_cd", cntrctCode)
    println("Done group by")
    documents.foreach(document => {
      if (document.containsKey("plan_type_trgt_cd_val_txt")) {
        val key = "plan_type_trgt_cd_val_txt"
        println(s"$key")
        if (rootDocument.containsKey(key)) {
          rootDocument.append(key, appendChildDocument(key, document, rootDocument))
        } else {
          rootDocument.append(key, document.get(key))
        }
      }
      if (document.containsKey("cmbnd_cvrg_1_trgt_cd_val_txt")) {
        val key = "cmbnd_cvrg_1_trgt_cd_val_txt"
        println(s"$key")
        if (rootDocument.containsKey(key)) {
          rootDocument.append(key, appendChildDocument(key, document, rootDocument))
        } else {
          rootDocument.append(key, document.get(key))
        }
      }
      if (document.containsKey("cmbnd_cvrg_2_trgt_cd_val_txt")) {
        val key = "cmbnd_cvrg_2_trgt_cd_val_txt"
        println(s"$key")
        if (rootDocument.containsKey(key)) {
          rootDocument.append(key, appendChildDocument(key, document, rootDocument))
        } else {
          rootDocument.append(key, document.get(key))
        }
      }
      if (document.containsKey("cmbnd_cvrg_3_trgt_cd_val_txt")) {
        val key = "cmbnd_cvrg_3_trgt_cd_val_txt"
        println(s"$key")
        if (rootDocument.containsKey(key)) {
          rootDocument.append(key, appendChildDocument(key, document, rootDocument))
        } else {
          rootDocument.append(key, document.get(key))
        }
      }

      if (document.containsKey("cmbnd_cvrg_4_trgt_cd_val_txt")) {
        val key = "cmbnd_cvrg_4_trgt_cd_val_txt"
        println(s"$key")
        if (rootDocument.containsKey(key)) {
          rootDocument.append(key, appendChildDocument(key, document, rootDocument))
        } else {
          rootDocument.append(key, document.get(key))
        }
      }

      if (document.containsKey("cmbnd_cvrg_5_trgt_cd_val_txt")) {
        val key = "cmbnd_cvrg_5_trgt_cd_val_txt"
        println(s"$key")
        if (rootDocument.containsKey(key)) {
          rootDocument.append(key, appendChildDocument(key, document, rootDocument))
        } else {
          rootDocument.append(key, document.get(key))
        }
      }

    })
    //println(rootDocument.toJson())
    rootDocument
  }
  *//**
   * @param dbase
   * @param collection
   * @param conf1
   * @return
   *//*
  def getWriteConfig(dbase: String, collection: String, conf1: SparkConf, url: String) = {
    import scala.collection.JavaConversions._
    import scala.collection.mutable._
    val opt: java.util.Map[String, String] = HashMap("writeConcern.w" -> "0", "writeConcern.journal" -> "false")
    //WriteConfig.create(dbase, collection, "mongodb://srcbdsrw:3Ds%40nthem@va33dlvehb300.wellpoint.com:27017/DSOABDS", 1, WriteConcernConfig.create(conf1).withOptions(opt).writeConcern)
    WriteConfig.create(dbase, collection, url, 1, WriteConcernConfig.create(conf1).withOptions(opt).writeConcern)
  }
*/}