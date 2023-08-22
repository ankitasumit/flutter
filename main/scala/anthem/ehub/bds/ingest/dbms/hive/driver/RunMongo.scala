package anthem.ehub.bds.ingest.dbms.hive.driver

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.typesafe.config.ConfigFactory

import anthem.ehub.bds.utils.Utils.validate
import anthem.ehub.bds.ingest.dbms.hive.apps.FullRefreshImport
import anthem.ehub.bds.ingest.dbms.hive.apps.DeltaImport
import anthem.ehub.bds.ingest.dbms.hive.apps.RefineImport
import anthem.ehub.bds.utils.MongoBDSConfig
import java.util.Properties
import anthem.ehub.bds.utils.Encryption
import org.bson.Document
import anthem.ehub.bds.utils.Utils
import com.typesafe.config.Config
import org.apache.spark.sql.SQLContext
import java.util.Date
import org.joda.time.DateTime
import org.joda.time.Minutes
import org.apache.spark.sql.SparkSession

/**
 * This Scala code reads the configuration details from Mongo DB and creates the Parameter instance, iterates through the paramater objects and calls the
 * respective Class.process method based on the load type
 *
 * @author
 *
 */
object RunMongo {
  def process(envType: String, frequency: String, activityName: String) = {

    val appConf = ConfigFactory.load()
    println("Before record")

    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.ui.port", "3012").set("spark.rdd.compress", "true").set("spark.scheduler.mode", "FAIR")
      .set("spark.dynamicAllocation.enabled", "true").set("spark.shuffle.service.enabled", "true")
      .set("spark.debug.maxToStringFields","300")
    /* .set("spark.rdd.compress", "true").set("spark.scheduler.mode", "FAIR")
      .set("spark.shuffle.service.enabled", "true")
      .set("spark.shuffle.compress", "true")
      .set("spark.rpc.io.serverThreads", "64")
      .set("spark.executor.extraJavaOptions","-XX:+PrintGCDetails -XX:+PrintGCTimeStamp -XX:ParallelGCThreads=4 -XX:+UseParallelGC")
      */
    conf.registerKryoClasses(Array(Class.forName("anthem.ehub.bds.ingest.dbms.hive.driver.RunMongo"),
      Class.forName("anthem.ehub.bds.ingest.dbms.hive.apps.FullRefreshImport"),
      Class.forName("anthem.ehub.bds.ingest.dbms.hive.apps.AppTrait"),
      Class.forName("anthem.ehub.bds.utils.Parameter"),
      Class.forName("anthem.ehub.bds.utils.Encryption"),
      Class.forName("anthem.ehub.bds.utils.Utils")))

    val sparkContext = new SparkContext(conf)
    val uri = appConf.getString("bds.mongodb.mongodb_uri_" + envType.toLowerCase) + "." + frequency
    println("Mongo URI" + appConf.getString("bds.mongodb.mongodb_uri_print_" + envType.toLowerCase) + "." + frequency)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL")
      .getOrCreate()
   // val sqlContext = new HiveContext(sparkContext)
    execute(envType, activityName, frequency, appConf, sparkContext, spark)
  }

  /**
   * @param envType
   * @param activityName
   * @param frequencyCollectionName
   * @param appConf
   * @param sparkContext
   * @param hiveContext
   */
  def execute(envType: String, activityName: String, frequencyCollectionName: String, appConf: Config, sparkContext: SparkContext, hiveContext: SparkSession) = {

    val configCollection = Utils.getMongoCollectionWithAppConf(envType, appConf.getString("bds.mongodb.config_collection"), appConf)
    val activityDocument = Utils.getDataFromCollection("name", activityName, configCollection) // DBToHiveLoad_parallel_1

    val frequencyCollection = Utils.getMongoCollectionWithAppConf(envType, frequencyCollectionName, appConf) // daily_load collection 

    val propEhub = loadProperties(MongoBDSConfig.getConfigDetail(envType, "DBTOHIVE_EHUB", appConf)) //DBToHiveEhub document DB Deails
    val propBnft = loadProperties(MongoBDSConfig.getConfigDetail(envType, "DBTOHIVE_BNFT", appConf)) ////DBToHiveEhub document  document
    val propPlan = loadProperties(MongoBDSConfig.getConfigDetail(envType, "DBTOHIVE_PLAN", appConf)) //
    val propPrsn = loadProperties(MongoBDSConfig.getConfigDetail(envType, "DBTOHIVE_PERSON", appConf))
    
    var properties: Properties = null

    val tables = activityDocument.getString("tableNames").split(",") //

    tables.foreach(tableName => {
      val startTime = new DateTime(new Date())
      println(s"Processing Table ********** ${tableName}")
      val document = Utils.getDataFromCollection("Source", tableName, frequencyCollection) //"Source", EHUB_MBR_DOMN.EHUB_MBR_ID, daily_load
      val parameter = validate(document) // case class 
      if (parameter.dbType == "DBTOHIVE_BNFT") {
        properties = propBnft
      } else if (parameter.dbType == "DBTOHIVE_EHUB") {
        properties = propEhub
      }else if (parameter.dbType == "DBTOHIVE_PLAN") {
        properties = propPlan
      }else if (parameter.dbType == "DBTOHIVE_PERSON") {
        properties = propPrsn
      }
          
      parameter.Loadtype match {
        case "FULLLOAD" => FullRefreshImport.process(parameter, properties, sparkContext, hiveContext)
        case "REFINE"   => RefineImport.process(parameter, properties, sparkContext, hiveContext)
        case "DELTA"    => DeltaImport.process(parameter, properties, sparkContext, hiveContext)
        case _          => println("Invalid import type")
      }
      println(s" To load the table ${tableName} it took  " + Minutes.minutesBetween(startTime, new DateTime(new Date())).getMinutes + " minutes")

    })

  }

  /**
   * @param doc
   * @return
   */
  def loadProperties(doc: Document) = {
    val dbserverurl = doc.getString("dbserverurl")
    val jdbcdriver = doc.getString("jdbcdriver")
    val dbuserid = doc.getString("dbuserid")
    val encryptedPassword = doc.getString("dbpassword")
    val dbpassword = Encryption.decrypt(encryptedPassword)
    val partitions = doc.getString("numpartitions")
    val hiveFileFormat = doc.getString("hivedataformat")
    val partitions_cols = doc.getString("partitions_cols")
    val properties = new Properties()
    properties.setProperty("driver", jdbcdriver)
    properties.setProperty("user", dbuserid)
    properties.setProperty("password", dbpassword)
    properties.setProperty("numpartitions", partitions)
    properties.setProperty("hivedataformat", hiveFileFormat)
    properties.setProperty("dbserverurl", dbserverurl)
    properties.setProperty("partitions_cols", partitions_cols)
    println(" properties " + properties.getProperty("hivedataformat"));
    properties
  }
} 