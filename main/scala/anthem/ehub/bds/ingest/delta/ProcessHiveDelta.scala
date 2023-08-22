  package anthem.ehub.bds.ingest.delta
  
  import anthem.ehub.bds.utils.Encryption
  import anthem.ehub.bds.utils.Parameter
  import anthem.ehub.bds.utils.Utils
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
  
  import anthem.ehub.bds.utils.MongoBDSConfig
  import org.apache.spark.sql.SparkSession
  
  object ProcessHiveDelta {
        
     @transient lazy val log = org.apache.log4j.LogManager.getLogger("anthem.ehub.bds.transform.ProcessHiveDelta")
  
       val APPEND_BATCH_ID_STRING_TEMPLATE = "\"<BATCH_ID>\" as batch_id, \"<INDICATOR>\" as Indicator, cast('<BATCH_DATE>' as timestamp) as batch_date"
  
    
       def main(args: Array[String]): Unit = {
       
       val log = LogManager.getRootLogger
       log.setLevel(Level.ERROR)
     val appConf = ConfigFactory.load()   
    val doc = MongoBDSConfig.getConfDetailKey(args(0),"Customertype","sparkconfig", appConf)
    var noExecutor = "20"
    var cores = "1"
    var executorMemory = "10G"
    var driverMemory = "20G"
    if(!doc.isEmpty()){
      noExecutor = doc.getString("noExecutor")
      cores = doc.getString("cores")
      executorMemory = doc.getString("executorMemory")
      driverMemory = doc.getString("driverMemory")
      
      println("Config taken from mongo configuration  "+noExecutor + " "+ cores + " "+ executorMemory  + "  "+ driverMemory)
     }
    
    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.ui.port", "10101")
      .set("spark.rdd.compress", "true")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.broadcast.compress", "true")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.shuffle.service.enabled", "true")
     // .set("spark.debug.maxToStringFields", "100")
      .setMaster("yarn")
      .set("spark.driver.memory", driverMemory)
      .set("spark.io.compression.codec", "lz4")
      .setExecutorEnv("num-executors", noExecutor)
      .setExecutorEnv("executor-cores", cores)
      .setExecutorEnv("executor-memory", executorMemory)
      .set("spark.yarn.queue", "pca_yarn")
      
      
      sparkConf.registerKryoClasses(Array(classOf[Process],classOf[UpdateDeltaInfo]))
      val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLocalProperty("spark.scheduler.pool", "pca_yarn") 
      val sqlContext = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .config("spark.sql.shuffle.partitions","200")
        .config("spark.sql.files.maxPartitionBytes","134217728")
        .getOrCreate()
      
     
      val envType = args(0).toLowerCase()
      val BDS_TYPES = "bds.types."
      val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val batchDate = dateFormat.format(new java.util.Date())
      
      println(s"Mongo Url "+appConf.getString("bds.mongodb.mongodb_uri_print_"+envType))
      
      if("All".equalsIgnoreCase(args(1))) {
          println("Delta works on bds.delta.delta_collections of configuration");
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
     
     def processDelta (deltaInfo:UpdateDeltaInfo) = {
        val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
        val todayDate = format.format(new java.util.Date())+" 00:00:00.0"
        val mongoDatabase = deltaInfo.appConfig.getString("bds.mongodb.mongodb_"+deltaInfo.envType)
        Utils.executeHQLQueries(deltaInfo.sqlContext,mongoDatabase,deltaInfo.appConfig.getString("bds.mongodb.mongodb_uri_"+deltaInfo.envType)+"?authMechanism=SCRAM-SHA-1", 
            deltaInfo.memberObj.getString("code"),"HQL")

        val query = deltaInfo.memberObj.getString("query").replace("<HIVE_TABLE>", deltaInfo.hiveTable)
        val distinctField = deltaInfo.memberObj.getString("distinctField")
        val tempquery = deltaInfo.memberObj.getString("query").replace("<HIVE_TABLE>", deltaInfo.hiveTable+"_temp")
        println("Data is about to read from Mongo DB "+new Date())
        val hiveDatadf = getHiveData(tempquery, deltaInfo.sqlContext)
       
        println("temp table read")
        val hiveDeltaDatadf = getHiveData(query, deltaInfo.sqlContext)
        println("delta table read")
        val dropColHiveDatadf = hiveDatadf.drop("Indicator").drop("batch_date")
        println("dropped column - temp")
        val deltadf = hiveDeltaDatadf.drop("Indicator").drop("batch_date").except(dropColHiveDatadf)
        println("dropped column - delta")
        import deltaInfo.sqlContext.implicits._
        
        val deltahcids = deltadf.select(distinctField).distinct().rdd.map(r => r(0)).collect().toList.asInstanceOf[List[String]]
        println("Get delta hcids")
        val updatedhcids = hiveDatadf.where(hiveDatadf.col(distinctField).isin(deltahcids: _*)).distinct().rdd.map(r => r(0)).collect().toList.asInstanceOf[List[String]]
                println("Get updated hcids")
        val appendedhcids = deltahcids.diff(updatedhcids)
                println("Get appended hcids")
        val nondeltaDF = hiveDatadf.where(!hiveDatadf.col(distinctField).isin(deltahcids: _*)).withColumn("batch_date",lit(todayDate))
                println("non delta df")
        val appendedDF = hiveDeltaDatadf.where(hiveDeltaDatadf.col(distinctField).isin(deltahcids: _*)).withColumn("Indicator",lit("I")).withColumn("batch_date",lit(todayDate)).withColumn("Indicator", when(hiveDeltaDatadf.col(distinctField).isin(appendedhcids: _*), lit("A")).otherwise($"Indicator")).withColumn("Indicator", when(hiveDeltaDatadf.col(distinctField).isin(updatedhcids: _*), lit("U")).otherwise($"Indicator")).withColumn("Indicator", when($"enrlmnt_trmntn_dt"<todayDate, "T").otherwise($"Indicator"))
                println("Get appended df")
       
        nondeltaDF.union(appendedDF).createOrReplaceTempView("temptable")
                println("Union")
        
        //val finaldf = hiveDeltaDatadf.withColumn("Indicator",lit("I")).withColumn("Indicator", when($distinctField.isin(appendedhcids: _*), lit("A")).otherwise($"Indicator")).withColumn("Indicator", when($distinctField.isin(updatedhcids: _*), lit("U")).otherwise($"Indicator")).withColumn("Indicator", when($"enrlmnt_trmntn_dt"<todayDate, "T").otherwise($"Indicator"))
        val finalquery = "insert overwrite table <mytable> select * from temptable".replace("<mytable>", deltaInfo.hiveTable)
        getHiveData(finalquery, deltaInfo.sqlContext)
    }
    
  
    def getHiveData(query: String, sqlContext: SparkSession) = {
      val dataFrame = sqlContext.sql(query)
      dataFrame
    }
     
  }