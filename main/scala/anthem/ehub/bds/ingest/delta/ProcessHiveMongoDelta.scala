package anthem.ehub.bds.ingest.delta

import java.util.Date



import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConcernConfig
import com.mongodb.spark.config.WriteConfig
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import anthem.ehub.bds.utils.MongoBDSConfig
import anthem.ehub.bds.utils.Utils
import org.apache.spark.sql.Column 

object ProcessHiveMongoDelta {

  @transient lazy val log = org.apache.log4j.LogManager.getLogger("anthem.ehub.bds.transform.ProcessHiveMongoDelta")

  val APPEND_BATCH_ID_STRING_TEMPLATE = "\"<BATCH_ID>\" as batch_id, \"<INDICATOR>\" as Indicator, cast('<BATCH_DATE>' as timestamp) as batch_date"

  def main(args: Array[String]): Unit = {
 
    val log = LogManager.getRootLogger
    log.setLevel(Level.ERROR)
    val appConf = ConfigFactory.load()
    val doc = MongoBDSConfig.getConfDetailKey(args(0), "Customertype", "sparkconfig", appConf)
    var noExecutor = "10"
    var cores = "1"
    var executorMemory = "20G"
    var driverMemory = "30G"
    if (!(doc == null)) {
      noExecutor = doc.getString("noExecutor")
      cores = doc.getString("cores")
      executorMemory = doc.getString("executorMemory")
      driverMemory = doc.getString("driverMemory")

      println("Config taken from mongo configuration  " + noExecutor + " " + cores + " " + executorMemory + "  " + driverMemory)
    }

    val sparkConf = new SparkConf().setAppName("TEST_DATA")
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
      .set("spark.sql.broadcastTimeout", "4800")
      .set("spark.yarn.queue", "ehub-xds_yarn")

    sparkConf.registerKryoClasses(Array(classOf[Process], classOf[UpdateDeltaInfo]))
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLocalProperty("spark.scheduler.pool", "ehub-xds_yarn")
    val sqlContext = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.sql.shuffle.partitions", "400")
      .getOrCreate()

      //.config("spark.sql.files.maxPartitionBytes", "134217728")
    //            .config("spark.sql.shuffle.partitions", "200")
    //      .config("spark.sql.files.maxPartitionBytes", "134217728")

    val envType = args(0).toLowerCase()
    val BDS_TYPES = "bds.types."
    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val batchDate = dateFormat.format(new java.util.Date())

    println(s"Mongo Url " + appConf.getString("bds.mongodb.mongodb_uri_print_" + envType))

    if ("All".equalsIgnoreCase(args(1))) {
      println("Delta works on bds.delta.delta_collections of configuration");
      val typeList = MongoBDSConfig.getConfigDetails(envType, "deltaToHive", appConf)

      typeList.foreach(memberObj => {
        val HIVE_DATABASE = memberObj.getString("hive_DB")
        sqlContext.sql(s"use $HIVE_DATABASE")
        println(s"Hive Database $HIVE_DATABASE")
        println("Processing of " + memberObj.getString("hiveDeltaKey"))
        processDelta(UpdateDeltaInfo(memberObj.getString("collection_name"), memberObj.getString("hiveTable"), sparkContext, sqlContext, sparkConf, appConf, batchDate, envType, memberObj))
      })

    } else {
      println("Collection name is " + args(1))
      val memberObj = MongoBDSConfig.getConfDetailKey(envType, "hiveDeltaKey", args(1), appConf)
      println(" memberobj "+  memberObj.toString()) 
      val HIVE_DATABASE = memberObj.getString("hive_DB")
      sqlContext.sql(s"use $HIVE_DATABASE")
      println(s"Hive Database $HIVE_DATABASE")
      println("Processing of " + memberObj.getString("hiveDeltaKey"))
      processDelta(UpdateDeltaInfo(memberObj.getString("collection_name"), memberObj.getString("hiveTable"), sparkContext, sqlContext, sparkConf, appConf, batchDate, envType, memberObj))
    }

  }

  def processDelta(deltaInfo: UpdateDeltaInfo) = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val todayDate = format.format(new java.util.Date()) + " 00:00:00.0"
    val mongoDatabase = deltaInfo.appConfig.getString("bds.mongodb.mongodb_" + deltaInfo.envType.toLowerCase)
    val URL = deltaInfo.appConfig.getString("bds.mongodb.mongodb_uri_" + deltaInfo.envType.toLowerCase)

    Utils.executeHQLQueries(deltaInfo.sqlContext, mongoDatabase, deltaInfo.appConfig.getString("bds.mongodb.mongodb_uri_" + deltaInfo.envType) + "?authMechanism=SCRAM-SHA-1",
      deltaInfo.memberObj.getString("code"), "HQL")

     def md5Hash(text: String): String =
      java.security.MessageDigest.getInstance("MD5").digest(text.trim.getBytes())
        .map(0xFF & _).map { "%02x".format(_) }.foldLeft("") { _ + _ }

    def hash_udf = udf(md5Hash _)
    
    
    //val columns = hiveDatadf.columns.mkString(",").split(",").map(r => new Column(r))
   //    hiveDatadf.withColumn("ss", hash_udf(concat_ws("", columns: _*)))
    //hiveDatadf.withColumn("ss", hash_udf(concat_ws("", columns(0),columns(1),columns(2))))
    
    val collectionName = deltaInfo.memberObj.getString("collection_name")
    val query = deltaInfo.memberObj.getString("query").replace("<HIVE_TABLE>", deltaInfo.hiveTable)
    val distinctField = deltaInfo.memberObj.getString("distinctField")
    val tempquery = deltaInfo.memberObj.getString("query").replace("<HIVE_TABLE>", deltaInfo.hiveTable+"_yesterday")
    println("Data is about to read from Mongo DB " + new Date())
    val yesDatadf = getHiveData(tempquery, deltaInfo.sqlContext)
    println(" Maint table " + collectionName)

    println("temp table read")
    val todayDatadf = getHiveData(query, deltaInfo.sqlContext)
    println("delta table read")
    todayDatadf.show(5)
    yesDatadf.createOrReplaceTempView("originalyesterdaydata")
    var dropColyesdf:DataFrame = null
    var dropColtodaydf:DataFrame = null
    if(deltaInfo.memberObj.getString("code").contains("NASCO")){
    dropColyesdf = yesDatadf.drop("Indicator").drop("batch_date").drop("batch_id").drop("md5hashkey")
    .withColumn("med_cpl_codes", concat_ws(",",sort_array(split(col("med_cpl_codes"),","))))
    .withColumn("den_cpl_codes", concat_ws(",",sort_array(split(col("den_cpl_codes"),","))))
    .withColumn("vsn_cpl_codes", concat_ws(",",sort_array(split(col("vsn_cpl_codes"),","))))
    .withColumn("phar_cpl_codes", concat_ws(",",sort_array(split(col("phar_cpl_codes"),","))))
    .withColumn("drg_cpl_codes", concat_ws(",",sort_array(split(col("drg_cpl_codes"),","))))
    .withColumn("eap_cpl_codes", concat_ws(",",sort_array(split(col("eap_cpl_codes"),","))))
    
    
     dropColtodaydf = todayDatadf.drop("Indicator").drop("batch_date").drop("batch_id").drop("md5hashkey")
    .withColumn("med_cpl_codes", concat_ws(",",sort_array(split(col("med_cpl_codes"),","))))
    .withColumn("den_cpl_codes", concat_ws(",",sort_array(split(col("den_cpl_codes"),","))))
    .withColumn("vsn_cpl_codes", concat_ws(",",sort_array(split(col("vsn_cpl_codes"),","))))
    .withColumn("phar_cpl_codes", concat_ws(",",sort_array(split(col("phar_cpl_codes"),","))))
    .withColumn("drg_cpl_codes", concat_ws(",",sort_array(split(col("drg_cpl_codes"),","))))
    .withColumn("eap_cpl_codes", concat_ws(",",sort_array(split(col("eap_cpl_codes"),","))))
    
    }else{
      dropColyesdf = yesDatadf.drop("Indicator").drop("batch_date").drop("batch_id").drop("md5hashkey")
      dropColtodaydf = todayDatadf.drop("Indicator").drop("batch_date").drop("batch_id").drop("md5hashkey")
    }
    println("dropped column - indicator n batch date delta   ")// + hiveDeltaDatadf.show(2))
    
    dropColyesdf.createOrReplaceTempView("yesdatatable")
    dropColtodaydf.createOrReplaceTempView("toddatatable")
    
    println("COLS "+dropColyesdf.columns.mkString(",") + " length "+dropColyesdf.columns.length)
    
    println("COLS "+dropColtodaydf.columns.mkString(",") + " length "+dropColtodaydf.columns.length)
    
    val columns = dropColtodaydf.columns.mkString(",")
    
    val cols = dropColtodaydf.columns.map(col =>{ "if(tab1.f1 IS NULL, tab2.f1, tab1.f1) AS f1".replace("f1", col)} ).mkString(",")
    
    var colJoin = dropColtodaydf.columns.map(col =>{ "tab1.<COL> = tab2.<COL>".replaceAll("<COL>",col) } ).mkString(" and ")
    
//    getHiveData("select * from yesdatatable", deltaInfo.sqlContext).show(2)
//    getHiveData("select * from toddatatable", deltaInfo.sqlContext).show(2)
    
    println("columns  "+ dropColtodaydf.columns.mkString(","))
    val columnArray = dropColtodaydf.columns.filter(! _.contains("last_updt_dtm"))
    
    val colSplit = columnArray.grouped(500).toArray
    var colArray =  Array[String]()
    var colChk = Array[String]()
    var i =0
    println(" colSplit  "+ colSplit.length)
    /*for(colA <- colSplit){
//      colArray:+= "md5(concat("+colA.mkString(",")+")) as md5"+i
       colArray:+= "md5(concat("+nullChkColumns(colA)+")) as md5"+i
      colChk:+= "tab1.md5"+i+"!=tab2.md5"+i
      i=i+1
      println("  i "+ i)
    }*/
    
//    val columSplit:Array[Array[Column]] = dropColtodaydf.columns.grouped(500).toArray
    
//    println("colArray  "+ colArray(0))
    println("colChk "+ colChk.mkString(","))
//     val md5b = "md5(concat("+columns..take(500)+")) as md5a"
    
    println("Columns before dropColyesdf  " +dropColyesdf.columns.length+"    "  +dropColyesdf.columns.mkString(","))
    println("Columns before dropColtodaydf  " + dropColtodaydf.columns.length +"   "+ dropColtodaydf.columns.mkString(","))
    println("Columns with nullCHk " + concat_ws(",", (nullChkColumn(colSplit(0)): _* )))
    //hiveDatadf.withColumn("ss", hash_udf(concat_ws("", columns: _*)))
    /*
    var newYesDF = dropColyesdf.withColumn("md5hash_0", hash_udf(concat_ws(",", (nullChkColumn(colSplit(0)): _* ))))
    newYesDF = newYesDF.withColumn("md5hash_1", hash_udf(concat_ws(",", (nullChkColumn(colSplit(1)): _* ))))
    newYesDF = newYesDF.withColumn("md5hash_2", hash_udf(concat_ws(",", (nullChkColumn(colSplit(2)): _* ))))
    
    var newToDF = dropColtodaydf.withColumn("md5hash_0", hash_udf(concat_ws(",", (nullChkColumn(colSplit(0)): _* ))))
    newToDF = newToDF.withColumn("md5hash_1", hash_udf(concat_ws(",", (nullChkColumn(colSplit(1)): _* ))))
    newToDF = newToDF.withColumn("md5hash_2", hash_udf(concat_ws(",", (nullChkColumn(colSplit(2)): _* ))))
    
    */
//    println("dropColyesdf  before "+ dropColyesdf.count())
//    println("dropColtodaydf	before	"+ dropColtodaydf.count())
    
    var newYesDF = dropColyesdf
    var newToDF = dropColtodaydf
    var filterFld =""
    var dropColArray =  Array[String]()
    for(colA <- colSplit){
      
      newYesDF = newYesDF.withColumn("md5hash_"+i, hash_udf(concat_ws(",", (nullChkColumn(colA): _* ))))
      newToDF = newToDF.withColumn("md5hash_"+i, hash_udf(concat_ws(",", (nullChkColumn(colA): _* ))))
      filterFld = filterFld +"tab1.md5hash_"+i +" != tab2.md5hash_"+i + " or "
      dropColArray:+= "md5hash_"+i
       i=i+1
      println("  i "+ i)
    }
    
    println("filterFld   "+ filterFld)
    var fltrFld = "" 
    if(filterFld.endsWith(" or ")){
      fltrFld = filterFld.patch(filterFld.lastIndexOf("or"), "", 2)
    }else{
      fltrFld = filterFld
    }
    println("fltrFld     "+ fltrFld)
    println("drop Columns " + dropColArray.mkString(","))
    
    println("Columns after dropColyesdf  " +newYesDF.columns.length+"    "  +newYesDF.columns.mkString(","))
    println("Columns after dropColtodaydf  " + newToDF.columns.length +"   "+ newToDF.columns.mkString(","))
    newYesDF.createOrReplaceTempView("yesterDatamd5")
    newToDF.createOrReplaceTempView("todayDatamd5")
    println("newYesDF   "+ newYesDF.count())
    println("newToDF		"+ newToDF.count())
    newYesDF.show(2)
    newToDF.show(2)
    val filterChk = "tab1.md5hash_0!=tab2.md5hash_0 or tab1.md5hash_1!=tab2.md5hash_1 or tab1.md5hash_2!=tab2.md5hash_2 " 
    val deltaData = getHiveData("select  distinct tab1.* from todayDatamd5 tab1, yesterDatamd5 tab2 where tab1.membercontract=tab2.membercontract and ("+filterChk+" )", deltaInfo.sqlContext)
     
     
//    val yesterDatamd5 = getHiveData("select md5(concat("+columns+")) as md5h,*  from yesdatatable", deltaInfo.sqlContext)
//    val yesterDatamd5 = getHiveData("select "+colArray.mkString(",") +",*  from yesdatatable", deltaInfo.sqlContext)
//    val todayDatamd5 = getHiveData("select  "+colArray.mkString(",") +",* from toddatatable", deltaInfo.sqlContext)
//    println("Cols "+ yesterDatamd5.columns.mkString(","))
//    yesterDatamd5.createOrReplaceTempView("yesterDatamd5")
//    todayDatamd5.createOrReplaceTempView("todayDatamd5")
//    println("yesterDatamd5  "+ yesterDatamd5.count())
//    println("todayDatamd5" + todayDatamd5.count())
//    println("Query  "+"select  tab1.* from todayDatamd5 tab1, yesterDatamd5 tab2 where tab1.membercontract=tab2.membercontract and ("+colChk.mkString(" or ")+")" ) 
//    val deltaData = getHiveData("select  tab1.* from todayDatamd5 tab1, yesterDatamd5 tab2 where tab1.membercontract=tab2.membercontract and ("+colChk.mkString(" or ")+" )", deltaInfo.sqlContext)
    
    
//    println(deltaData.count() + "  deltaData ")
    deltaData.show(2) 
    
    deltaData.createOrReplaceTempView("deltaData")

    val UpdatedDelta = getHiveData("Select 'U' as Indicator,current_date as batch_date,'' as batch_id, * from deltaData",deltaInfo.sqlContext)
//    val updatedDelta_withDrop = UpdatedDelta.drop("md5hash_0").drop("md5hash_1").drop("md5hash_2")
    val updatedDelta_withDrop = UpdatedDelta.drop(dropColArray:_*)
    //val finalUpdatedDelta = updatedDelta_withDrop.withColumn("Indicator", when($"enrlmnt_trmntn_dt" < todayDate, "T").otherwise($"Indicator"))
    val finalUpdatedDelta = updatedDelta_withDrop.withColumn("Indicator", when(updatedDelta_withDrop.col("enrlmnt_trmntn_dt").lt(todayDate) , "T").otherwise(updatedDelta_withDrop.col("Indicator")))
    finalUpdatedDelta.createOrReplaceTempView("finalUpdatedDelta")
    finalUpdatedDelta.show(2)
    println("finalUpdatedDelta  "+ finalUpdatedDelta.count())
    
    
    
//    val appended = deltaInfo.sqlContext.sql(""" select a.* from today a left join  yesterday b  on  a.membercontract = b.membercontract  where b.membercontract is null  """)
    
    val appendedDelta = getHiveData("Select 'A' as Indicator,current_date as batch_date,'' as batch_id, tab1.* from todayDatamd5 tab1 left join yesterDatamd5 tab2 on tab1.membercontract=tab2.membercontract where tab2.membercontract is null ",deltaInfo.sqlContext)
    println("Appened Count " + appendedDelta.count())
//    val appendedDelta_withDrop = appendedDelta.drop("md5hash_0").drop("md5hash_1").drop("md5hash_2")
    val appendedDelta_withDrop = appendedDelta.drop(dropColArray:_*)
    val finalappendedDelta = appendedDelta_withDrop.withColumn("Indicator", when(appendedDelta_withDrop.col("enrlmnt_trmntn_dt").lt(todayDate) , "T").otherwise(appendedDelta_withDrop.col("Indicator")))
    finalappendedDelta.createOrReplaceTempView("finalappendedDelta")
    finalappendedDelta.show(2)
    
    /** UnComment the below lines to write to MongoDB   */
    
    val writeToMongoUpdDF = getHiveData("Select membercontract as _id,* from finalUpdatedDelta ", deltaInfo.sqlContext)
    val writeToMongoApdDF = getHiveData("Select membercontract as _id,* from finalappendedDelta ", deltaInfo.sqlContext)
    writeToMongoDB(writeToMongoUpdDF, getWriteConfig(mongoDatabase, collectionName, deltaInfo.sparkConfig, URL), URL, collectionName, deltaInfo.appConfig, deltaInfo.envType)
    writeToMongoDB(writeToMongoApdDF, getWriteConfig(mongoDatabase, collectionName, deltaInfo.sparkConfig, URL), URL, collectionName, deltaInfo.appConfig, deltaInfo.envType)
		
    /** Ends here  */
    
    /*
    val dDelta = deltaData.drop("md5hash_0").drop("md5hash_1").drop("md5hash_2")
    dDelta.createOrReplaceTempView("dDelta")
    val filterYesterdayDat =  getHiveData(" select tab1.* from  yesterDatamd5 tab1 where membercontract in ( select membercontract from  dDelta order by membercontract ) order by tab1.membercontract ", deltaInfo.sqlContext)  
    val fnlYesDt = filterYesterdayDat.drop("md5hash_0").drop("md5hash_1").drop("md5hash_2")
    checkAndDeleteFolder(deltaInfo.sparkContext,"hdfs://nameservicets1/dv/hdfsdata/vs2/ehb/bds1/phi/no_gbd/r000/outbound/Extracts/DeltaChk/")
    fnlYesDt.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs://nameservicets1/dv/hdfsdata/vs2/ehb/bds1/phi/no_gbd/r000/outbound/Extracts/DeltaChk/Yesterday/")
    dDelta.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs://nameservicets1/dv/hdfsdata/vs2/ehb/bds1/phi/no_gbd/r000/outbound/Extracts/DeltaChk/DeltaData/")
        */   
		
    
//    val unionquery = "select *,'A' as Indicator,current_date as batch_date,'' as batch_id from ( select count(*) AS Records_Count,* from "+
//  "(select * from yesdatatable UNION ALL select * from toddatatable) tab1 GROUP BY <COL_STRING> ) tab2 where Records_Count < 2".replace("<COL_STRING>", columns)
      /*
    var unionquery = "select * from toddatatable tab1 left join yesdatatable tab2  on ("+colJoin+")   where tab2.Indicator is null "
    println("unionquery "+ unionquery)
    val deltadf = getHiveData(unionquery, deltaInfo.sqlContext)
    println("except " + deltadf.count()) 
    import deltaInfo.sqlContext.implicits._

    deltadf.drop("Records_Count").createOrReplaceTempView("deltatable")
    
    //deltadf.show(2)
  //  yesDatadf.show(2)
     val updatedcolumns= cols+",if(tab1.Indicator is null,tab2.Indicator,if(tab1.membercontract = tab2.membercontract,'U',tab1.Indicator)) as Indicator,if(tab1.batch_date is null,tab2.batch_date,tab1.batch_date) as batch_date,if(tab1.batch_id is null,tab2.batch_id,tab1.batch_id) as batch_id"
    
     val query1 = "select <colnames> from deltatable tab1 full outer join originalyesterdaydata tab2 on (tab1.membercontract = tab2.memberContract)".replace("<colnames>",updatedcolumns)

     val finaldf = getHiveData(query1, deltaInfo.sqlContext)
     println("finaldf  "+ finaldf.count())
     println("Columns "+ finaldf.columns.mkString(","))
     val terminatedDf = finaldf.withColumn("Indicator", when($"enrlmnt_trmntn_dt" < todayDate, "T").otherwise($"Indicator"))
    
    terminatedDf.createOrReplaceTempView("member_info_nasco_temp")
    println("terminatedDf "+ terminatedDf.count())
    val finalDeltaDF = getHiveData("select * from member_info_nasco_temp where Indicator in ('A','U','T') ", deltaInfo.sqlContext)
    println( "finalDeltaDF  "+ finalDeltaDF.count())
    finalDeltaDF.show(2)
    */
    /*getHiveData("drop table member_info_nasco_temp_delta_22May", deltaInfo.sqlContext)
    val finalquery = "create table member_info_nasco_temp_delta_22May as select * from member_info_nasco_temp"
    val fnlDF = getHiveData(finalquery, deltaInfo.sqlContext)
    println("COunt  "+ fnlDF.count())
    */
    println("Get appended df")
  }

  
  
      
   /* deltadf.createOrReplaceTempView("deltadf")
    val updatedquery = "select <colnames>,'U' as Indicator,current_date as batch_date,'' as batch_id from deltadf a join yesdatatable b on (a.memberContract=b.memberContract)".replace("<colnames>", cols)
    val appendedquery = "select <colnames>,'A' as Indicator,current_date as batch_date,'' as batch_id from deltadf a where a.memberContract not in (select memberContract from yesdatatable)".replace("<colnames>", cols)
    val updatedcols= cols+",a.Indicator,a.batch_date,a.batch_id"
    val nondeltaquery = "select <colnames> from originalyesterdaydata a where a.memberContract not in (select memberContract from deltadf)".replace("<colnames>", updatedcols)
    
    val updateddf= getHiveData(updatedquery, deltaInfo.sqlContext).drop("Records_Count")
    //getHiveData(updatedquery, deltaInfo.sqlContext).drop("Records_Count").createOrReplaceTempView("updateddf")
    
    val appendeddf = getHiveData(appendedquery, deltaInfo.sqlContext).drop("Records_Count")
   //  getHiveData(appendedquery, deltaInfo.sqlContext).drop("Records_Count").createOrReplaceTempView("appendeddf")
     
     //val sql = "select * from (select * from updateddf union all select * from appendeddf) tab"
     
    val nondeltadf =  getHiveData(nondeltaquery, deltaInfo.sqlContext)
    
    val finaldf = updateddf.union(appendeddf).union(nondeltadf)*/
  
      
   /* val updatedhcids = yesDatadf.where(yesDatadf.col(distinctField).isin(deltahcids: _*)).rdd.map(r => r(0)).collect().toList.asInstanceOf[List[String]]
    println("Get updated hcids - " +updatedhcids.length)
    val appendedhcids = deltahcids.diff(updatedhcids)
    println("Get appended hcids - " +appendedhcids.length)
    val nondeltaDF = yesDatadf.where(!yesDatadf.col(distinctField).isin(deltahcids: _*)) //.withColumn("batch_date", lit(todayDate))
    println("non delta df")
    val appendedDF = todayDatadf.where(todayDatadf.col(distinctField).isin(deltahcids: _*)).withColumn("Indicator", lit("I")).withColumn("batch_date", lit(todayDate)).withColumn("Indicator", when(todayDatadf.col(distinctField).isin(appendedhcids: _*), lit("A")).otherwise($"Indicator")).withColumn("Indicator", when(todayDatadf.col(distinctField).isin(updatedhcids: _*), lit("U")).otherwise($"Indicator")).withColumn("Indicator", when($"enrlmnt_trmntn_dt" < todayDate, "T").otherwise($"Indicator"))
    val mongoData = appendedDF.withColumn("_id", concat($"member_id", $"CNTRCT_CD"))
    mongoData.show(10)*/
  
     /* val updateddf = nondeltaDF.union(appendedDF)
    updateddf.createOrReplaceTempView("temptable")
    println("Union")*/

    //val finaldf = hiveDeltaDatadf.withColumn("Indicator",lit("I")).withColumn("Indicator", when($distinctField.isin(appendedhcids: _*), lit("A")).otherwise($"Indicator")).withColumn("Indicator", when($distinctField.isin(updatedhcids: _*), lit("U")).otherwise($"Indicator")).withColumn("Indicator", when($"enrlmnt_trmntn_dt"<todayDate, "T").otherwise($"Indicator"))
    //val finalquery = "insert overwrite table <mytable> select * from temptable".replace("<mytable>", deltaInfo.hiveTable)
    //getHiveData(finalquery, deltaInfo.sqlContext)

   // writeToMongoDB(mongoData, getWriteConfig(mongoDatabase, collectionName, deltaInfo.sparkConfig, URL), URL, collectionName, deltaInfo.appConfig, deltaInfo.envType)
  
  def getWriteConfig(dbase: String, collection: String, conf1: SparkConf, url: String) = {
    import scala.collection.JavaConversions._
    import scala.collection.mutable._
    val opt: java.util.Map[String, String] = HashMap("writeConcern.w" -> "majority", "writeConcern.journal" -> "false", "maxBatchSize" -> "1000")
    WriteConfig.create(dbase, collection, url, 1, WriteConcernConfig.create(conf1).withOptions(opt).writeConcern)
  }
  def writeToMongoDB(dataFrame: DataFrame, writeConfig: WriteConfig, url: String, collectionName: String, appConf: Config, envType: String) = {

    val mongoClient = new MongoClient(new MongoClientURI(url + "?authMechanism=SCRAM-SHA-1"))
    val database = mongoClient.getDatabase(appConf.getString("bds.mongodb.mongodb_" + envType.toLowerCase))

    MongoSpark.save(dataFrame, writeConfig)
    println("Data has written to Mongo DB")
  }

  def getHiveData(query: String, sqlContext: SparkSession) = {
    val dataFrame = sqlContext.sql(query)
    dataFrame
  }

  def nullChkColumns(arg: Array[String]):String ={
    
    var newArray =  Array[String]()
    for(ar <-arg){
      if(ar.endsWith("_dt") ||ar.endsWith("_dtm")){
        newArray:+= "if("+ar+" is null, cast(0 as timestamp), "+ar+")"
      }else{
        newArray:+= "if("+ar+" is null, '', "+ar+")"
      }
    }
    newArray.mkString(",")
  }
  
  
  
  def nullChkColumn(arg: Array[String]):Array[Column] ={
  
    import org.apache.spark.sql.functions._
    var newArray =  Array[Column]()
    for(ar <-arg){
//      if(ar.endsWith("_dt") ||ar.endsWith("_dtm")){
      if(ar.contains("cpl_codes")){
//        newArray:+= col("if("+ar+" is null, cast(0 as timestamp), "+ar+")")
        //concat_ws(',',sort_array(split(med_cpl_codes,',')))  as med_cpl_codes1
//        newArray:+= col("concat_ws(',',sort_array(split("+ ar+",',')))").as(ar)
        newArray:+= col(ar)
      }else{
//        newArray:+= col("if("+ar+" is null, '', "+ar+")")
        newArray:+= col(ar)
      }
    }
    newArray
  }  
  
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
  
}