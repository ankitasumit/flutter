package anthem.ehub.bds.extracts
import java.io.File
import java.util.Calendar
import java.util.Date
import java.util.Locale
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions._

import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.bson.Document
import org.bson.conversions.Bson

import com.mongodb.spark._
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._
import com.mongodb.spark.config.ReadConfig
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import anthem.ehub.bds.utils.MongoBDSConfig
import anthem.ehub.bds.utils.Utils

object OtherExtracts {

  case class SummaryReport(customerType: String, sourceType: String, grpByBasedOn: String, contractCode: String, fileName: String, extractCount: Long, srcFileCount: Long, emailCount: Long,
                           dateStr: String, createdDate: Date, modifiedDate: Date)

  case class SchemaData(cmnKey: String, Hcid_id: String, Indicator: String, First_Name: String, Middle_Name: String, Last_Name: String, SSN: String, Date_of_Birth: java.sql.Date, Gender: String, Relationship_Code: String, Subscriber_SSN: String, Address_Line_1: String, Address_Line_2: String, City: String, State: String, ZIP_Code: String, Phone_Number: String, Email_Address: String, Member_ID_for_Subscriber: String, Employee_ID: String, Employment_Effective_Date: java.sql.Date, Employment_Termination_Date: java.sql.Date, Employment_Status_Code: String, Retirement_Date: java.sql.Date, Insurance_Carrier: String, Medical_Plan_Code: String, Health_Plan_Coverage_Effective_Date: java.sql.Date, Health_Plan_Coverage_Termination_Date: java.sql.Date, Medical_Coverage_Tier_Code: String, Coverage_Tier_Effective_Date: java.sql.Date, Pharmacy_Plan_Code: String, Division: String, Location_Code: String, Job_Title: String, Pay_Level: String, Business_Function: String, Other_Reporting_Code: String, Confidentiality_Flag: String, AIM_Flag: String, Member_Sequence_Number: String, Subscriber_ID: String, Brand_ID: String, Group_ID: String, Med_Source_System_ID: String, Medical_Contract_Code: String, Medical_SOurce_Network_ID: String, Individual_Unique_ID: String, Subscriber_Unique_ID: String, Subgroup_ID: String, Claims_Cross_Reference_Key: String, HSA_FLAG: String, Dental_Standalone_Indicator: String, Dental_HCID: String, Dental_Carrier_Member_ID: String, Dental_Insurance_Carrier_Name: String, Dental_Plan_Code: String, Dental_Contract_Code: String, Dental_Coverage_Tier_Code: String, Dental_Plan_Coverage_Effective_Date: java.sql.Date, Dental_Plan_Coverage_Termination_Date: java.sql.Date, Dental_Source_System_ID: String, Vision_Standalone_Flag: String, Vision_Carrier_Member_ID: String, Vision_HCID: String, Vision_Insurance_Carrier_Name: String, Vision_Plan_Code: String, Vision_Contract_Code: String, Vision_Coverage_Tier_Code: String, Vision_Plan_Coverage_Effective_Date: java.sql.Date, Vision_Plan_Coverage_Termination_Date: java.sql.Date, Vision_Source_System_ID: String, Underwriting_State_Code: String, MBU: String, Exchange_Type_Code: String, Medical_Product_Id: String, RX_Product_id: String, Dental_product_id: String, Vision_Product_id: String, Product_Type: String, Company_code: String, CPL_Code: String, HRA_HSA_Account_Type: String, HRA_HSA_Vendor: String, FSA_Account_Type: String, FSA_Vendor: String, National_Indicator: String)
  
  case class opsSchema(SSN: String, HCID: String, Last_Name: String, First_Name: String, Date_Of_Birth: java.sql.Date, Sequence_Number: String, Member_Code: String, New_Member_Code: String, Relationship_Code: String, Client_Employer_Group_ID: String, Client_Employer_Group_Name: String, Email_Address: String, EffectiveDate: java.sql.Date, LastUpdatedDate: java.sql.Date, TerminationDate: java.sql.Date, SourceSystemCD: String, EmailType: String, PreferredFlag: String, EmailStatus: String, MBR_ID: String)

  def main(args: Array[String]): Unit = {

    println("Processing of Main ")
    val stTime = System.currentTimeMillis()
    /*    val sparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.ui.port", "3012").set("spark.rdd.compress", "true").set("spark.scheduler.mode", "FAIR")
      .set("spark.dynamicAllocation.enabled", "true").set("spark.shuffle.service.enabled", "true").set("spark.debug.maxToStringFields", "50")*/

    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.ui.port", "10101")
      .set("spark.rdd.compress", "true")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.broadcast.compress", "true")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.shuffle.service.enabled", "true")
      .set("spark.debug.maxToStringFields", "50")
      .setMaster("yarn")
      .set("spark.driver.memory", "3G")
      .set("spark.io.compression.codec", "lz4")
      .setExecutorEnv("num-executors", "50")
      .setExecutorEnv("executor-cores", "2")
      .setExecutorEnv("executor-memory", "5G")
      .set("spark.yarn.queue", "ehub-xds_yarn")

    val log = LogManager.getRootLogger
    log.setLevel(Level.ERROR)

    val sparkContext = new SparkContext(sparkConf)
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val sqlContext = SparkSession.builder().appName("Other Extract").config("spark.sql.warehouse.dir", warehouseLocation).getOrCreate()
    val appConf = ConfigFactory.load()

    sparkConf.registerKryoClasses(Array(Class.forName("anthem.ehub.bds.extracts.OtherExtracts"))) //Comt for local
    sparkContext.setLocalProperty("spark.scheduler.pool", "ehub-xds_yarn")

    // args[0]: EnvType= SIT DEV UAT  args[1]: CustomerType= "ops411"  "mesa~paladina~ops411" args[2]:sourceType= "WGS~ISG" args[3]:Load= "full "Daily"  
    println("AfterRegister")
    if ("".equalsIgnoreCase(args(0)) || "".equalsIgnoreCase(args(1)) || "".equalsIgnoreCase(args(2)) || "".equalsIgnoreCase(args(3))) {
      println("Enough data not provided to proceed for file Generation");
      System.exit(1);
    } else {
      loadReportGen(appConf, sparkContext, sparkConf, sqlContext, args)
    }
    println("End of processing Time taken Total  " + TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - stTime) + " in Minutes")
  }

  def loadReportGen(appConf: Config, sparkContext: SparkContext, sparkConf: SparkConf, sqlContext: SparkSession, args: Array[String]) = {
    var deltaStDt = "";
    if (!args(3).equalsIgnoreCase("full")) {
      deltaStDt = calculateDeltaStartDate(args, appConf);
    }

    if (args(2) != null && !args(2).contains("#")) {
      loadReportGenConfigData(appConf, sparkContext, sparkConf, sqlContext, args, deltaStDt)
    } else {
      genCmbndExtracts(appConf, sparkContext, sparkConf, sqlContext, args, deltaStDt, false)
    }

  }

  def loadReportGenConfigData(appConf: Config, sparkContext: SparkContext, sparkConf: SparkConf, sqlContext: SparkSession, args: Array[String], deltaStDt: String) = {
    println("Processing of loadReportGenConfigData " + args.mkString(" "))

    //    var cusType = concatString(args(1));
    //    var srcType = concatString(args(2)); //"WGS","ISG","NASCO"

    var header: String = null
    var extrcatLocation: String = null
    val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd")
    val dateFolder = dateFormat.format(new Date())

    val confrecords = MongoBDSConfig.getConfingData(args(0), args(1).toLowerCase().split("~"), args(2).toUpperCase().split("~"), appConf)
    println("Size  " + confrecords.length)
    var fnsuffix: String = null
    for (doc <- confrecords) {
      if (doc.getString("Customertype").startsWith("castlight")) {
        processCastlightExtracts(appConf, sparkContext, sparkConf, sqlContext, doc, args, deltaStDt, true)
      } else if (doc.containsKey("conditionTypes")) {
        header = processConditionalExtracts(appConf, sparkContext, sparkConf, sqlContext, doc, args, deltaStDt)
        extrcatLocation = doc.getString("extractLocation")
        fnsuffix = doc.getString("fileNameSuffix")
      } else if (doc.getString("Customertype").equalsIgnoreCase("ehp")) {
        processEHPExtracts(appConf, sparkContext, sparkConf, sqlContext, doc, args, deltaStDt)
      }else if(doc.getString("Customertype").equalsIgnoreCase("healthguide")){
//        getHealthGuideData(appConf, sparkContext, sparkConf, sqlContext, doc, args, deltaStDt)
      }else {
        processExtracts(appConf, sparkContext, sparkConf, sqlContext, doc, args, deltaStDt, true)
      }
    }

    var mergeLocation: String = extrcatLocation + "/" + dateFolder;
    if (header != null) {
      if ("full".equalsIgnoreCase(args(3))) {
        val sourceLocation = mergeLocation + "/full/merged"
        Utils.copyFromHDFSToLocalFile(sparkContext, sourceLocation, mergeLocation + "/" + dateFolder.concat("_FULL_").concat(fnsuffix).concat(".csv"), header, "")
      } else {
        val sourceLocation = mergeLocation + "/delta/merged"
        Utils.copyFromHDFSToLocalFile(sparkContext, sourceLocation, mergeLocation + "/" + dateFolder.concat("_DELTA_").concat(fnsuffix).concat(".csv"), header, "")
      }
    }
    println("End of loadReportGenConfigData")
  }

  def genCmbndExtracts(appConf: Config, sparkContext: SparkContext, sparkConf: SparkConf, sqlContext: SparkSession, args: Array[String], deltaStDt: String, generateFile: Boolean) = {

    val consRecords = MongoBDSConfig.getConfingData(args(0), args(1).toLowerCase().split("~"), args(2).toUpperCase().split("#"), appConf)
    var listDF: List[DataFrame] = List[DataFrame]()
    var arrDF = Array[DataFrame]()
    var document: Document = null
    for (doc <- consRecords) {
//      val df = processCastlightExtracts(appConf, sparkContext, sparkConf, sqlContext, doc, args, deltaStDt, generateFile)
      var df:DataFrame = null
      if (doc.getString("Customertype").startsWith("castlight")) {
         df = processCastlightExtracts(appConf, sparkContext, sparkConf, sqlContext, doc, args, deltaStDt, generateFile)
      } else {
         df = processExtracts(appConf, sparkContext, sparkConf, sqlContext, doc, args, deltaStDt, generateFile)
      }

      println("df length      ---->     ")
      if (df != null && df.take(1).length > 0) {
        //println("df  != null  "+ df.take(1).length +"       Count "  + df.count())
        listDF = listDF :+ df
      }
      document = doc
    }
    println(listDF.length + "    length  ")
    val tt = System.currentTimeMillis()
    var consDF: DataFrame = null
    if (listDF.length > 1) {
      consDF = listDF(0)
      for (i <- 1 until listDF.length) {
        consDF = consDF.union(listDF(i))
      }
    } else if (listDF.length == 1) {
      consDF = listDF(0)
    }
    println("Total timeTaken to union " + TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - tt) + " in Minutes")
    println("consDF      " + consDF.count())
    processFileGeneration(consDF, document, args, sparkContext, sqlContext, appConf)
    println("File Genration Completed")
  }
/*
  def getHealthGuideData(appConf: Config, sparkContext: SparkContext, sparkConf: SparkConf, sqlContext: SparkSession, doc: Document, args: Array[String], deltaStDt: String) = {
    val csType = doc.getString("Customertype").toUpperCase()
    val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd")
    val dateFolder = dateFormat.format(new Date())

    println("customer type " + csType)
    val filepath = doc.getString("fileLocation") 
    val fileformat = doc.getString("fileformat")
    var df:DataFrame = null 
    if(fileformat.equalsIgnoreCase("json")){
       df = sqlContext.read.json(filepath)
    }else if (fileformat.equalsIgnoreCase("parquet")){
      df = sqlContext.read.parquet(filepath)
      df.show(10)
    }else{
      df = sqlContext.read.text(filepath)
    }

     val db = appConf.getString("bds.mongodb.mongodb_" + args(0).toLowerCase()) 
     val uri = appConf.getString("bds.mongodb.mongodb_uri_" + args(0).toLowerCase())
     val collection_name = doc.getString("collectionName")
  
     val writeConfig =  WriteConfig.create(db,collection_name, uri, 500000, WriteConcernConfig.create(sparkConf).writeConcern)
     
     MongoSpark.save(df, writeConfig)
  }
  */
  def processEHPExtracts(appConf: Config, sparkContext: SparkContext, sparkConf: SparkConf, sqlContext: SparkSession, doc: Document, args: Array[String], deltaStDt: String) = {
    val csType = doc.getString("Customertype").toUpperCase()
    val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd")
    val dateFolder = dateFormat.format(new Date())

    println("customer type " + csType)
    val HIVE_DATABASE = doc.getString("hive_DB")
    sqlContext.sql(s"use $HIVE_DATABASE")
    println(s"Hive Database $HIVE_DATABASE")
    val path = doc.get("extractLocation") + "/" + dateFolder

    println("Path " + path)
    val query = doc.getString("query")
    val ouputrecords = sqlContext.sql(query)
    val ouputrecords_col = ouputrecords.columns.toList
    import sqlContext.implicits._
    val EHPExtractdf = ouputrecords.map(r => { Utils.mkJson(r, ouputrecords_col) })

    Utils.checkAndDeleteFolder(sparkContext, path)
    EHPExtractdf.rdd.saveAsTextFile(path)
    val dateFormatTime = new java.text.SimpleDateFormat("yyyyMMddhhmmss")
    val dateTime = dateFormatTime.format(new Date())
    val fileName = doc.getString("fileNameSuffix").replaceAll("<datetime>", dateTime)
    val finalLocation = path + "/merged"
    Utils.copyFromHDFSToLocalFile(sparkContext, path, finalLocation + "/" + fileName, "", "")
    println("Extract Generated at path " + finalLocation)
  }
  def calculateDeltaStartDate(args: Array[String], appConf: Config) = {
    val deltaObj = MongoBDSConfig.getDeltaDetails(args(0), "FrequencyType", args(3).toUpperCase(), appConf)
    val deltaStdt = getDeltaStartDate(deltaObj.getString("Units"), deltaObj.getString("Frequency"), "Yes")
    deltaStdt
  }

  def processConditionalExtracts(appConf: Config, sparkContext: SparkContext, sparkConf: SparkConf, sqlContext: SparkSession, doc: Document, args: Array[String], deltaStDt: String) = {
    println("Processing of processConditionalExtracts   " + doc.get("conditionTypes") + "    " + doc.get("sourceType") + "      " + doc.getString("Customertype"))
    val condiTypes: List[String] = doc.get("conditionTypes").toString().split(",").map(_.trim).toList

    val uri = appConf.getString("bds.mongodb.mongodb_uri_" + args(0).toLowerCase()) + "." + doc.get("collectionName")
    //val readConfig = ReadConfig(Map("uri" -> uri)) //.withOption("readPreference.name", "secondaryPreferred")//ReadConfig(Map("uri" -> uri, "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sparkContext))) //ReadConfig(Map("uri" -> uri))
    val readConfig = ReadConfig(Map("uri" -> uri)).withOption("readPreference.name", "secondaryPreferred").withOption("spark.mongodb.input.partitionerOptions.samplesPerPartition", "100000")
    var matchBlk = ""
    if (args(3).equalsIgnoreCase("full")) {
      matchBlk = doc.getString("matchBlkAll")
    } else {
      matchBlk = doc.getString("matchBlkDelta").replace("<ST_DATE>", deltaStDt).replace("<ED_DATE>", getDeltaStartDate("Now", "Now", "No"))
    }

    val projection = doc.getString("projectBlk")
    //ET_DATE~now~now,EEF_DATE~-6~Months,BR_DATE~-18~Years
    matchBlk = replaceDateinMatchBlk(matchBlk, doc.getString("queryDateField"))
    val genblk = replaceDateinMatchBlk(doc.getString("genMatchBlockAll"), doc.getString("queryDateField"))
    println("matchBlk======>			" + matchBlk + "   , " + projection)
    val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd")
    val dateFolder = dateFormat.format(new Date())
    val genData = MongoSpark.load(sparkContext, readConfig).withPipeline(Seq(Document.parse(genblk)))
    
    println("After generic match block")
    val outputList = condiTypes.map(condiType => genData.withPipeline(
      List(
        Document.parse(matchBlk.replace("<CON_TYPE>", condiType)),
        Document.parse(projection.replace("<CON_TYPE>", condiType))))).toList
    println("Crossed Here 1");
    var i: Int = 0;
    var consoldf: DataFrame = null;
    var extractType = "DELTA"
    if (args(3) == "full") {
      extractType = "FULL"
    }

    var headerFlag = true;
    var header: String = null
    println("Crossed Here 2   " + outputList.size);
/*    val executor: ExecutorService = Executors.newFixedThreadPool(outputList.length)
    var futureList = new scala.collection.mutable.ListBuffer[Future[String]]()
    outputList.map(out => {
      futureList += executor.submit(new Callable[String]() {
        def call() = {
          println("Printed")
          val dataFrame = fixColumns(out.toDF(), doc, sqlContext, condiTypes(i), "columnHeaders")
          if (headerFlag) {
            header = dataFrame.columns.mkString(doc.getString("delimiter"))
            headerFlag = false
          }
          writeDF(dataFrame, condiTypes(i), extractType, sparkContext, dateFolder, doc)
          i = i + 1;
          condiTypes(i)
        }
      })
    })

    for (fut <- futureList) {
      println("Came inside " + fut.get)
    }*/
    
    outputList.map(out => {
      println("Came inside loop ")
      if (out != null) { //&& out.toDF().take(1).length>0
        val dataFrame = fixColumns(out.toDF(), doc, sqlContext, condiTypes(i), "columnHeaders")
        if (headerFlag) {
          header = dataFrame.columns.mkString(doc.getString("delimiter"))
          headerFlag = false
        }
        writeDF(dataFrame, condiTypes(i), extractType, sparkContext, dateFolder, doc)
      }
      i = i + 1;
    })
    println("End of Processing of " + doc.getString("sourceType") + "  For Customer Type " + doc.getString("Customertype").toUpperCase())
    header
  }

  //  def writeDF(dataFrame: DataFrame, sourceCode: String, emailTypes: String, loadType: String, sourcePath: String, sparkContext: SparkContext, dateFolder: String,delimiter: String) = {
  def writeDF(dataFrame: DataFrame, emailTypes: String, loadType: String, sparkContext: SparkContext, dateFolder: String, doc: Document) = {
    println("Processing of  WriteDF ");
    val sourceCode = doc.getString("sourceType")
    val sourcePath = doc.getString("extractLocation")
    val delimiter = doc.getString("delimiter")
    val fnsuffix = doc.getString("fileNameSuffix")
    if ("DELTA".equalsIgnoreCase(loadType)) {
      val path = sourcePath + "/" + dateFolder + "/delta/" + sourceCode + "/" + emailTypes
      val mergeLocation = sourcePath + "/" + dateFolder + "/delta/merged"
      Utils.writeDataToCSV(dataFrame, sparkContext, path, mergeLocation, dateFolder.concat(emailTypes).concat(sourceCode).concat("_DELTA_").concat(fnsuffix).concat(".csv"), false, null, null, delimiter, "")
      //.concat("_DELTA_EmailList_eHub.csv")
    } else {
      val path = sourcePath + "/" + dateFolder + "/full/" + sourceCode + "/" + emailTypes
      val mergeLocation = sourcePath + "/" + dateFolder + "/full/merged"
      Utils.writeDataToCSV(dataFrame, sparkContext, path, mergeLocation, dateFolder.concat(emailTypes).concat(sourceCode).concat("_FULL_").concat(fnsuffix).concat(".csv"), false, null, null, delimiter, "")
    }
    println("End of Processing of  WriteDF ");
  }

  def processExtracts(appConf: Config, sparkContext: SparkContext, sparkConf: SparkConf, sqlContext: SparkSession, doc: Document, args: Array[String], deltaStDt: String, generateFile: Boolean) = {
    println("Processing of  processExtracts " + args.mkString(" "));
    val uri = appConf.getString("bds.mongodb.mongodb_uri_" + args(0).toLowerCase()) + "." + doc.get("collectionName")
    val readConfig = ReadConfig(Map("uri" -> uri)) //.withOption("readPreference.name", "secondaryPreferred") //ReadConfig(Map("uri" -> uri, "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sparkContext)))
    var matchBlk: String = "";
    if (args(3).equalsIgnoreCase("full")) {
      matchBlk = doc.getString("matchBlkAll")
    } else {
      matchBlk = doc.getString("matchBlkDelta").replace("<ST_DATE>", deltaStDt).replace("<ED_DATE>", getDeltaStartDate("Now", "Now", "No"))
    }
    var docList = Seq[Bson]()
    //ET_DATE~now~now,EEF_DATE~-6~Months,BR_DATE~-18~Years
    matchBlk = replaceDateinMatchBlk(matchBlk, doc.getString("queryDateField"))

    val projection: String = doc.getString("projectBlk")
    println("matchBlk spark load  =====>  " + matchBlk + "    " + projection);
    if (doc.get("queryExecutor") != null) {
      for (blk <- doc.get("queryExecutor").asInstanceOf[java.util.ArrayList[String]]) {

        if (blk == "matchBlkAll") { docList = docList :+ Document.parse(matchBlk) }
        else if (blk == "projectBlk") { docList = docList :+ Document.parse(projection) }
        else { docList = docList :+ Document.parse(doc.getString(blk)) }
      }
    } else {
      docList = docList :+ Document.parse(matchBlk)
      docList = docList :+ Document.parse(projection)
    }

    val ouputrecords = MongoSpark.load(sparkContext, readConfig).withPipeline(docList)
    println("Passed Mongo Spark load")

    val csType = doc.getString("Customertype").toUpperCase()
    val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd")
    val dateFolder = dateFormat.format(new Date())
    var extractType = "Delta"
    if (args(3) == "full") {
      extractType = "full"
    }
    //    println("Checking for output not null  "+ ouputrecords.toDF().take(1).length )
    /*
    if (ouputrecords != null) { // && ouputrecords.toDF().take(1).length>0
      val finalDataFrame = fixColumns(ouputrecords.toDF(), doc, sqlContext, csType, "columnHeaders")
      val path = doc.get("extractLocation") + "/" + dateFolder + "/" + doc.getString("sourceType")
      val mergeLocation = doc.get("extractLocation") + "/" + dateFolder + "/" + "merged"
      val headerPath = doc.get("extractLocation") + "/" + dateFolder + "/" + "Header"
      val fileName = "Anthem_" + csType + "_" + dateFolder
      println("File Generation Path  " + path)

      Utils.writeDataToCSV(finalDataFrame, sparkContext, path, mergeLocation, fileName + ".csv", false, null, null, doc.getString("delimiter"), "")
      val header = finalDataFrame.columns.mkString(doc.getString("delimiter"))
      val finalLocation = mergeLocation + "/merged"
      Utils.copyFromHDFSToLocalFile(sparkContext, mergeLocation, finalLocation + "/" + fileName + ".csv", header, "")
    } */
    if (generateFile && ouputrecords != null & ouputrecords.take(1).length > 0) {
      processFileGeneration(ouputrecords.toDF(), doc, args, sparkContext, sqlContext, appConf)
    } else {
      println("No Data to generate")
    }
    println("End of Processing of " + doc.get("sourceType") + "  For Customer  " + csType)
    ouputrecords.toDF()
  }

  def processCastlightExtracts(appConf: Config, sparkContext: SparkContext, sparkConf: SparkConf, sqlContext: SparkSession, doc: Document, args: Array[String], deltaStDt: String, genFile: Boolean) = {
    println("Processing of  processCastlightExtracts " + args.mkString(" "));
    val uri = appConf.getString("bds.mongodb.mongodb_uri_" + args(0).toLowerCase()) + "." + doc.get("collectionName")
    val readConfig = ReadConfig(Map("uri" -> uri)).withOption("readPreference.name", "secondary").withOption("spark.mongodb.input.partitionerOptions.samplesPerPartition", "100000")
    //.withOption("partitioner", "MongoSplitVectorPartitioner")//.withOption("spark.mongodb.input.partitionerOptions.samplesPerPartition", "1000") //ReadConfig(Map("uri" -> uri, "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sparkContext)))
    var matchBlk: String = "";
    var qryDataFrame: DataFrame = null
    if (args(3).equalsIgnoreCase("full")) {
      matchBlk = doc.getString("matchBlkAll")
    } else {
      matchBlk = doc.getString("matchBlkDelta").replace("<ST_DATE>", deltaStDt).replace("<ED_DATE>", getDeltaStartDate("Now", "Now", "No"))
    }
    var docList = Seq[Bson]()
    if (doc.get("queryExecutor") != null) {
      for (blk <- doc.get("queryExecutor").asInstanceOf[java.util.ArrayList[String]]) {
        if (blk == "matchBlkAll") {
          docList = docList :+ Document.parse(matchBlk.replaceAll("<INDIC_ID>", concatString(args(4))).replaceAll("<GRP_ID>", concatString(args(5))))
        } else {
          //            	println(doc.getString(blk).replaceAll("<GRP_ID>", concatString(args(5))))
          docList = docList :+ Document.parse(doc.getString(blk).replaceAll("<GRP_ID>", concatString(args(5))))
        }
      }
    } else {
      docList = docList :+ Document.parse(matchBlk.replaceAll("<INDIC_ID>", concatString(args(4))).replaceAll("<GRP_ID>", concatString(args(5))))
      docList = docList :+ Document.parse(doc.getString("projectBlk"))
                println(matchBlk.replaceAll("<INDIC_ID>", concatString(args(4)) ).replaceAll("<GRP_ID>", concatString(args(5))) )
                println(doc.getString("projectBlk"))
    }
    println("Main Query Printed--------------------------------------")
    //      	val database = MongoBDSConnection.MongoDB(args(0), appConf)
    //      	val collName = database.getCollection(doc.getString("collectionName"))
    //      	println("Collection Count "+ doc.getString("collectionName")  +    "        "+ collName.count())
    println("Match block -- "+doc.getString("genMatchBlockAll"))
    val genData = MongoSpark.load(sparkContext, readConfig).withPipeline(Seq(Document.parse(doc.getString("genMatchBlockAll"))))
    println("Passed Here genMatchBlockAll")
    //genData.persist()
    val outputMain = genData.withPipeline(docList)
    println("Passed Here outputmain")
    //        println("outputMain   "+ outputMain.count()) 
    //        println("Collection Count "+ doc.getString("collectionName")  +    "        "+ collName.count())
    var hcidArray: List[String] = List[String]()
    var outputMainDF: Dataset[SchemaData] = null;
    //      	val outputMainDS =      outputMain.toDS[SchemaData]
    println("Before if blk")
    if (outputMain != null && outputMain.take(1).length > 0) {
      //   outputMainDF =      outputMain.toDF()
      println("came if inside ")
      outputMainDF = outputMain.toDS[SchemaData]()
      outputMainDF.createOrReplaceTempView("outputMainDF")
      hcidArray = outputMainDF.select(outputMainDF.col(doc.getString("distinctFldName"))).distinct().rdd.map(r => r(0)).collect().toList.asInstanceOf[List[String]]
      println("came inside if block")
    } 

    println("   hcidArray " + hcidArray.length + "     ")

    println("Before Rem Executor ")
    var remdocList = Seq[Bson]()
    if (doc.get("remQueryExecutor") != null) {
      println("came inside  remQueryExecutor ")
      for (blk <- doc.get("remQueryExecutor").asInstanceOf[java.util.ArrayList[String]]) {
        if (blk == "remMatchBlockAll") {
          //            	println(doc.getString("remMatchBlockAll").replaceAll("<INDIC_ID>", concatString(args(4))).replaceAll("<GRP_ID>", concatString(args(5))).replace("<ID_KEY>", concatString(hcidArray)) )
          remdocList = remdocList :+ Document.parse(doc.getString("remMatchBlockAll").replaceAll("<INDIC_ID>", concatString(args(4))).replaceAll("<GRP_ID>", concatString(args(5))).replace("<ID_KEY>", concatString(hcidArray)))
        } else {
          //            	println(doc.getString(blk).replaceAll("<GRP_ID>", concatString(args(5)) ))  
          remdocList = remdocList :+ Document.parse(doc.getString(blk).replaceAll("<GRP_ID>", concatString(args(5))))
        }
      }
    } else {
      println("Else Query remdocList")
      remdocList = remdocList :+ Document.parse(doc.getString("remMatchBlockAll").replaceAll("<INDIC_ID>", concatString(args(4))).replaceAll("<GRP_ID>", concatString(args(5))).replace("<ID_KEY>", concatString(hcidArray)))
      remdocList = remdocList :+ Document.parse(doc.getString("projectBlk"))
      println("Else Query")
                println(doc.getString("remMatchBlockAll").replaceAll("<INDIC_ID>", concatString(args(4))).replaceAll("<GRP_ID>", concatString(args(5)) ).replace("<ID_KEY>", concatString(hcidArray)) )
                println(doc.getString("projectBlk"))
    }
    println("Rem Dep Query Finished  ---------------------------------------------------------")
    val remOutput = genData.withPipeline(remdocList)

    println("Crossed here  rem output ")
    val dfjoinQuery = doc.getString("sqlQueryDF1")
    var dfupdatedList: List[DataFrame] = List[DataFrame]()
    var updatedMainDF: DataFrame = null
    //        println("remOutput   "+ remOutput.count())
    if (remOutput != null && remOutput.take(1).length > 0 && outputMain != null && outputMainDF.take(1).length > 0) {
      //          val reminDF = remOutput.toDF()
      val reminDF = remOutput.toDS[SchemaData]()
      reminDF.createOrReplaceTempView("remaining_cntrcts")
      //          println("show reminDF "+reminDF.count() )
      //          reminDF.show()
      //          println(" outputMainDF  "+ outputMainDF.count())
      //          println(" reminDF  "+ reminDF.count())
      val dfjoinQ = dfjoinQuery.replaceAll("<TAB1>", "outputMainDF").replaceAll("<TAB2>", "remaining_cntrcts")
      println("dfjoinQ  " + dfjoinQ)
      updatedMainDF = sqlContext.sql(dfjoinQ)
      //          println("updatedMainDF  "+ updatedMainDF.columns.mkString(","))
      //          println("updatedMainDF  "+ updatedMainDF.count())  
    } else {
      println(" came inside else");
      if (outputMainDF != null && outputMainDF.take(1).length > 0) {
        updatedMainDF = outputMainDF.toDF()
      } else {
        println("No Data to generate")
      }
      //          println("updatedMainDF  "+ updatedMainDF.count())
    }

    val addressUpdateQuery = doc.getString("sqlQueryDF2")
    val consolQuery = doc.getString("sqlQueryDF")
    val consolDFQuery = consolQuery.replaceAll("<COL_NAME>", concatStringMaxQuery("max(tab1.", doc.getString("TColumnHeaders"), ") as "))
    if (updatedMainDF != null && updatedMainDF.take(1).length > 0) {
      //        println("updatedMainDF  "+ updatedMainDF.count())
      //        updatedMainDF.select(updatedMainDF.col("Member_ID_for_Subscriber")).show(10)
      //        updatedMainDF.show()
      updatedMainDF.createOrReplaceTempView("AddressUpdate")
      /* Working  code */
      val scrbrAddQry = doc.getString("sqlQueryDF3") //"Select tab1.* from <TAB1> tab1 where tab1.Indicator='SUB' "
      val scrbrQry = scrbrAddQry.replaceAll("<TAB1>", "AddressUpdate").replaceAll("<COL_NAME>", concatString("tab5.", doc.getString("TColumnHeaders"), " as "))
      val scrbrDF = sqlContext.sql(scrbrQry) //updatedMainDF.filter(updatedMainDF.col("Indicator").like("SUB"))
      val depDF = sqlContext.sql("Select tab1.* from AddressUpdate tab1 where tab1.Indicator='DEP' ")
      //        println("scrbrDF   "+ scrbrDF.columns.mkString(",")) 
      //  	    println("  scrbrDF  "+ scrbrDF.count())
      //  	    println("depDF   "+ depDF.columns.mkString(",")) 
      //  	    println("  depDF  "+ depDF.count())
      //  	    println("scrbr ")
      //  	    scrbrDF.show()
      //  	    println(" Dep DF" )
      //  	    depDF.show()
      scrbrDF.createOrReplaceTempView("scrbrData")
      depDF.createOrReplaceTempView("depData")

      /*
  	    val addQuery = addressUpdateQuery.replaceAll("<TAB1>", "AddressUpdate")
        println("addQuery  "+ addQuery)
        val addressUpdateDF = sqlContext.sql(addQuery)
        println("ch  -------------------------------------------"+ addressUpdateDF!=null)
//        val consDF = scrbrDF.join(depDF, scrbrDF.col("Hcid_id")===depDF.col("Hcid_id"))
//        println(" consDF------>  "+ consDF.count())
//        consDF.show(2)
        println(" addressUpdateDF  -- show")
        addressUpdateDF.show(2)
        println("addressUpdateDF  "+ addressUpdateDF.count())
        val scrbrAddQry =  doc.getString("sqlQueryDF3")
  	    val scrbrQry = scrbrAddQry.replaceAll("<TAB1>", "AddressUpdate").replaceAll("<COL_NAME>", concatString("tab5.", doc.getString("TColumnHeaders")," as "))
  	    val scrbrDF =  sqlContext.sql(scrbrQry) 
        val consolDF = scrbrDF.union(addressUpdateDF) 
  	    */

      //val df = scrbrDF.alias("tab2").join(depDF.alias("tab1"),"Hcid_id")
      //df.createTempView("addQuery")
      // println("df  alias  "+ df.columns.mkString(",")) 
      // df.show(2)
      /*  Working code */
      val qry = doc.getString("sqlQueryDF4")
      val addressUpdateDF_back = sqlContext.sql(qry)
      //  	    println("addressUpdateDF_back  "+ addressUpdateDF_back.count())
      val consolDF = scrbrDF.union(addressUpdateDF_back)
      //  	    println("consolDF  "+ consolDF.count())
      println("consolDF ")
      // consolDF.show(2)

      consolDF.createOrReplaceTempView("ConsolidatedDF")
      //        updatedMainDF.createOrReplaceTempView("ConsolidatedDF")
      val consQuery = consolDFQuery.replaceAll("<TAB1>", "ConsolidatedDF")
      println(consolDFQuery)
      //      println("consolDF  "+ consolDF.columns.mkString(","))
      qryDataFrame = sqlContext.sql(consQuery)
      //       println("qryDataFrame  "+ qryDataFrame.columns.mkString(","))
      //        println("qryDataFrame  "+ qryDataFrame.count())
      //        qryDataFrame.show()
      if (genFile) {
        processFileGeneration(qryDataFrame, doc, args, sparkContext, sqlContext, appConf)
      }
      qryDataFrame
    } else {
      println("No Data to Generate")
      null
    }

  }

  def processFileGeneration(qryDataFrame: DataFrame, doc: Document, args: Array[String], sparkContext: SparkContext, sqlContext: SparkSession, appConf: Config) {
    println("Came inside processcastlightFileGeneration")
    val csType = doc.getString("Customertype").toUpperCase()
    val dateFormatTime = new java.text.SimpleDateFormat("yyyyMMddhhmmss")
    val dateTime = dateFormatTime.format(new Date())
    val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd")
    val dateFolder = dateFormat.format(new Date())
    var extractType = "Delta"
    if (args(3) == "full") {
      extractType = "full"
    }
    val fName = doc.getString("fileNameSuffix").replaceAll("<DT>", dateFolder).replaceAll("<DTMIN>", dateTime)
    val path = doc.get("extractLocation") + "/" + dateFolder + "/Generate"
    val mergeLocation = doc.get("extractLocation") + "/" + dateFolder + "/merged"
    val finalLoc = doc.get("extractLocation") + "/" + dateFolder + "/final"
    println("crossed here ")
    val generateFileBasedOn = doc.getString("generateFileBasedOn")
    val header:String = "H|"+dateFolder+"|F"
    val numFormat = java.text.NumberFormat.getIntegerInstance(java.util.Locale.US)
    numFormat.setMinimumIntegerDigits(20)
    numFormat.setGroupingUsed(false)
    if (qryDataFrame != null && qryDataFrame.take(1).length > 0) {
      println("came inside qryDF")

      Utils.checkAndCreateFolder(sparkContext, finalLoc)
      val qryDF = fixColumns(qryDataFrame, doc, sqlContext, "qryDataFrame", "TColumnHeaders")
      if (generateFileBasedOn != null) {
        val grpByFld = qryDataFrame.select(qryDataFrame.col(generateFileBasedOn)).distinct().rdd.map(r => r(0)).collect().toList.asInstanceOf[List[String]]
        println("   grpByFld   " + grpByFld.length + "    " + grpByFld.mkString("  "))
        var cnt = 5
        if (grpByFld.filter(_ != null).length < 5) {
          cnt = grpByFld.filter(_ != null).length
        }
        val executor: ExecutorService = Executors.newFixedThreadPool(cnt) 
        var futureList = new scala.collection.mutable.ListBuffer[Future[String]]()
        for (grpBy <- grpByFld) {
          val finalDataFrame = qryDF.filter(qryDF.col(generateFileBasedOn).equalTo(grpBy))
         if (finalDataFrame != null && grpBy != null) {
            val fileName = fName.replaceAll("<GENID>", grpBy) 
            val finalDF = finalDataFrame.drop("cmnKey").drop("Hcid_id")
           
            futureList += executor.submit(new Callable[String]() {
              def call() = {
                 /*  val cntrctCodeList = finalDF.select("Medical_Contract_Code").distinct().rdd.map(r => r(0)).collect().toList.asInstanceOf[List[String]]
                   val emailCount = finalDF.filter(finalDF("Email_Address") !== "").count()
                   val srObj = new SummaryReport(csType,doc.getString("sourceType"), grpBy, cntrctCodeList.mkString(","),fileName, finalDF.count(),0, emailCount,  dateFolder.toString(), new Date(), null )
                   val res = processSummaryReport(srObj, args, sparkContext, appConf)  
                   println("Completed Summary report process") */
                println("Printed")
                val trailer:String = "T|"+numFormat.format(finalDF.count())+"|"+numFormat.format(finalDF.filter(finalDF("Indicator") === "SUB").count())+"|"+numFormat.format(finalDF.filter(finalDF("Indicator") === "DEP").count())
                
                val fileN = generateFile(finalDF, path + "/" + grpBy, mergeLocation, fileName, finalLoc, doc.getString("delimiter"), sparkContext,header,trailer)
                fileN
              }
            })
          }
        }
        var i = 0
        for (fut <- futureList) {
          println("Came inside status")
          println(fut.get)
        }

        executor.shutdown()
        executor.awaitTermination(100, TimeUnit.SECONDS)
        println("Finished all threads")
      } else {
        val fileName = "Anthem_" + csType + "_" + dateFolder
        val header = qryDF.columns.mkString(doc.getString("delimiter"))
        val trailer = "T|"+numFormat.format(qryDF.count())
        generateFile(qryDF, path, mergeLocation, fileName + ".csv", finalLoc, doc.getString("delimiter"), sparkContext,header,trailer)
      }
    }
  }

  def generateFile(finalDataFrame: DataFrame, path: String, mergeLocation: String, fileName: String, finalLoc: String, delimiter: String, sparkContext: SparkContext, header:String, trailer:String) = {
    Utils.writeDataToCSV(finalDataFrame, sparkContext, path, mergeLocation, fileName, false, null, header, delimiter, trailer)
    val headers = finalDataFrame.columns.mkString(delimiter)
    val finalLocation = mergeLocation + "/merged"
    Utils.copyFromHDFSToLocalFile(sparkContext, mergeLocation + "/" + fileName, finalLoc + "/" + fileName, headers, "")
    //    scala.sys.process.Process(s"hadoop fs -cp $mergeLocation/$fileName.csv $finalLoc/  ")
    fileName
  }

  //case class SummaryReport(customerType:String, sourceType:String, grpByBasedOn: String, contractCode: String, fileName: String, extractCount: String, srcFileCount:String,
  //    dateStr: String, createdDate: Date, modifiedDate: Date)

  def processSummaryReport(sreport: SummaryReport, args: Array[String], sparkContext: SparkContext, appConf: Config) {
    val doc = new Document("grpByBasedOn", sreport.grpByBasedOn).append("contractCode", sreport.contractCode).append("fileName", sreport.fileName)
      .append("extractCount", sreport.extractCount).append("dateStr", sreport.dateStr).append("createdDate", sreport.createdDate)
      .append("customerType", sreport.customerType).append("sourceType", sreport.sourceType).append("emailCount", sreport.emailCount)
    val res = MongoBDSConfig.InsertSummaryReportData(args(0), doc, appConf)
    res
  }

  def fixColumns(dataFrame: DataFrame, doc: Document, sqlContext: SparkSession, conType: String, columnHeadersName: String) = {
    val listColumns = dataFrame.columns.toList
    val totalColumns = doc.getString(columnHeadersName).split(",").toList //appConf.getStringList("bds.colsDisplay")//colHeader.split(",").toList
    var data: DataFrame = null

    if (dataFrame.take(1).length > 0) {
      if (doc.getString("sqlDF") != null) {

        data = dataFrame.na.fill("", listColumns)
        //        var sqlDFList = doc.get("sqlDF")
        val sqlDFList = doc.get("sqlDF").asInstanceOf[java.util.ArrayList[Document]]
        for (document <- sqlDFList) {
          //        sqlDFList.foreach{ document =>  
          println("Came here")
          data = filterDF(data, document, doc, sqlContext)
        }
      } else {
        val diffCols = CollectionUtils.subtract(totalColumns, listColumns)
        var sqlOutput: DataFrame = null
        if (diffCols.size() > 0) {
          dataFrame.createOrReplaceTempView(conType)
          var colNames = " "
          for (col <- diffCols) {
            colNames += "'' as " + col.toString() + ","
          }

          val query = "Select *,<COLS> from " + conType
          val qquery = query.replace("<COLS>", StringUtils.chop(colNames))
          println("Query	" + qquery)
          val stt = System.currentTimeMillis()
          sqlOutput = sqlContext.sql(s"$qquery")
          println("Time Taken for Query Execu " + TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - stt))
        }
        if (sqlOutput == null) {
          sqlOutput = dataFrame
        }
        data = sqlOutput.na.fill("", listColumns)
        println(" distinct " + doc.getString("doDistinctDF"))

      }

      println("before withColumn");
      val dateFmt = doc.getString("dateFormatinCSV")

      import org.apache.spark.sql.functions._
      if (doc.getString("dateFormatFields") != null) {
        val dateField1 = doc.getString("dateFormatFields").split(",")
        for (dtField <- dateField1) {
          val field = dtField.split("~")(0)
          println("field:" + field)
          val dtFormat = dtField.split("~")(1)
          data = data.withColumn(field, when(col(field).isNull, "").otherwise(date_format(col(field), dtFormat)))
          println("data:" + data)
        }
      }
      if (doc.getString("dateField") != null) {
        val dateField = doc.getString("dateField").split(",")
        for (dtField <- dateField) {
          data = data.withColumn(dtField, when(col(dtField).isNull, "").otherwise(date_format(col(dtField), dateFmt)))
        }
      }
      var finalDataFrame = data.select(totalColumns.head, totalColumns.tail: _*)
      if (doc.getString("doDistinctDF") != null) {
        println("Doing distinct ")
        finalDataFrame = finalDataFrame.distinct()
      }
      finalDataFrame
    } else {
      data
    }

  }

  def filterDF(data: DataFrame, document: Document, actDoc: Document, sqlContext: SparkSession) = {
    println("Enterred filterDF  ")
    var outDF: DataFrame = null
    var sqlDF: DataFrame = null
    var columns: Array[String] = null

    val templateName = document.getString("templateName")
    data.createOrReplaceTempView(templateName)
    if (document.getString("colHeader") != null) {
      val colHeaderName = document.getString("colHeader")
      println("colHeaderName " + colHeaderName)
      columns = actDoc.getString(colHeaderName).split(",")
      outDF = data.select(columns.head, columns.tail: _*)
    } else {
      outDF = data
    }
    if (document.getString("filterKey") != null && document.getString("filterValue") != null) {
      outDF = outDF.filter(data.col(document.getString("filterKey")).like(document.getString("filterValue")))
    }
    if (document.getString("sqlQuery") != null) {
      val sqlQueryDF = actDoc.getString(document.getString("sqlQuery")).replace("<SQL_TABLENAME>", templateName)
      val sqlDF = sqlContext.sql(sqlQueryDF)

      if (document.getString("doUnionDistinct") != null) {
        outDF = outDF.union(sqlDF).distinct()
      }
      outDF = outDF.select(columns.head, columns.tail: _*)

    }

    println("Exiting filterDF ")
    outDF
  }
  def replaceDateinMatchBlk(matchBlk: String, dtTypeStr: String) = {
    var mthBlk = matchBlk
    if (dtTypeStr != null) {
      for (dtfld <- dtTypeStr.split(",")) {
        val dtf = dtfld.split("~")
        val dt = getDate(dtf(2), dtf(1))
        mthBlk = mthBlk.replace("<" + dtf(0) + ">", dt)
        println("<" + dtf(0) + ">" + "     " + dt + "     " + mthBlk)
      }
    }
    mthBlk
  }

  /**
   *
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

  def concatString(arg: String): String = {
    var outTy: String = null;
    val newArray: Array[String] = new Array[String](arg.split("~").length);
    var i: Int = 0;
    if (arg.indexOf("~") > 0) {
      val ty = arg.split("~")
      for (t <- ty) {
        newArray(i) = "\"" + t + "\""
        i = i + 1;
      }
      outTy = newArray.mkString(",")
    } else {
      outTy = "\"" + arg + "\""
    }
    return outTy
  }

  def concatString(arg: List[String]): String = {
    //    var outTy: String = null;
    var newArray: List[String] = List[String]();
    var i: Int = 0;
    for (t <- arg) {
      newArray = newArray :+ "\"" + t + "\""
      i = i + 1;
    }
    val outTy = newArray.mkString(",")
    //    println(" values   "+ outTy)
    return outTy
  }

  def concatString(prefix: String, arg: String, suffix: String): String = {
    var outTy: String = null;
    val newArray: Array[String] = new Array[String](arg.split(",").length);
    var i: Int = 0;
    if (arg.indexOf(",") > 0) {
      val ty = arg.split(",")
      for (t <- ty) {
        newArray(i) = prefix + t + suffix + t
        i = i + 1;
      }
    }
    outTy = newArray.mkString(",")

    return outTy
  }

  def concatStringMaxQuery(prefix: String, arg: String, suffix: String): String = {
    var outTy: String = null;
    val newArray: Array[String] = new Array[String](arg.split(",").length);
    var i: Int = 0;
    if (arg.indexOf(",") > 0) {
      val ty = arg.split(",")
      for (t <- ty) {
        if (t.equalsIgnoreCase("CPL_Code")) {
          newArray(i) = "concat_ws(',', collect_set(" + t + ")) as " + t
        } else {
          newArray(i) = prefix + t + suffix + t
        }
        i = i + 1;
      }
    }
    outTy = newArray.mkString(",")

    return outTy
  }
  //ET_DATE~now~day,EEF_DATE~-6~MONTH,BR_DATE~-18~YEAR
  def getDate(units: String, freq: String): String = {
    val fmt = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", Locale.ENGLISH)
    val cals = Calendar.getInstance()
    if (units == "Hours") {
      cals.add(Calendar.HOUR, Integer.parseInt(freq))
    } else if (units == "Days") {
      cals.add(Calendar.DAY_OF_MONTH, Integer.parseInt(freq))
    } else if (units == "Months") {
      cals.add(Calendar.MONTH, Integer.parseInt(freq))
    } else if (units == "Years") {
      cals.add(Calendar.YEAR, Integer.parseInt(freq))
    }
    val date = fmt.format(cals.getTime())
    date
  }

  def getDeltaStartDate(units: String, freq: String, delta: String): String = {
    val fmt = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", Locale.ENGLISH)
    val cals = Calendar.getInstance()
    if (units == "Hours") {
      cals.add(Calendar.HOUR, Integer.parseInt(freq))
    } else if (units == "Days") {
      cals.add(Calendar.DAY_OF_MONTH, Integer.parseInt(freq))
    } else if (units == "Months") {
      cals.add(Calendar.MONTH, Integer.parseInt(freq))
    } else if (units == "Years") {
      cals.add(Calendar.YEAR, Integer.parseInt(freq))
    }
    if (delta == "Yes") {
      cals.set(Calendar.HOUR, 0)
      cals.set(Calendar.MINUTE, 0)
      cals.set(Calendar.SECOND, 0)
      cals.set(Calendar.MILLISECOND, 0)
    }
    val batchDt = fmt.format(cals.getTime())
    batchDt
  }
}