package anthem.ehub.bds.extracts

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import java.util.Date
import com.typesafe.config.Config
import anthem.ehub.bds.utils.MongoBDSConfig
import org.apache.spark.sql.DataFrame
import org.bson.Document
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.bson.conversions.Bson
import anthem.ehub.bds.utils.Utils
import java.util.concurrent.TimeUnit
import org.apache.spark.SparkConf
import com.typesafe.config.ConfigFactory
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object SummaryReport {

  def main(args: Array[String]): Unit = {
    println("Processing of Main ")
    val stTime = System.currentTimeMillis()
    val sparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.ui.port", "3012").set("spark.rdd.compress", "true").set("spark.scheduler.mode", "FAIR")
      .set("spark.dynamicAllocation.enabled", "true").set("spark.shuffle.service.enabled", "true")

    val log = LogManager.getRootLogger
    log.setLevel(Level.ERROR)

    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = SparkSession
      .builder()
      .appName("Spark SQL")
      .getOrCreate()
    val appConf = ConfigFactory.load()

    sparkConf.registerKryoClasses(Array(Class.forName("anthem.ehub.bds.extracts.SummaryReport")))
    if ("".equalsIgnoreCase(args(0))) {
      println("Enough data not provided to proceed for file Generation");
      System.exit(1);
    } else {
      val dfMap = getSourceFileData(appConf, sparkContext, sqlContext, args(0))
      processSummaryCount(appConf, sparkContext, sqlContext, dfMap, args(0))
    }
    println("End of processing Time taken Total  " + TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - stTime) + " in Minutes")

  }

  def getSourceFileData(appConf: Config, sparkContext: SparkContext, sqlContext: SparkSession, envType: String) = {
    val confRecords = MongoBDSConfig.getKeyBasedConfDetails(envType, "Customertype", "SummaryReport_SourceFile", appConf)
    val dt = new Date()
    val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd")
    val currDt = dateFormat.format(new Date())
    var dfMap = scala.collection.mutable.Map[String, DataFrame]()
    for (confDoc <- confRecords) {
      val sourceFilePath: String = confDoc.getString("filePath")
      var sourceDF = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", confDoc.getString("delimiter")).load(sourceFilePath)
      for(col<- sourceDF.columns){sourceDF = sourceDF.withColumnRenamed(col, col.replaceAll("[^a-zA-Z0-9]", ""))}
      for (i <- 1 until 4){
          if(confDoc.getString("templateName"+i)!=null){
	        sourceDF.createOrReplaceTempView(confDoc.getString("templateName" + i))
	        sourceDF = sqlContext.sql(confDoc.getString("sqlQuery" + i).replaceAll("<TableName>", confDoc.getString("templateName" + i)))
          }  
      }
      dfMap.put(confDoc.getString("sourceType"), sourceDF)
    }
    dfMap
  } 

  def processSummaryCount(appConf: Config, sparkContext: SparkContext, sqlContext: SparkSession, dfMap: scala.collection.mutable.Map[String, DataFrame], envType: String) {
	  println("Came inside processSummaryCount")
      val confRecords = MongoBDSConfig.getKeyBasedConfDetails(envType, "Reporttype", "SummaryReport", appConf)  //Get from Config Coll
      for(doc <- confRecords){
        val srcType = doc.getString("sourceType").split(",") //WGS,NASCO 
        var docArray = Array[Document]()
        val fetchMap = MongoBDSConfig.getCntrctCodeSummaryReportData(envType, doc.getString("matchBlock"), doc.getString("project"), appConf) //get Contract Code SR
        if(fetchMap!=null &&  fetchMap.size>0){
          println("fetchMap    "+ fetchMap.toString())
          for((key,value) <- fetchMap){
            var count:Long = 0;
              for(srcTy <- srcType){ // Get DF based on SRC Type
                val df = dfMap.get(srcTy).get
//                val arrayCount = df.filter(df.col("cntrct_code")===cntrctCd).select("count").collect().map(_(0).asInstanceOf[Long])
                import sqlContext.implicits._
                val arrayCount = df.where($"cntrct_code".isin(value.split(","):_*)).select("count").collect().map(_(0).asInstanceOf[Long])
                if(arrayCount.length>0){
                  count  = count + arrayCount.reduce(_+_)
                } 
                println("Count   "+ count)
              }
            val bsonDoc = new Document("grpByBasedOn",key).append("customerType", doc.getString("Customertype"))
            .append("srcFileCount", count).append("modifiedDate", new Date())
            docArray :+= bsonDoc
          }
          val res = MongoBDSConfig.UpdateSummaryReportData(envType, docArray, appConf)
          generateSummaryReport(appConf, sparkContext, sqlContext, doc, envType)
        }
      } ///1 castlight type gets complete
          println("Completed processSummaryCount") 
  }

  def generateSummaryReport(appConf: Config, sparkContext: SparkContext, sqlContext: SparkSession, document: Document, envType: String) {

    val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd")
    val currDt = dateFormat.format(new Date())
    val uri = appConf.getString("bds.mongodb.mongodb_uri_" + envType.toLowerCase()) + "." + document.get("collectionName")
    val readConfig = ReadConfig(Map("uri" -> uri)).withOption("readPreference.name", "secondaryPreferred") //ReadConfig(Map("uri" -> uri, "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sparkContext)))
    var docList = Seq[Bson]()
    docList = docList :+ Document.parse(document.getString("matchRptBlk").replaceAll("<DATE_STR>", currDt))
    docList = docList :+ Document.parse(document.getString("projectRptBlk"))
    val outputMain = MongoSpark.load(sparkContext, readConfig).withPipeline(docList)
    val path = document.get("extractLocation") + "/" + currDt
    val mergeLocation = document.get("extractLocation") + "/" + currDt + "/" + "merged"
    val fileName = document.getString("fileName").replaceAll("<DTSTR>", currDt)
    val finalDataFrame = outputMain.toDF()
    Utils.writeDataToCSV(finalDataFrame, sparkContext, path, mergeLocation, fileName, false, null, null, document.getString("delimiter"),"")
    val header = finalDataFrame.columns.mkString(document.getString("delimiter"))
    val finalLocation = document.get("extractLocation") + "/" + currDt
    Utils.copyFromHDFSToLocalFile(sparkContext, mergeLocation, finalLocation + "/" + fileName, header,"")
    println("Completion of  Report for CustomerType " + document.getString("Customertype") + "   in Path" + path)
  }
}