package anthem.ehub.bds.utils

import org.bson.Document

import com.mongodb.BasicDBObject
import com.mongodb.client.model.IndexOptions
import com.typesafe.config.Config
import com.mongodb.client.model.UpdateOptions
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.util.Date
import com.mongodb.client.result.UpdateResult

object MongoBDSConfig {
  

  def getConfigDetail(envType:String, filterValue:String, appConf: Config) = {
    
    val cursor = getFilterData(envType,"filterType",filterValue,appConf,"bds.mongodb.config_collection")
    val ir = cursor.iterator();
    var document:Document = null;
    while(ir.hasNext()){
     document= ir.next(); 
    }
    document
  }
  
  def getConfigDetails(envType: String, filterValue: String, appConf: Config) = {
    val cursor = getFilterData(envType,"filterType",filterValue,appConf,"bds.mongodb.config_collection")
    val ir = cursor.iterator();
    var docArray = Array[Document]()
    while(ir.hasNext()){
     docArray:+=ir.next()
    }
    docArray
  }

   def getConfDetailKey(envType: String, filterKey: String,filterValue:String, appConf: Config) = {
     val cursor = getFilterData(envType,filterKey,filterValue,appConf,"bds.mongodb.config_collection")
      val ir = cursor.iterator();
      var document:Document = null;
      while(ir.hasNext()){
       document= ir.next(); 
      }
      document
   }
   
    def getKeyBasedConfDetails(envType: String, filterKey: String,filterValue:String, appConf: Config) = {
     val cursor = getFilterData(envType,filterKey,filterValue,appConf,"bds.mongodb.config_collection")
      val ir = cursor.iterator();
      var docArray = Array[Document]()
      while(ir.hasNext()){
       docArray:+=ir.next(); 
      }
      docArray
   }
   def getConfingData(envType: String, custType:Array[String],srcType:Array[String], appConf: Config) = {
      val database = MongoBDSConnection.MongoDB(envType, appConf)
      val collection = database.getCollection(appConf.getString("bds.mongodb.config_collection"))
      val searchQuery = new BasicDBObject();
      searchQuery.put("Customertype", new BasicDBObject("$in",custType ))
      searchQuery.put("sourceType", new BasicDBObject("$in",srcType ));
      val cur = collection.find(searchQuery);
      val irr = cur.iterator();
      var docuArray = Array[Document]();
      while(irr.hasNext()){
        var doc = irr.next()
        doc.append("matchBlkAll", doc.getString("matchBlockBefore")+doc.getString("matchBlockAll")+doc.getString("matchBlockAfter"))
        doc.append("matchBlkDelta", doc.getString("matchBlockBefore")+doc.getString("matchBlock")+doc.getString("matchBlockAfter"))
        doc.append("projectBlk", doc.getString("projectBlockBefore")+doc.getString("projectBlock")+doc.getString("projectBlockAfter"))
       docuArray+:=doc
      }
      docuArray
   }
   def getDeltaDetails(envType: String, filterKey: String,filterValue:String, appConf: Config) = {
     val cursor = getFilterData(envType,filterKey,filterValue,appConf,"bds.mongodb.delta_config_collection")
      val ir = cursor.iterator();
      var document:Document = null;
      while(ir.hasNext()){
       document= ir.next(); 
      }
      document
   }

  def getFilterData(envType:String, filterKey:String, filterValue:String, appConf: Config, collectionName:String)={
    val database = MongoBDSConnection.MongoDB(envType, appConf)
    val collection = database.getCollection(appConf.getString(collectionName))
    val searchQuery = new BasicDBObject();
    searchQuery.put(filterKey, filterValue)
    val cursor = collection.find(searchQuery);
    cursor
  }
  
   def getIndexDetails(envType: String, filterValue: String, sourceType:Array[String], appConf: Config) = {
    val database = MongoBDSConnection.MongoDB(envType, appConf)
    val collection = database.getCollection(appConf.getString("bds.mongodb.config_collection"))
    val searchQuery = new BasicDBObject();
    searchQuery.put("filterType", filterValue)
    if(sourceType!=null){
      searchQuery.append("sourceType", new BasicDBObject("$in", sourceType))
    }
    println("sq   "+ searchQuery.toJson())
    val cursor = collection.find(searchQuery);
    val ir = cursor.iterator();
    var docArray = Array[Document]()
    while(ir.hasNext()){
     docArray:+=ir.next()
    }
    docArray
  }
   
   def getIndexes(envType: String,collectionName: String,appConf: Config ) = {
    val database = MongoBDSConnection.MongoDB(envType, appConf)
    val collection = database.getCollection(collectionName)  
    val indexes = collection.listIndexes()
    indexes
  }
   
   def createIndex(envType:String, doc:Document, appConf: Config) {
      val database = MongoBDSConnection.MongoDB(envType, appConf)
      val collection = database.getCollection(doc.getString("collectionName"))
      val indexopts = new IndexOptions()
      indexopts.background(true)
      indexopts.name(doc.getString("indexName"))
      if(doc.getBoolean("sparse")!=null){
        indexopts.sparse(true) 
      }
      collection.createIndex(Document.parse(doc.getString("indexFields")), indexopts)
    } 
   
   def InsertSummaryReportData(envType:String, doc:Document, appConf: Config){
     val database = MongoBDSConnection.MongoDB(envType, appConf)
     val collection = database.getCollection(appConf.getString("bds.mongodb.summaryreport_collection"))
     val query = new Document("grpByBasedOn",doc.getString("grpByBasedOn")).append("customerType", doc.getString("customerType")).append("dateStr", doc.getString("dateStr"))
     val updateQuery = new Document("$set",doc)
     val updateOptions = new UpdateOptions()
     updateOptions.upsert(true)
     collection.updateOne(query, updateQuery, updateOptions)
   }
   
   def UpdateSummaryReportData(envType:String, doc:Document, appConf: Config){
     val database = MongoBDSConnection.MongoDB(envType, appConf)
     val collection = database.getCollection(appConf.getString("bds.mongodb.summaryreport_collection"))
     val query = new Document("grpByBasedOn",doc.getString("grpByBasedOn")).append("customerType", doc.getString("customerType")).append("dateStr", doc.getString("dateStr"))
     val updateQuery = new Document("$set",doc)
     val updateOptions = new UpdateOptions()
     updateOptions.upsert(true)
     collection.updateOne(query, updateQuery, updateOptions)
   }
   
   def UpdateSummaryReportData(envType:String, docList:Array[Document], appConf: Config){
     val database = MongoBDSConnection.MongoDB(envType, appConf)
     val collection = database.getCollection(appConf.getString("bds.mongodb.summaryreport_collection"))
     var upRes:UpdateResult = null
     for(doc <- docList){
       val query = new Document("grpByBasedOn",doc.getString("grpByBasedOn")).append("customerType", doc.getString("customerType"))
       val updateQuery = new Document("$set",doc)
       val updateOptions = new UpdateOptions()
       updateOptions.upsert(true)
       upRes = collection.updateMany(query, updateQuery, updateOptions)
     }
   }
   def getCntrctCodeSummaryReportData(envType:String, custType:String,datestr:String, appConf: Config) = {
     val database = MongoBDSConnection.MongoDB(envType, appConf)
     val collection = database.getCollection(appConf.getString("bds.mongodb.summaryreport_collection"))
     val searchQuery = new BasicDBObject();
     searchQuery.put("customerType", custType.toUpperCase())
     searchQuery.put("dateStr", datestr)
     val cursor = collection.find(searchQuery)
     val ir = cursor.iterator();
     var docArray = Array[String]()
     var outMap = scala.collection.mutable.Map[String,String]()
      while(ir.hasNext()){
        val map = ir.next()
        outMap += (map.getString("grpByBasedOn") -> map.getString("contractCode"))
      }
      outMap
  }
   
   def getSourceFileData(filePath:String,appConf: Config,sparkContext: SparkContext,sqlContext:SQLContext,envType:String, custType:String,datestr:String) = {
    val confRecords = MongoBDSConfig.getKeyBasedConfDetails(envType, "Customertype", "SummaryReport", appConf) 
    val dt = new Date()
    val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd")
    val currDt = dateFormat.format(new Date())
    val isgSourceFilePath:String = appConf.getString("bds.mongodb.isgSourceFilePath")
    val wgsSourceFilePath:String = appConf.getString("bds.mongodb.wgsSourceFilePath")
    val isgSourceDF = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "|").load(isgSourceFilePath)
    
    
    val wgsSourceDF = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "\t").load(wgsSourceFilePath)
    wgsSourceDF.createOrReplaceTempView("wgsSourceDF")
    val wgsDF = sqlContext.sql("""select substring(a.C0,0,14) as subscriber_id,substring(a.C0,16,03) as mbr_seq_nbr,substring(a.C0,19,04) as cntrct_cde,substring(a.C0,23,09) as scr_cde,substring(a.C0,33,06) as case_id,substring(a.C0,43,08) as enrlmet_efctv_dt,substring(a.C0,51,08) as end_dt,substring(a.C0,59,09) as hcid,substring(a.C0,79,03) as new_mbr_id from wgsSourceDF a""")
    wgsDF.createOrReplaceTempView("wgsSourceTemp")
    val ffdf = sqlContext.sql("""select distinct C2,C4,C6 from sourceDFTemp where C8>CURRENT_DATE""")
    ffdf.createOrReplaceTempView("filteredData")
    val ddf  = sqlContext.sql("""select C2 as cntrct_code, count(*) as count from filteredData group by C2""")
    
    
    val srcType = confRecords(0).getString("sourceTypes").split(",")
    
    isgSourceDF.createOrReplaceTempView("sourceDFTemp")
    val fdf = sqlContext.sql("""select distinct C2,C4,C6 from sourceDFTemp""")
    fdf.createOrReplaceTempView("filteredData")
    val df  = sqlContext.sql("""select C2 as cntrct_code, count(*) as count from filteredData group by C2""")
    
    val extractData = getCntrctCodeSummaryReportData(envType, custType,datestr,appConf)
    var sourceCountMap = scala.collection.mutable.Map[String,String]()
    
    extractData.foreach(x =>{
      val key = x._1
      val values = x._2.split(",")
      
      var count:Int = 0
      for(cntrct_cd <- values){
        count  = count + df.filter(df.col("cntrct_code")===cntrct_cd).select("count").collect().map(_(0).asInstanceOf[Int]).reduce(_+_)
      }
      
      sourceCountMap += (key -> count.toString())
    })
   
    sourceCountMap
  }
}