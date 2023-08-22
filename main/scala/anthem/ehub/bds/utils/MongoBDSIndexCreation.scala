package anthem.ehub.bds.utils

import java.util.concurrent.TimeUnit

import org.bson.Document

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object MongoBDSIndexCreation {
  
  def main(args: Array[String]): Unit = {
    
      println("Processing of Main MongoBDSIndexCreation")
      val stTime = System.currentTimeMillis()
      val appConf = ConfigFactory.load()
      
      if ("".equalsIgnoreCase(args(0)) || "".equalsIgnoreCase(args(1)) ){
        println("Enough data not provided to proceed for file Generation");
        System.exit(1);
      }else{
        generateIndexes(appConf, args)
      }
       
     println("End of processing Time taken Total  " + TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis()-stTime) +" in Minutes")
  }
  
   def generateIndexes(appConf: Config, args: Array[String]) = {
       var confrecords  = Array[Document]()
       if(args(1)=="All"){
         confrecords = MongoBDSConfig.getIndexDetails(args(0), "Mongo_Indexes", null, appConf)
       }else{
         confrecords = MongoBDSConfig.getIndexDetails(args(0), "Mongo_Indexes", args(1).split("~"), appConf)
       }
//       println("confrecords  " + confrecords.length)
        for(doc <- confrecords){
          val docu = validateIndexExists(args(0), doc.getString("collectionName"), doc.getString("indexName"), appConf)
          if(docu.getString("exists")!="Y"){
            println("Index Creation Started for "+ doc.getString("collectionName") + "    "+ doc.getString("indexName"))
            MongoBDSConfig.createIndex(args(0), doc, appConf)
            println("Index Creation Completed for "+ doc.getString("collectionName") + "    "+ doc.getString("indexName"))
          }
        }
   }
   
   
   def validateIndexExists(envType:String, collectionName:String, indexName:String, appConf:Config) = {
     val indexes = MongoBDSConfig.getIndexes(envType, collectionName, appConf)
     val ixit = indexes.iterator()
     var doc = new Document()
//     println("name  "+ indexName )
     import scala.util.control.Breaks._
     breakable{ 
     while(ixit.hasNext()){
         doc = ixit.next()
//         println( doc.getString("name")    +   "    "+indexName)
         if(doc.getString("name")==indexName){
           doc.append("exists", "Y")
           break
         }
      }}
//     println("----->" +doc.toJson())
     doc
   }
   
}