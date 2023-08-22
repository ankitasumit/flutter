package anthem.ehub.bds.config

import java.util.Date

import org.bson.Document

import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.log4j.Level

final case class BDSMongoDBConfigInfo(name: String, jobSequence: String, sparkJars: String, sparkJarsLocation: String, sparkHomeLocation: String,
                                      appResourceJar: String, threadSleep: Int, sparkExecutorExtraClassPath: String, deployMode: String, checkFrequency:Boolean,
                                      toTerminateAfterThreadAwake:Boolean
                                      )

final case class BDSMongoJobsInfo(name: String, isActive: Boolean, frequency: String, lastExecutionDate: Date, nextInvocationDate: Date,
                                  fullQualifiedSparkJobName: String, parameters: String, sparkMaster: String, sparkExecutorCores: String,
                                  sparkExecutorInstances: String, sparkExecutorMemory: String, sparkDriverMemory: String, sparkAppName: String, sparkDriverClassPath: String,
                               sparkSupportingJars: String, sparkDriverCores: String, appResourceJar: String, sparkDynamicAllocation: String)

//scala -classpath '*.jar' anthem.ehub.bds.config.BDSMainSpark dev
//scala -classpath '*.jar' anthem.ehub.bds.config.BDSMainSpark dev (or) scala -classpath '*.jar' anthem.ehub.bds.config.BDSMainSpark dev ALL
//To run this code we need these jars bds_framework-1.0-SNAPSHOT.jar,bds_header_utility-1.0-SNAPSHOT.jar,commons-csv-1.4.jar,joda-time-2.9.4.jar,log4j-1.2.17.jar,
//mongo-java-driver-3.2.2.jar,mongo-spark-connector_2.10-1.1.0.jar,ojdbc6.jar,spark-csv_2.10-1.2.0.jar,spark-launcher_2.10-1.6.0-cdh5.7.2.jar
/**
 * This code is used to fetch the details from Mongo and then calls the SparkJobLauncher for each individual job
 * @author AF55641
 *
 */
object BDSMainSpark {
	val log = Logger.getLogger("anthem.ehub.bds.config.BDSMainSpark")
  def main(args: Array[String]) {
    val appConf = ConfigFactory.load()
    val uri = appConf.getString("bds.mongodb.mongodb_uri_" + args(0).toLowerCase())
    log.debug(appConf.getString("bds.mongodb.mongodb_uri_print_" + args(0).toLowerCase()))
    val mongoClient = new MongoClient(new MongoClientURI(uri + "?authMechanism=SCRAM-SHA-1"))
    val database = mongoClient.getDatabase(appConf.getString("bds.mongodb.mongodb_" + args(0).toLowerCase()))
    val collection = database.getCollection(appConf.getString("bds.mongodb.config_collection"))

    val searchQuery = new BasicDBObject();
    searchQuery.put("name", "main");
    val cursor = collection.find(searchQuery);
    val ir = cursor.iterator();
    var document: Document = null;
    if (ir.hasNext()) {
      document = ir.next();
    }

    val bdsConfig = BDSMongoDBConfigInfo(document.getString("name"), document.getString("jobSequence"), document.getString("sparkJars"),
      document.getString("sparkJarsLocation"), document.getString("sparkHomeLocation"),
      document.getString("appResourceJar"), document.getInteger("threadSleepInMins"),
      document.getString("sparkExecutorExtraClassPath"), document.getString("deployMode"), document.getBoolean("checkFrequency"),
      document.getBoolean("toTerminateAfterThreadAwake")
      
      ) //Main

    val jobs = bdsConfig.jobSequence.split(",")
    if (args.length == 1 || "ALL".equalsIgnoreCase(args(1))) {
      jobs.foreach(jobName => {
        log.debug(s"Calling the job name ${jobName}")
        SparkJobLanucher.launchJob(jobName, bdsConfig, collection)
        log.debug(s"Execution done for ${jobName}")
      })
    } else {
      log.debug(s"Calling the job for ${args(1)}")
      SparkJobLanucher.launchJob(args(1), bdsConfig, collection)
      log.debug(s"Execution done for ${args(1)}")
    }
  }
}