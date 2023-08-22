package anthem.ehub.bds.config

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.launcher.SparkAppHandle
import org.apache.spark.launcher.SparkLauncher
import org.bson.Document
import org.joda._
import org.joda.time.DateTime
import org.joda.time.Days
import org.joda.time.Months
import org.joda.time.Weeks

import com.mongodb.BasicDBObject
import com.mongodb.client.MongoCollection
import java.util.Arrays.ArrayList
import scala.collection.mutable.ListBuffer
import org.apache.log4j.Logger


/**
 * @author AF55641
 * This Object is used to launch the Spark Job based on the configurations
 */
object SparkJobLanucher {
  val frequency = Set("DAILY", "WEEKLY", "MONTHLY");
  val log = Logger.getLogger("anthem.ehub.bds.config.SparkJobLanucher")
   
  /**
 * Launches the Job
 * @param bdsMongoJobInfo
 * @param bdsConfig
 */
def launchJob(jobActivityName:String, bdsConfig:BDSMongoDBConfigInfo,collection:MongoCollection[Document]) {
    log.debug("Processing "+jobActivityName)
      val jobs = getJobsToExecuteInPrallel(jobActivityName, collection)
      if(jobs != null && jobs.size>0) { //jobs have to be executed in Parallel
        var handles =  new ListBuffer[SparkAppHandle]()
         jobs.foreach(jobName => {
           log.debug("Parallel Job Name is "+jobName)
           val bdsMongoJobInfoJob = getJobInfo(getDataFromCollection("name", jobName, collection), bdsConfig)
           if(bdsMongoJobInfoJob != null && bdsMongoJobInfoJob.isActive && checkIfJobCanExecute(bdsMongoJobInfoJob,bdsConfig)) {
                val sparkHandle = sparkSequenceLauncher(bdsMongoJobInfoJob, bdsConfig).startApplication()
                log.debug(s"Executed Job name in parallel ${jobName}")
                updateMongo(jobName, collection)
                handles += sparkHandle
           } else {
             log.debug("Frequency does not match, check the frequency it should be DAILY,WEEKLY, MONTHLY or check if the job is active or not "+jobName)
           }
         })
          if(handles != null && handles.size>0) {
             var flag = true;
             while(flag) {
            	 Thread.sleep(bdsConfig.threadSleep*(1000*60))
            	 checkToTerminate(collection,handles, null) //Checks the toTerminateAfterThreadAwake
            	 val listBoolean:ListBuffer[Boolean]= new ListBuffer[Boolean]
               handles.foreach(sparkSubmitHandle => {
                 log.debug(s"Job application ${sparkSubmitHandle.getAppId} state is ${sparkSubmitHandle.getState.name()} - is final "+sparkSubmitHandle.getState.isFinal())
                 listBoolean.append(sparkSubmitHandle.getState.isFinal())
               })
             
              flag = !listBoolean.foldLeft(true)(_ && _) //Checking the list of booleans if all true then while will exits else it will continue
              log.debug("All jobs completed "+ !flag)
             }
           }
      } else {
        val bdsMongoJobInfoJob = getJobInfo(getDataFromCollection("name", jobActivityName, collection), bdsConfig)
       
        log.debug("bdsMongoJobInfoJob.isActive"+bdsMongoJobInfoJob.isActive)
         if(bdsMongoJobInfoJob != null && checkIfJobCanExecute(bdsMongoJobInfoJob,bdsConfig) && bdsMongoJobInfoJob.isActive) {
            val sparkHandle = sparkSequenceLauncher(bdsMongoJobInfoJob, bdsConfig).startApplication(SparkJobSequenceListener)
            log.debug(s"Executed Job name in  ${jobActivityName}")
            updateMongo(bdsMongoJobInfoJob.name, collection)
            while (!sparkHandle.getState.isFinal()) {
            Thread.sleep(bdsConfig.threadSleep*(1000*60)) // Checking for every n minutes
            checkToTerminate(collection,null, sparkHandle)
            log.debug(s"Current Status of the Spark Job is ${sparkHandle.getAppId} --${sparkHandle.getState}  --- Job Name - ${jobActivityName}")
            if (sparkHandle.getState.isFinal) {
              log.debug(s"Spark job is completed with state ${sparkHandle.getState.isFinal}- please check the yarn ui for further details ${sparkHandle.getAppId} ")
            }
          }
            
         } else {
            log.debug("Frequency does not match, check the frequency it should be DAILY,WEEKLY, MONTHLY or check if the job is active or not")
        }
      }
  }

  /**
   * Checks whether the job needs to terminate and also terminates
   * @param collection
   */
  def checkToTerminate(collection:MongoCollection[Document],handles: ListBuffer[SparkAppHandle], sparkSubmitHandle: SparkAppHandle) = {
    log.debug("Check if the program needs to exit ")
    val document = getDataFromCollection("name", "main", collection)
    log.debug(" Exit " + document.getBoolean("toTerminateAfterThreadAwake"))
    if (document.getBoolean("toTerminateAfterThreadAwake")) {
      if (handles != null) {
        handles.foreach(sparkHandle => {
          terminateJob(sparkHandle)
        })
      } else {
        terminateJob(sparkSubmitHandle)
      }
      log.debug("Program will exit ")
      System.exit(1)
    }
  }

/**
 * 
 * @param sparkAppHandle
 */
def terminateJob(sparkAppHandle:SparkAppHandle) = {
  log.debug(s"Job application ${sparkAppHandle.getAppId} current state is ${sparkAppHandle.getState.name()} and this job will be killed since toTerminateAfterThreadAwake is true")
  if(!sparkAppHandle.getState.isFinal()) {
    sparkAppHandle.stop() //
    Thread.sleep(3000) //To give a pause for to clean shutdown
     log.debug(s"After ${sparkAppHandle.getAppId} current state is ${sparkAppHandle.getState.name()} and this job will be killed since toTerminateAfterThreadAwake is true")
	  sparkAppHandle.kill()
	  log.debug("Job has killed")
  } else {
     log.debug("Job has already finished")
  }
}

/**
 * Fetches the information about the jobs which needs to be executed in Parallel
 * @param activityName
 * @param collection
 * @return
 */
def getJobsToExecuteInPrallel(activityName:String, collection:MongoCollection[Document]) :ListBuffer[String]={
  log.debug("activity name is "+activityName)  
  val searchQuery = new BasicDBObject();
    searchQuery.put("name", activityName);
    val cursor = collection.find(searchQuery);
    val ite = cursor.iterator();
    var document1:Document = null;
    var jobs:ListBuffer[String] = null;
    while(ite.hasNext()) {
     document1= ite.next();
     log.debug(document1)
     if(document1 != null)
     log.debug("***********************8*"+document1.getString("jobs"))
    }
    if(document1!= null && document1.getString("jobs") !=null) {
      jobs =ListBuffer(document1.getString("jobs").trim().split(","):_*)
    } else {
       log.debug("Document is null")
      jobs = new ListBuffer[String]()
    }
    jobs
}


/**
 * Updates the lastExecutionDate and nextInvocation date on the Mongo Job's
 * @param bdsMongoJobInfo
 * @param collection
 */
def updateMongo(jobName:String, collection:MongoCollection[Document]) = {
    log.debug(s"Updating the last execution date and next invocation date for job ${jobName}")
    val query = new BasicDBObject();
    val filter = new Document("name", jobName);
    val udpatValue = new Document();
    udpatValue.put("lastExecutionDate", new Date())
    udpatValue.put("nextInvocationDate", new Date()) //Based on frequency //TODO : Need to fix, this field is not being used
    val updateOperationDocument = new Document("$set", udpatValue);
    collection.updateOne(filter, updateOperationDocument)
    log.debug("Updated the record")

  }
  /**
 * This method creates the Spark Job Launcher based on BDSConfigInfo and BDSMongoJobsInfo
 * @param bdsMongoJobInfo
 * @param bdsConfig
 */
def sparkSequenceLauncher(bdsMongoJobInfo:BDSMongoJobsInfo, bdsConfig:BDSMongoDBConfigInfo) = {
    val arguments = bdsMongoJobInfo.parameters.split(",")
    log.debug("Deployed mode is cluster Arguments are varguments:_*")
    log.debug("Spark qualified name "+bdsMongoJobInfo.fullQualifiedSparkJobName)
    log.debug("Spark app resource jar "+bdsConfig.sparkJarsLocation+"/"+bdsMongoJobInfo.appResourceJar)
    log.debug("Spark Master "+bdsMongoJobInfo.sparkMaster)
    log.debug("Spark executor cores "+bdsMongoJobInfo.sparkExecutorCores)
    log.debug("Spark executor instances "+bdsMongoJobInfo.sparkExecutorInstances)
    log.debug("Spark driver memory "+bdsMongoJobInfo.sparkDriverMemory)
    log.debug("Spark driver cores "+bdsMongoJobInfo.sparkDriverCores)
    log.debug("Spark driver class path "+bdsMongoJobInfo.sparkDriverClassPath)
    log.debug("Spark driver dynamic allocation "+bdsMongoJobInfo.sparkDynamicAllocation)
    log.debug("Spark home location "+bdsConfig.sparkHomeLocation)
    log.debug("Deploy Mode is  "+bdsConfig.deployMode)
    var launcher = new SparkLauncher()
    .setConf("spark.app.name", bdsMongoJobInfo.sparkAppName)
      .setMainClass(bdsMongoJobInfo.fullQualifiedSparkJobName)
      .setAppResource(bdsConfig.sparkJarsLocation+"/"+bdsMongoJobInfo.appResourceJar)
      .setMaster(bdsMongoJobInfo.sparkMaster)
      .setConf("spark.executor.cores", bdsMongoJobInfo.sparkExecutorCores)
      .setConf("spark.executor.memory", bdsMongoJobInfo.sparkExecutorMemory)
      .setConf("spark.executor.instances", bdsMongoJobInfo.sparkExecutorInstances)
      .setConf("spark.driver.memory", bdsMongoJobInfo.sparkDriverMemory)
      .setConf("spark.driver.cores", bdsMongoJobInfo.sparkDriverCores)
      .setConf("spark.driver.extraClassPath", bdsMongoJobInfo.sparkDriverClassPath)
      .setConf("spark.dynamicAllocation.enabled", bdsMongoJobInfo.sparkDynamicAllocation)
      .addAppArgs(arguments:_*)
      .setSparkHome(bdsConfig.sparkHomeLocation)
      .setVerbose(false)
      val supportingJars = bdsMongoJobInfo.sparkSupportingJars.split(",")
      log.debug(bdsConfig.sparkJarsLocation)
      log.debug(supportingJars)
      supportingJars.foreach(jar =>
       launcher= launcher.addJar(bdsConfig.sparkJarsLocation+"/"+jar)
      )
      launcher.setDeployMode(bdsConfig.deployMode)
  }
  
 
  /**
 * This method checks the given frequency with current date to identify whether the job is executable or not
 * @param frequency
 * @param lastExecutionDate
 * @return
 */
def checkIfJobCanExecute(bdsMongoJobsInfo:BDSMongoJobsInfo,bdsConfig:BDSMongoDBConfigInfo) ={
     
    if(bdsConfig.checkFrequency) {
    	val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
    			val lastExecutedDate = new DateTime(dateFormat.format(bdsMongoJobsInfo.lastExecutionDate))
    			val todaysDate = new DateTime(dateFormat.format(new Date()))
    			val frequency = bdsMongoJobsInfo.frequency.toUpperCase().trim()
    			log.debug("Frequency is "+frequency)
    			
    	val execute = frequency match {
    	case "DAILY" if(Days.daysBetween(lastExecutedDate,todaysDate).getDays>0) =>true
    	case "WEEKLY" if(Weeks.weeksBetween(lastExecutedDate,todaysDate).getWeeks>0) =>true
    	case "MONTHLY" if(Months.monthsBetween(lastExecutedDate,todaysDate).getMonths>0) =>true
    	
    	}
    	log.debug("Job can be executed based on the configuration "+execute) 
    	execute
    } else {
      true
    }
  }

/**
 * Populates the information in case class
 * @param document
 * @param bdsConfig
 * @return
 */
def getJobInfo(document:Document, bdsConfig:BDSMongoDBConfigInfo)= {
        BDSMongoJobsInfo(
                            document.getString("name"), 
                            document.getBoolean("isActive"),
                            document.getString("frequency").toUpperCase(),
                            document.getDate("lastExecutionDate"), 
                            document.getDate("nextInvocationDate"), 
                            document.getString("fullQualifiedSparkJobName"),
                            document.getString("parameters"),
                            document.getString("sparkMaster"), 
                            document.getString("sparkExecutorCores"), 
                            document.getString("sparkExecutorInstances"),
                            document.getString("sparkExecutorMemory"), 
                            document.getString("sparkDriverMemory"),
                            document.getString("sparkAppName"),
                            document.getString("sparkDriverClassPath"), 
                            bdsConfig.sparkJars, 
                            document.getString("sparkDriverCores"),
                            if(document.getString("appResourceJar") != null && document.getString("appResourceJar").trim().length()>0) {
                              document.getString("appResourceJar") 
                            } else {
                             bdsConfig.appResourceJar
                            } , 
                            document.getString("sparkDynamicAllocation")
                            )
}

/**
 * Fetches the document from collection based on field and vlaue
 * @param field
 * @param value
 * @param collection
 * @return
 */
def getDataFromCollection(field:String, value:String, collection:MongoCollection[Document]) ={
    val searchQuery = new BasicDBObject();
    searchQuery.put(field, value);
    val cursor = collection.find(searchQuery);
    val ite = cursor.iterator();
    var document1:Document = null;
    var jobs:List[String] = null;
    while(ite.hasNext()) {
     document1= ite.next();
     log.debug(document1)
    }
    document1
}

  /**
 * This method checks the given frequency with current date to identify whether the job is executable or not
 * @param frequency
 * @param lastExecutionDate
 * @return
 */
def checkIfJobCanExecute(frequency:String, lastExecutionDate: Date) ={
     val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
     val lastExecutedDate = new DateTime(dateFormat.format(lastExecutionDate))
     val todaysDate = new DateTime(dateFormat.format(new Date()))

     val execute = frequency match {
       case "DAILY" if(Days.daysBetween(lastExecutedDate,todaysDate).getDays>0) =>true
       case "WEEKLY" if(Weeks.weeksBetween(lastExecutedDate,todaysDate).getWeeks>0) =>true
       case "MONTHLY" if(Months.monthsBetween(lastExecutedDate,todaysDate).getMonths>0) =>true
     }
     execute
  }

object SparkJobSequenceListener extends SparkAppHandle.Listener {

    /**
     * @param handle
     */
    private def info(handle: SparkAppHandle): Unit = {
     log.debug(s"The application Id ${handle.getAppId} status is ${handle.getState} ....")
    }
    /* (non-Javadoc)
     * @see org.apache.spark.launcher.SparkAppHandle.Listener#infoChanged(org.apache.spark.launcher.SparkAppHandle)
     */
    override def infoChanged(handle: SparkAppHandle): Unit = {
      info(handle)
    }
    /* (non-Javadoc)
     * @see org.apache.spark.launcher.SparkAppHandle.Listener#stateChanged(org.apache.spark.launcher.SparkAppHandle)
     */
    override def stateChanged(handle: SparkAppHandle): Unit = {
     info(handle)
      if (handle.getState.isFinal) {
         log.debug(s"Spark job is completed - please check the yarn ui for further details ${handle.getAppId}")
      }
    }
  }
}