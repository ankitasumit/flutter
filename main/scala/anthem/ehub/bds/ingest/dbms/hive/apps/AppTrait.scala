package anthem.ehub.bds.ingest.dbms.hive.apps
import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

import anthem.ehub.bds.utils.Parameter

trait AppTrait {
  def process(param: Parameter, properties: Properties, sc: SparkContext, sqlCntxt: SparkSession)

  def upateTimeStamp(dataFrame: DataFrame, sqlContext: SQLContext): DataFrame = {
    val timeStampFields = dataFrame.dtypes.filter(colName => colName._2 == "TimestampType").map(tup => tup._1)

    import java.sql.Timestamp
    val convertTimeStamp = (timeValue: java.sql.Timestamp) => {
      if (timeValue != null) {
        if (timeValue.compareTo(Timestamp.valueOf("9999-12-30 00:00:00.0")) >= 0) {
          Timestamp.valueOf("9999-12-31 00:00:00.0")
        } else {
          timeValue
        }
      } else {
        null
      }
    }
    sqlContext.udf.register("convertTimeStamp", convertTimeStamp)
    println("Registering the schema")
    val columnNames = dataFrame.dtypes;
    val convertedCols = columnNames.map(colName =>
      if (timeStampFields.contains(colName._1)) {
        "convertTimeStamp(" + colName._1 + ")"
      } else {
        colName._1
      })
    //Select EHUB_MBR_ID,MBR_ID,SOR_CD,ID,ID_DESC,convertTimeStamp(ID_STRT_DT),convertTimeStamp(ID_END_DT),
    //ID_TYPE_SRC_CD_VAL_TXT,ID_TYPE_CLMN_PHYSCL_NM,ID_TYPE_TRGT_CD_VAL_TXT,
    //convertTimeStamp(LAST_UPDT_DTM),LAST_UPDT_USER_ID from hiveTableData
    val queryString = "Select " + convertedCols.mkString(",") + " from hiveTableData ";
    println("Query is ***************************" + queryString);
    dataFrame.registerTempTable("hiveTableData");
    val finalDF = sqlContext.sql(queryString);
    finalDF
  }
}