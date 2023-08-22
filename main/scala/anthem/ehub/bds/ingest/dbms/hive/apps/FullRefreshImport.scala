package anthem.ehub.bds.ingest.dbms.hive.apps
import java.util.Properties
import org.apache.spark.SparkContext
import com.typesafe.config.ConfigFactory
import anthem.ehub.bds.utils.Encryption
import anthem.ehub.bds.utils.Parameter
import org.apache.spark.sql.SparkSession

/**
 * This code does the full refresh of the Oracle data to Hive
 * @author 
 *
 */
object FullRefreshImport extends AppTrait {

  def process(param: Parameter,properties:Properties, sc: SparkContext, sqlCntx: SparkSession) = {
      
      val primarykey = param.primarykey
      val source = param.Source
      val dbserverurl = properties.getProperty("dbserverurl")
      val partitions = Integer.parseInt(properties.getProperty("numpartitions"))
      val hiveFileFormat = properties.getProperty("hivedataformat")
      val mindf = sqlCntx.read.jdbc(dbserverurl, s"(select min(ora_hash($primarykey)) as mi from $source) B", properties)
      val count_min = mindf.first()(0).asInstanceOf[java.math.BigDecimal].longValue()
      val maxdf = sqlCntx.read.jdbc(dbserverurl, s"(select max(ora_hash($primarykey)) as mx from $source) B", properties)
      val count_max = maxdf.first()(0).asInstanceOf[java.math.BigDecimal].longValue()
      
      val dbmsData = sqlCntx.read.jdbc(dbserverurl, s"(select  ora_hash($primarykey) as  insert_num, A.* from $source a) B", "insert_num", count_min, count_max, partitions.toInt, properties)

      val hiveDatabase = param.target.split("\\.")(0)
      println("Hive Database" + hiveDatabase)
      val hiveTableName = param.target.split("\\.")(1)
      println("Hive Table:" + hiveTableName)
      
      sqlCntx.sql("use " + hiveDatabase)

      val finalDBMSData = dbmsData.drop("insert_num")

      sqlCntx.sql(s"use $hiveDatabase")

      println("table  " + hiveTableName)

      println("before ================== drop")
      sqlCntx.sql(s"DROP TABLE IF EXISTS $hiveTableName")

      println("After ================== drop  $table")
      //  else
     
      val schema = finalDBMSData.schema.simpleString.replaceAll("struct<", "").replaceAll(">", "").replaceAll(":", "    ") //.replaceAll("char", "varchar")
      println("SCHEMA ****************** \\n"+schema+"\\n\\n")
      val create_statement = s"create table $hiveTableName ( $schema) stored as parquet"
      
      sqlCntx.sql(s"$create_statement")
      println("After table $table created like tmp_tbl")
      finalDBMSData.write.format(hiveFileFormat).mode("overwrite").insertInto(hiveDatabase + """.""" + hiveTableName)
      println("Data has written to Hive DB Table"+hiveTableName)
    }
}