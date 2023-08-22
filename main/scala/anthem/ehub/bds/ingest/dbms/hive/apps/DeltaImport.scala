package anthem.ehub.bds.ingest.dbms.hive.apps
import java.util.Properties

import org.apache.spark.SparkContext

import com.typesafe.config.ConfigFactory

import anthem.ehub.bds.utils.Parameter
import org.apache.spark.sql.SparkSession

object DeltaImport {
  def process(param: Parameter,properties:Properties, sc: SparkContext, sqlCntx: SparkSession) =
    {

      val db = param.target.split("\\.")(0)
      val table = param.target.split("\\.")(1)
      val properties = new Properties()
      
      val lastupdated_by = param.lastupdated_by
      val primary_key = param.primarykey
      val lastupdated_value = param.lastupdated_by
      val dbserverurl = properties.getProperty("dbserverurl")
      
      if (lastupdated_by != "" && primary_key != "" && lastupdated_value != "") {

        val delta_query = "SELECT * FROM database WHERE $lastupdated_by > $lastupdated_value"

        val delta_df = sqlCntx.read.jdbc(dbserverurl, s"($delta_query) B", properties)
        sqlCntx.sql(s"use $db")
        val table_df = sqlCntx.sql(s"select * from $table")

        delta_df.registerTempTable("delta_table")
        table_df.registerTempTable("trgt_table")

        val upsets =s"with union_tmp as (select * from trgt_table union all select * from delta_table) select * from (select *, ROW_NUMBER() OVER (PARTITION BY $primary_key ORDER BY $lastupdated_by DESC) AS rnk from union_tmp ) A where rnk =1"
        
        val final_df = sqlCntx.sql(s"$table").drop("rnk")
       // val new_df = sqlCntx.sql(s"select D.* from delta_table D left outer join trgt_table T on D.$primary_key = T.$primary_key where D.$primary_key is null")
        final_df.registerTempTable("final_table")
        sqlCntx.sql(s"insert overwrite into table $table select * from final_table")
      }

    }
}