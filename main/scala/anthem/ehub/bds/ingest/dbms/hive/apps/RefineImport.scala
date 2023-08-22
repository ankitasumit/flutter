package anthem.ehub.bds.ingest.dbms.hive.apps
import java.util.Properties

import org.apache.spark.SparkContext

import com.typesafe.config.ConfigFactory

import anthem.ehub.bds.utils.Parameter
import org.apache.spark.sql.SparkSession

object RefineImport extends AppTrait {
  def process(param: Parameter,properties:Properties, sc: SparkContext, sqlCntx: SparkSession) =
    {
      
      val primarykey = param.primarykey
      val source = param.Source
      val delta = param.delta
      val dbserverurl = properties.getProperty("dbserverurl")
      
      val maxdf = sqlCntx.read.jdbc(dbserverurl, s"(select count(1) from $source) B", properties)
      maxdf.show()
      val count_max = maxdf.first()(0).asInstanceOf[java.math.BigDecimal].longValue()
      val table_df = sqlCntx.read.jdbc(dbserverurl, s"($delta) B", properties)

      val db = param.target.split("\\.")(0)
      val table = param.target.split("\\.")(1)
      sqlCntx.sql("use " + db)

      val t = table_df.drop("insert_num")
      t.registerTempTable("tmp_tbl")
      sqlCntx.sql(s"use $db")
      sqlCntx.sql(s"DROP TABLE IF EXISTS $table")
      sqlCntx.sql(s"create table $table as select * from tmp_tbl")

    }
}