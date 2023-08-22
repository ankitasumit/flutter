
package anthem.ehub.bds.ingest.dbms.hive.driver

/**
 * This Scala code reads the passed arguments and invokes the RunMongo with the passed parameter
 *
 */
object DBToHive {
  def main(args: Array[String]): Unit = {
      
    if (args.size == 3) {
      val frequency = Set("DAILY_LOAD", "WEEKLY_LOAD", "MONTHLY_LOAD","DAILY_LOAD_BNFT")
      println(args.mkString("  "))
      println(frequency)
     if (frequency.contains(args(1).toUpperCase())) {
        println("Frequency is "+args(1))
        RunMongo.process(args(0), args(1), args(2))
    }
    }
  }
}
//daily_load_bnft