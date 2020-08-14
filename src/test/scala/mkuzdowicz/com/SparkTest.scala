package mkuzdowicz.com

import org.apache.spark.sql.SparkSession

trait SparkTest {

  val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("DataTest")
    .getOrCreate()

}
