package mkuzdowicz.com.customer.shopping.patterns

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.joda.time.LocalDate


object Application extends App {

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  val spark = SparkSession.builder()
    .master("local")
    .appName("customer-shopping-patterns")
    .getOrCreate()

  // make spark less verbose
  spark.sparkContext.setLogLevel(Level.ERROR.toString)

  val cfg = PipelineConfig(
    inputPaths = InputPaths(
      customers = "src/main/resources/input_data/customers.csv",
      products = "src/main/resources/input_data/products.csv",
      transactions = "src/main/resources/input_data/transactions/"
    ),
    outputPath = "src/main/resources/output_data/outputs"
  )

  val now = LocalDate.now()
  val runId = now.toString + "-" + System.currentTimeMillis()

  val customerShoppingPatterns = new CustomerShoppingPatterns(cfg)

  val (customersDS, productsDS, transactionsDS) = customerShoppingPatterns.loadDataSets(spark)

  val patterns = customerShoppingPatterns
    .generatePatterns(spark, customersDS, productsDS, transactionsDS)

  patterns.repartition(1).write
    .option("header", "true").csv(s"${cfg.outputPath}/$runId")
}

