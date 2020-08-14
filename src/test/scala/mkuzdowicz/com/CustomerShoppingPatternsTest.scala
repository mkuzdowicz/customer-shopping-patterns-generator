package mkuzdowicz.com

import java.io.File
import java.sql.Timestamp

import mkuzdowicz.com.customer.shopping.patterns.model.CustomerShoppingPattern
import mkuzdowicz.com.customer.shopping.patterns.{CustomerShoppingPatterns, InputPaths, PipelineConfig, model}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class CustomerShoppingPatternsTest extends FlatSpec with Matchers with SparkTest with BeforeAndAfterAll {

  private val cfg = PipelineConfig(
    inputPaths = InputPaths(
      customers = "src/test/resources/input_data/customers/",
      products = "src/test/resources/input_data/products/",
      transactions = "src/test/resources/input_data/transactions/"
    ),
    outputPath = "src/test/resources/output_data/"
  )

  private val pipeline = new CustomerShoppingPatterns(cfg)

  import spark.implicits._

  private val customersDS = spark.createDataset(Seq(
    model.Customer("C1", 7),
    model.Customer("C2", 4),
    model.Customer("C3", 8)
  ))

  private val productsDS = spark.createDataset(Seq(
    model.Product("P01", "detergent", "house"),
    model.Product("P02", "shorts", "clothes"),
    model.Product("P03", "cherry", "fruit_veg")
  ))

  private val transactionsDS = spark.createDataset(Seq(
    model.Transaction("C1", Timestamp.valueOf("2020-01-22 23:14:37.762209"), List(model.BasketItem("P01", 100))),
    model.Transaction("C1", Timestamp.valueOf("2019-12-10 23:14:37.762209"), List(model.BasketItem("P01", 90))),

    model.Transaction("C1", Timestamp.valueOf("2019-12-10 23:14:37.762209"), List(model.BasketItem("P02", 33))),

    model.Transaction("C2", Timestamp.valueOf("2020-01-05 23:14:37.762209"), List(model.BasketItem("P02", 22))),
    model.Transaction("C2", Timestamp.valueOf("2020-01-15 23:14:37.762209"), List(model.BasketItem("P02", 20))),
    model.Transaction("C2", Timestamp.valueOf("2020-01-20 23:14:37.762209"), List(model.BasketItem("P02", 9))),
    model.Transaction("C2", Timestamp.valueOf("2020-01-20 23:14:37.762209"), List(model.BasketItem("P02", 22))),

    model.Transaction("C2", Timestamp.valueOf("2019-12-10 11:14:37.762209"), List(model.BasketItem("P01", 100))),

    model.Transaction("C3", Timestamp.valueOf("2020-01-21 23:14:37.762209"), List(model.BasketItem("P03", 33)))
  ))

  private def writeTestSourceData() = {
    customersDS.write
      .option("header", "true")
      .csv(cfg.inputPaths.customers)

    productsDS.write
      .option("header", "true")
      .csv(cfg.inputPaths.products)

    transactionsDS.write.json(cfg.inputPaths.transactions)
  }

  override def beforeAll() = {
    FileUtils.deleteDirectory(new File("src/test/resources/input_data"))
    FileUtils.deleteDirectory(new File("src/test/resources/output_data"))
    writeTestSourceData()
  }

  it should "load and transform input data into CustomerShoppingPattern Report" in {

    val (customersDS, productsDS, transactionsDS) = pipeline.loadDataSets(spark)

    val actual = pipeline.generatePatterns(spark, customersDS, productsDS, transactionsDS)

    actual.collect() should contain theSameElementsAs Seq(
      CustomerShoppingPattern("C1", 7, "P01", "house", 2),
      CustomerShoppingPattern("C1", 7, "P02", "clothes", 1),
      CustomerShoppingPattern("C2", 4, "P02", "clothes", 4),
      CustomerShoppingPattern("C2", 4, "P01", "house", 1),
      CustomerShoppingPattern("C3", 8, "P03", "fruit_veg", 1),
    )

  }

}
