package mkuzdowicz.com.customer.shopping.patterns

import mkuzdowicz.com.customer.shopping.patterns.model.CustomerShoppingPattern
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}

case class InputPaths(customers: String, products: String, transactions: String)

case class PipelineConfig(inputPaths: InputPaths, outputPath: String)

class CustomerShoppingPatterns(config: PipelineConfig) {

  import config._

  def loadDataSets(spark: SparkSession): (Dataset[model.Customer],
    Dataset[model.Product], Dataset[model.Transaction]) = {

    import spark.implicits._

    val customersDS = spark.read
      .option("header", "true")
      .schema(ScalaReflection.schemaFor[model.Customer].dataType.asInstanceOf[StructType])
      .csv(inputPaths.customers)
      .as[mkuzdowicz.com.customer.shopping.patterns.model.Customer]

    val productsDS = spark.read
      .option("header", "true")
      .schema(ScalaReflection.schemaFor[model.Product].dataType.asInstanceOf[StructType])
      .csv(inputPaths.products)
      .as[mkuzdowicz.com.customer.shopping.patterns.model.Product]

    val transactionsDS = spark.read
      .schema(ScalaReflection.schemaFor[model.Transaction].dataType.asInstanceOf[StructType])
      .json(inputPaths.transactions)
      .as[mkuzdowicz.com.customer.shopping.patterns.model.Transaction]

    (customersDS, productsDS, transactionsDS)
  }

  def generatePatterns(spark: SparkSession,
                       customers: Dataset[model.Customer],
                       products: Dataset[model.Product],
                       transactions: Dataset[model.Transaction]) = {

    import spark.implicits._

    val customerProduct: Dataset[(String, String)] = transactions.flatMap {
      t =>
        t.basket.map(b => (t.customer_id, b.product_id))
    }

    val grouped = customerProduct
      .groupBy($"_1".alias("customer_id"), $"_2".alias("product_id"))
      .agg(count("*").alias("purchase_count"))

    grouped.join(customers, "customer_id")
      .join(products, "product_id")
      .select($"customer_id", $"loyalty_score", $"product_id",
        $"product_category", $"purchase_count")
      .as[CustomerShoppingPattern]
  }

}