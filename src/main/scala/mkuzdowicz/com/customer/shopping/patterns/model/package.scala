package mkuzdowicz.com.customer.shopping.patterns

package object model {

  case class Customer(customer_id: String, loyalty_score: Int)

  case class Product(product_id: String, product_description: String, product_category: String)

  case class Transaction(customer_id: String, date_of_purchase: java.sql.Timestamp, basket: List[BasketItem])

  case class BasketItem(product_id: String, price: Double)

  case class CustomerShoppingPattern(
                                      customer_id: String,
                                      loyalty_score: Int,
                                      product_id: String,
                                      product_category: String,
                                      purchase_count: Long
                                    )

}
