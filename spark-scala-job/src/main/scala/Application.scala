import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode

object Application extends App {

  val spark = SparkSession.builder() 
    .appName("Migracion de de datos")
    .master("local")
    .getOrCreate()
  
  /*
   * Posibles dimensiones y hecho:
   *  - FACT: Sale
   *    ALL POINTERS,
   *    Cost,
   *    Review_Rating
   *  - Dim: Customer
   *    Customer_ID,
   *    Age,
   *    Gender,
   *    Subscription_Status,
   *    Previous_Purchases,
   *    Purchase_Frequency
   *  - Dim: Product
   *    Item_Purchased,
   *    Category,
   *    Size,
   *    Color
   *  - Dim: Payment_Detail
   *    Shipping_Type,
   *    Discount_Applied,
   *    Promo_Code_Used,
   *    Payment_Method
   *  - Dim: Location
   *    Location
   */

  val shopSchema = StructType(
    Array(
      StructField("Customer_ID", IntegerType, false),
      StructField("Age", IntegerType, false),
      StructField("Gender", StringType, false),
      StructField("Item_Purchased", StringType, false),
      StructField("Category", StringType, false),
      StructField("Cost", IntegerType, false),
      StructField("Location", StringType, false),
      StructField("Size", StringType, false),
      StructField("Color", StringType, false),
      StructField("Season", StringType, false),
      StructField("Review_Rating", DoubleType, false),
      StructField("Subscription_Status", StringType, false),
      StructField("Shipping_Type", StringType, false),
      StructField("Discount_Applied", StringType, false),
      StructField("Promo_Code_Used", StringType, false),
      StructField("Previous_Purchases", IntegerType, false),
      StructField("Payment_Method", StringType, false),
      StructField("Purchase_Frequency", StringType, false),
    )
  )

  val shopDF = spark.read
    .schema(shopSchema)
    .options(Map(
      "path" -> "src/main/data/shopping_trends_updated.csv",
      "header" -> "true",
      "sep" -> ",",
    ))
    .csv()

  shopDF.cache().count()

  shopDF.printSchema()

  shopDF.select(count("*")).show()   

  // No hay nulos
  shopDF.select(shopDF.columns.map(c => count(when(col(c).isNull || col(c) === "" || col(c).isNaN, c)).alias(c)): _*).show()

  // Seleccion de tablas
  val customerDF = shopDF.select(
    col("Customer_ID"),
    col("Age"),
    col("Gender"),
    col("Subscription_Status"),
    col("Previous_Purchases"),
    col("Purchase_Frequency")
  )

  val finalCustomerDF = customerDF.groupBy(
    col("Customer_ID"),
    col("Age"),
    col("Gender"),
    col("Subscription_Status"),
    col("Previous_Purchases"),
    col("Purchase_Frequency")
  ).agg(
    count("*").as("Times_Bought")
  )

  val productDF = shopDF.select(
    col("Item_Purchased"),
    col("Category"),
    col("Size"),
    col("Color")
  )

  val finalProductDF = productDF.groupBy(
    col("Item_Purchased"),
    col("Category"),
    col("Size"),
    col("Color")
  )
  .agg(
    count("*").as("Times_Bought")
  )
  .withColumn("Product_ID", monotonically_increasing_id())

  val paymentDetailDF = shopDF.select(
    col("Shipping_Type"),
    col("Discount_Applied"),
    col("Promo_Code_Used"),
    col("Payment_Method")
  )

  val finalPaymentDetailDF = paymentDetailDF.withColumn("Detail_ID", monotonically_increasing_id())

  val finalLocationDF = shopDF.select(
    col("Location")
  ).groupBy(
    col("Location")
  ).agg(
    count("*").as("Appearances")
  ).withColumn("Location_ID", monotonically_increasing_id())

  val factDF = shopDF
    .join(finalProductDF, Seq("Item_Purchased", "Category", "Size", "Color"))
    .join(finalCustomerDF, "Customer_ID")
    .join(finalPaymentDetailDF, Seq("Shipping_Type", "Discount_Applied", "Promo_Code_Used", "Payment_Method"))
    .join(finalLocationDF, "Location")
    .select(
      col("Customer_ID"),
      col("Product_ID"),
      col("Detail_ID"),
      col("Location_ID"),
      col("Cost"),
      col("Review_Rating")
    )

  // TODO: Load into psql
  finalCustomerDF.write
    .format("jdbc")
    .mode(SaveMode.Overwrite)
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5123/datawarehouse",
      "user" -> "tentacion_admin",
      "dbtable" -> "public.dim_customer"
    ))
    .save()

  finalProductDF.write
    .format("jdbc")
    .mode(SaveMode.Overwrite)
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5123/datawarehouse",
      "user" -> "tentacion_admin",
      "dbtable" -> "public.dim_product"
    ))
    .save()

  finalLocationDF.write
    .format("jdbc")
    .mode(SaveMode.Overwrite)
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5123/datawarehouse",
      "user" -> "tentacion_admin",
      "dbtable" -> "public.dim_location"
    ))
    .save()

  finalPaymentDetailDF.write
    .format("jdbc")
    .mode(SaveMode.Overwrite)
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5123/datawarehouse",
      "user" -> "tentacion_admin",
      "dbtable" -> "public.dim_detail"
    ))
    .save()

  factDF.write
    .format("jdbc")
    .mode(SaveMode.Overwrite)
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5123/datawarehouse",
      "user" -> "tentacion_admin",
      "dbtable" -> "public.fact_sale"
    ))
    .save()

  spark.stop()

}
