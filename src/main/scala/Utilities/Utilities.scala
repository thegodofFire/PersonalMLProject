package Utilities

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}

class Utilities {

  val customer_schema =
    StructType(
      Array(
        StructField("CustomerID", IntegerType, true),
        StructField("ClusterLabel", IntegerType, true)
      )
    )

  def readFile(fileType: String, sqlContext: SQLContext, file: String): DataFrame = {
    if (fileType.equalsIgnoreCase("excel")) {
      sqlContext
        .read
        .format("com.crealytics.spark.excel")
        .option("location", file)
        .option("useHeader", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .option("inferSchema", "true")
        .option("addColorColumns", "False")
        .load(file)
    } else {
      sqlContext
        .read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(file)
    }
  }

}
