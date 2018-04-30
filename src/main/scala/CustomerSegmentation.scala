import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.rdd.RDD

class CustomerSegmentation {

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

  def createSparkSession(mode: String, appName: String): SparkSession = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val newSparkSession = SparkSession
      .builder()
      .master(mode)
      .appName(appName)
      .getOrCreate()

    newSparkSession
  }

  def createVectors(sqlContext: SQLContext) = {
    val retailDataFileExcel = "C:\\Users\\Nikhil\\Documents\\Projects\\Data\\Online Retail.xlsx"
    val retailDataFileCSV = "C:\\Users\\Nikhil\\Documents\\Projects\\Data\\Online Retail.csv"

    val retailDataRaw = readFile("csv", sqlContext, retailDataFileCSV)

    // Drop null values
    val slimmedData = retailDataRaw.na.drop("any")

    // Add price column
    val dataWithPrice = slimmedData.withColumn("Total_Price", round(slimmedData.col("Quantity")*slimmedData.col("UnitPrice")))

    // Cache this DF as we will use this several times
    dataWithPrice.cache()

    // Convert Timestamp data to dd/MM/yy HH:mm format
    val newTimestampFormat = to_timestamp(dataWithPrice.col("InvoiceDate"), "dd/MM/yy HH:mm")
    val timestampDF = dataWithPrice.withColumn("NewInvoiceDate", newTimestampFormat)

    // Calculate Recency

    // Get Most Recent Invoice Date
    val mostRecentInvoiceTimeStamp = timestampDF.agg(max("NewInvoiceDate")).collect()(0).getTimestamp(0)

    val daysBeforeDF = timestampDF.withColumn("DaysBefore", datediff( lit(mostRecentInvoiceTimeStamp), timestampDF.col("NewInvoiceDate")))

    val recencyDF = daysBeforeDF.groupBy("CustomerID").agg(min("DaysBefore").alias("Recency"))

    // Calculate Frequency
    val frequencyDF = dataWithPrice.groupBy("CustomerID", "InvoiceNo").count()
      .groupBy("CustomerID")
      .agg(count("*")
        .alias("Frequency"))

    // Calculate Monetary value
    val monetaryDF = dataWithPrice
      .groupBy("CustomerID")
      .agg(round(sum("Total_Price"), 2)
        .alias("Monetary"))

    val mfDF = monetaryDF
      .join(frequencyDF, monetaryDF.col("CustomerID") === frequencyDF.col("CustomerID"), "inner")
      .drop(frequencyDF.col("CustomerID"))

    val rfmDF = mfDF
      .join(recencyDF, recencyDF.col("CustomerID") === mfDF.col("CustomerID"), "inner")
      .drop(recencyDF.col("CustomerID"))

    val orderedRFM = rfmDF.select( "CustomerID", "Recency", "Frequency", "Monetary" )

    orderedRFM.cache()

    val customerLabels = orderedRFM.rdd.map(rec => rec(0))

    val customerVecs = orderedRFM.rdd.map{ rec =>
      val recency = rec.getInt(0)
      val frequency = rec.getInt(1)
      val monetary = rec.getLong(2)
      Vectors.dense(recency, frequency, monetary)
    }

    // Standardize Vectors
    val standardizer = new StandardScaler(true, true)

    val standardizedModel = standardizer.fit(customerVecs)

    val customerData = customerLabels.zip(standardizedModel.transform(customerVecs))

    customerData.take(5).foreach(println)

    /*val customerRfmVals = customerData.map(rec => rec(1))

    customerRfmVals.take(5).foreach(println)

    customerRfmVals*/
  }

  def createSegments(numClusters: Int, numIterations: Int, sparkSession: SparkSession) = {
    val sqlContext = sparkSession.sqlContext

    val inputVectors = createVectors(sqlContext)

    //inputVectors.foreach(println)

    //val clusters = KMeans.train(inputVectors, numClusters, numIterations)
  }
}

object CustomerSegmentationModule extends CustomerSegmentation {

  val mySession = createSparkSession("local[*]", "Vicki")

  def main(args: Array[String]): Unit = {
    createSegments(2, 20, mySession)
  }
}
