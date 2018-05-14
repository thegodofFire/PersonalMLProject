import java.io.File

import Utilities.Utilities

import org.apache.commons.io.FileUtils

import org.apache.spark.rdd.RDD

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

//import util.control.Breaks._

class CustomerSegmentation {

  val utilities = new Utilities

  var retailDataRaw: DataFrame = _

  private def createVectors(sqlContext: SQLContext): (RDD[Any], RDD[Vector], DataFrame) = {
    val fileType = 1

    val basePath = "C:\\Users\\Nikhil\\"
    val basePathDFZ = "C:\\Users\\nvellala\\"

    val retailDataFileExcel = basePath + "\\Documents\\Projects\\Data\\Online Retail.xlsx"
    val retailDataFileCSV = basePath + "\\Documents\\Projects\\Data\\Online Retail.csv"

    val retailDataFileExcelDFZ = basePathDFZ + "\\Documents\\Projects\\Data\\Online Retail.xlsx"
    val retailDataFileCsvDFZ = basePathDFZ + "\\Documents\\Projects\\Data\\Online Retail.csv"

    if (fileType == 1) {
      try {
        retailDataRaw = utilities.readFile("csv", sqlContext, retailDataFileCSV)
      } catch {
        case e: Exception => {
          retailDataRaw = utilities.readFile("csv", sqlContext, retailDataFileCsvDFZ)
        }
      }
    } else {
      try {
        retailDataRaw = utilities.readFile("excel", sqlContext, retailDataFileExcel)
      } catch {
        case e: Exception => {
          retailDataRaw = utilities.readFile("excel", sqlContext, retailDataFileExcelDFZ)
        }
      }
    }

    // Save Retail Data
    retailDataRaw
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "true")
      .save("ClusteringData/RawData/retail.csv")

    // Drop null values
    val slimmedData = retailDataRaw.na.drop("any")

    // Add price column
    val dataWithPrice = slimmedData.withColumn("Total_Price", round(slimmedData.col("Quantity") * slimmedData.col("UnitPrice")))

    // Cache this DF as we will use this several times
    dataWithPrice.cache()

    // Convert Timestamp data to dd/MM/yy HH:mm format
    val newTimestampFormat = to_timestamp(dataWithPrice.col("InvoiceDate"), "dd/MM/yy HH:mm")
    val timestampDF = dataWithPrice.withColumn("NewInvoiceDate", newTimestampFormat)

    // Calculate Recency

    // Get Most Recent Invoice Date
    val mostRecentInvoiceTimeStamp = timestampDF.agg(max("NewInvoiceDate")).collect()(0).getTimestamp(0)

    val daysBeforeDF = timestampDF.withColumn("DaysBefore", datediff(lit(mostRecentInvoiceTimeStamp), timestampDF.col("NewInvoiceDate")))

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

    val orderedRFM = rfmDF.select("CustomerID", "Recency", "Frequency", "Monetary")

    orderedRFM.cache()

    val customerLabels = orderedRFM.rdd.map(rec => rec(0))

    val customerVecs = orderedRFM.rdd.map { rec =>
      try {
        val recency = rec.getInt(0)
        val frequency = rec.getInt(1)
        val monetary = rec.getLong(2)

        Vectors.dense(recency, frequency, monetary)
      } catch {
        case e: Exception => {
          Vectors.dense(0, 0, 0.0)
        }
      }
    }

    // Standardize Vectors
    val standardizer = new StandardScaler(true, true)

    val standardizedModel = standardizer.fit(customerVecs)

    val customerData = customerLabels.zip(standardizedModel.transform(customerVecs))

    val customerRfmVals = customerData.map(rec => rec._2) // take the vector part of (Any, Vector) tuple

    // Save orderedRFM
    orderedRFM
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "true")
      .save("ClusteringData/OrderedRFM/ordered.csv")

    (customerLabels, customerRfmVals, orderedRFM)
  }

  private def createSegments(numClusters: Int, numIterations: Int, sparkSession: SparkSession): (RDD[Any], RDD[Vector], KMeansModel) = {
    val sqlContext = sparkSession.sqlContext

    val res = createVectors(sqlContext)

    val clusterLabels = res._1
    val cust_rfm_vals = res._2
    val orderedRFM = res._3

    val clusters = KMeans.train(cust_rfm_vals, numClusters, numIterations)

    (clusterLabels, cust_rfm_vals, clusters)
  }

  def saveVectorData(sparkSession: SparkSession, kMeansModel: KMeansModel, cust_labels: RDD[Any], cust_rfm_vals: RDD[Vector]) = {
    val custClusterLabels = kMeansModel.predict(cust_rfm_vals)

    val customer_ML_Labels = cust_labels.zip(custClusterLabels)

    val cust_Clust_Labels = customer_ML_Labels.map(rec => Row(rec._1, rec._2))

    val customer_ml_clusters_df = sparkSession.sqlContext.createDataFrame(cust_Clust_Labels, utilities.customer_schema)

    customer_ml_clusters_df
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "true")
      .save("ClusteringData/Cust_Clust_Data/cust_clust_data.csv")
  }

  def createModel(mySession: SparkSession, elbowMode: Boolean) = {
    var currEff: Double = 1.0
    var newEff: Double = 0.0
    var numClusters = 1
    var kMeansModel: KMeansModel = null

    val dirPath = "ClusteringModel"
    val dir = new File(dirPath)

    if (elbowMode == true) {
      while( currEff > newEff ) {

        /*if (numClusters > 5) {
          break
        }*/

        val res = createSegments(numClusters, 20, mySession)

        val cust_lables = res._1
        val cust_rfm_vals = res._2
        kMeansModel = res._3

        // persist cust/cluster information
        saveVectorData(mySession, kMeansModel, cust_lables, cust_rfm_vals)

        newEff = kMeansModel.computeCost(cust_rfm_vals)

        if (newEff > currEff) {
          currEff = newEff
        }

        numClusters += 1
      }
    } else {
      val res = createSegments(4, 20, mySession)

      val cust_lables = res._1
      val cust_rfm_vals = res._2
      kMeansModel = res._3

      // persist cust/cluster information
      saveVectorData(mySession, kMeansModel, cust_lables, cust_rfm_vals)
    }

    // if previous model exits, delete and replace
    try {
      FileUtils.cleanDirectory(dir)
    } catch {
      case e: Exception => {
        println(s"Clean Model Folder exception due to: ${e.getMessage}")
      }
    }

    kMeansModel.save(mySession.sparkContext, dirPath)
  }
}
