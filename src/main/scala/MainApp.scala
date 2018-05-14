import Utilities.Utilities
import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.clustering.KMeansModel

case class Description(description: String, count: Long)

class MainApp {

  val utilities = new Utilities

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

  def createSegmentData(sparkSession: SparkSession, dirPath: String): (DataFrame, DataFrame) = {
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    val kmeansModel = KMeansModel.load(sparkContext, dirPath)

    // Load Cust/Cluster Data
    val customer_ml_clusters_df = utilities.readFile("csv", sqlContext, "ClusteringData/Cust_Clust_Data/cust_clust_data.csv")
    val orderedRFM = utilities.readFile("csv", sqlContext,"ClusteringData/OrderedRFM/ordered.csv")

    val finalClusterData = orderedRFM.join(customer_ml_clusters_df, Seq("CustomerID"))

    val cluster_centers = finalClusterData
      .groupBy("ClusterLabel")
      .agg(
        mean(finalClusterData.col("Recency")),
        mean(finalClusterData.col("Frequency")),
        mean(finalClusterData.col("Monetary"))
      )

    (finalClusterData, cluster_centers)
  }

}

object MainApp extends MainApp {

  val mySession = createSparkSession("local[*]", "Vicki")

  def main(args: Array[String]): Unit = {
    // check if clustering model exists. if not, create it
    val dirPath = "ClusteringModel"
    val dir = new File(dirPath)

    val forced = false
    val elbowMode = false

    if (dir.list().length < 0 || forced == true) {
      val clusterBuilder = new CustomerSegmentation()

      clusterBuilder.createModel(mySession, elbowMode)
    }

    val rawData = utilities.readFile("csv", mySession.sqlContext, "ClusteringData/RawData/retail.csv")

    val segmentData = createSegmentData(mySession, dirPath)

    val customerData = segmentData._1

    val numClusters = 4
    val clusterData = segmentData._2

    val valuedClusterData = clusterData.withColumn("Total_Value", clusterData.col("avg(Recency)") * clusterData.col("avg(Frequency)") * clusterData.col("avg(Monetary)"))

    valuedClusterData.createTempView("Valued_Cluster_Data")

    val highestValueClusterArray = mySession.sql("SELECT ClusterLabel FROM Valued_Cluster_Data WHERE Total_Value = (SELECT MAX(Total_Value) FROM Valued_Cluster_Data)").collect().map { row =>
      row.getInt(0)
    }

    val highestValueCluster = highestValueClusterArray(0)

    val rawDataWithClusters = rawData.join(customerData, Seq("CustomerID"))
    rawDataWithClusters.createTempView("RAW_CLUSTER_DATA")

    val mostCommonProducts = mySession.sql(s"SELECT Description, COUNT(Description) FROM RAW_CLUSTER_DATA WHERE ClusterLabel = $highestValueCluster GROUP BY Description ORDER BY COUNT(Description) DESC").take(1).map { row =>
      Description(row.getString(0), row.getLong(1))
    }

    val product = mostCommonProducts(0).description

    println(s"Product to recommend to Cluster $highestValueCluster customers in neighboring countries: $product")

  }
}
