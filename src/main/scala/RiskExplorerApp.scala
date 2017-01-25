import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object RiskExplorerApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Risk eXplorer").setMaster("local")
    val sc = new SparkContext(conf)
    println("Mutley! Do something!")
    sc.stop()
  }
}
