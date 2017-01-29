import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.max

object DrawdownCalculator {
  def calculate(sparkSession: SparkSession, df: DataFrame): Double = {

    val windowUptoCurrentRow = Window.orderBy("date").rowsBetween(Long.MinValue, 0)
    val dfWithRollingMaxPrice = df.withColumn("rolling_max_price",
                                              max(df("price")).over(windowUptoCurrentRow))

    val dfWithRollingDrawdowns = dfWithRollingMaxPrice.withColumn("rolling_dd",
      max(dfWithRollingMaxPrice("rolling_max_price") - dfWithRollingMaxPrice("price")).over(windowUptoCurrentRow))
    dfWithRollingDrawdowns.show()

    val maxDrawdown = dfWithRollingDrawdowns.agg(max("rolling_dd")).collect(){0}.getDouble(0)

    maxDrawdown
  }
}