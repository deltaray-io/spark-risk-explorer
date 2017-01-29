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

    dfWithRollingDrawdowns.createOrReplaceTempView("DrawdownCalculation")

    val dfWithOrderedDrawndowns = sparkSession.sql("SELECT date, price, rolling_dd, rolling_max_price, " +
                                                   "(rolling_dd / rolling_max_price) as drawdown_pct " +
                                                   "FROM DrawdownCalculation ORDER BY drawdown_pct ASC")

    dfWithOrderedDrawndowns.show()

    val rollingDrawdown = dfWithOrderedDrawndowns.first().getDouble(2)
    val rollingMaxPrice = dfWithOrderedDrawndowns.first().getDouble(3)
    val maxDrawdownPct = dfWithOrderedDrawndowns.first().getDouble(4)

    maxDrawdownPct
  }
}