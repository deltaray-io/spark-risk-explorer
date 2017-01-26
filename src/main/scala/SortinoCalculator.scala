import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lag

object SortinoCalculator {

  def calculate(sparkSession: SparkSession, df: DataFrame, targetReturn: Double = 0): Double = {
    val partition = Window.orderBy("date")
    val dsWithLagPrice = df.withColumn("prev_price", lag(df("price"), 1).over(partition))
    val dsWithReturn = dsWithLagPrice.withColumn("return", (dsWithLagPrice("price") - dsWithLagPrice("prev_price")) /
                                                            dsWithLagPrice("prev_price"))
    val dsWithExcessReturn = dsWithReturn.withColumn("excess_return", dsWithReturn("return") - targetReturn)

    dsWithExcessReturn.createOrReplaceTempView("SortinoCalculation")

    val avgReturnDf = sparkSession.sql("SELECT AVG(return) FROM SortinoCalculation")
    val avgReturn = avgReturnDf.first.getDouble(0)

    val targetDownsideDeviationDf = sparkSession.sql("SELECT SQRT(AVG(POW(" +
                                                     " (CASE WHEN excess_return < 0 THEN excess_return ELSE 0 END), " +
                                                     "  2))) AS target_downside_deviation " +
                                                     "FROM SortinoCalculation " +
                                                     "WHERE excess_return IS NOT NULL" )
    val targetDownsideDeviation = targetDownsideDeviationDf.first.getDouble(0)


    val sortino = (avgReturn - targetReturn) / targetDownsideDeviation

    sortino
  }

}
