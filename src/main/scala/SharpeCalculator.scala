import org.apache.spark.sql.{DataFrame, SparkSession}

object SharpeCalculator {
  def calculate(sparkSession: SparkSession, df: DataFrame, riskFreeRate: Double = 0): Double = {
    val dfWithReturn = RiskUtils.dfWithReturnColumn(df)

    dfWithReturn.createOrReplaceTempView("SharpeCalculation")

    val avgExcessReturnDf = sparkSession.sql("SELECT " +
                                             s"AVG(return - $riskFreeRate) / STDDEV(return) AS sharpe " +
                                             "FROM SharpeCalculation " +
                                             "WHERE return IS NOT NULL")

    val sharpeRatio = avgExcessReturnDf.first.getDouble(0)
    sharpeRatio
  }
}