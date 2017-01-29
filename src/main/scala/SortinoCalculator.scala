import org.apache.spark.sql.{DataFrame, SparkSession}

object SortinoCalculator {
  def calculate(sparkSession: SparkSession, df: DataFrame, targetReturn: Double = 0): Double = {
    val dfWithReturn = RiskUtils.dfWithReturnColumn(df)
    val dfWithExcessReturn = dfWithReturn.withColumn("excess_return", dfWithReturn("return") - targetReturn)

    dfWithExcessReturn.createOrReplaceTempView("SortinoCalculation")

    val avgReturnDf = sparkSession.sql("SELECT AVG(return) FROM SortinoCalculation")
    val avgReturn = avgReturnDf.first.getDouble(0)

    val targetDownsideDeviationDf = sparkSession.sql("SELECT SQRT(AVG(POW(" +
                                                     " (CASE WHEN excess_return < 0 THEN excess_return ELSE 0 END), " +
                                                     "  2))) AS target_downside_deviation " +
                                                     "FROM SortinoCalculation " +
                                                     "WHERE excess_return IS NOT NULL" )
    val targetDownsideDeviation = targetDownsideDeviationDf.first.getDouble(0)


    val sortinoRatio = (avgReturn - targetReturn) / targetDownsideDeviation

    sortinoRatio
  }
}
