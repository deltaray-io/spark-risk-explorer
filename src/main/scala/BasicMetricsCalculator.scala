import org.joda.time._
import org.apache.spark.sql.{DataFrame, SparkSession}

object BasicMetricsCalculator {
  def calculate(sparkSession: SparkSession, df: DataFrame): (Double, Double, Double) = {
    val dfWithReturn = RiskUtils.dfWithReturnColumn(df)

    dfWithReturn.createOrReplaceTempView("BasicMetricsCalculator")

    val dfWithDateOrder = sparkSession.sql("SELECT FIRST(date) AS first_date, FIRST(price) AS first_price, " +
                                           "LAST(date) AS last_date, LAST(price) AS last_price " +
                                           "FROM BasicMetricsCalculator ")
    val firstDate = new DateTime(dfWithDateOrder.first.getDate(0))
    val firstPrice = dfWithDateOrder.first.getDouble(1)
    val lastDate = new DateTime(dfWithDateOrder.first.getDate(2))
    val lastPrice = dfWithDateOrder.first.getDouble(3)

    val days = Days.daysBetween(firstDate, lastDate).getDays
    val years = days.toDouble / 252  // 252 trading days translates to 365 calendar days
    val cagr = if (years == 0) 0 else (math.pow(lastPrice / firstPrice, 1/years) - 1) * 100
    val returnPct = ((lastPrice - firstPrice) / firstPrice) * 100

    val dfWithPositivePeriodPct = sparkSession.sql("SELECT " +
      "(SUM(CASE WHEN return >= 0 THEN 1 ELSE 0 END) / COUNT(1)) * 100 AS positive_period_pct " +
      "FROM BasicMetricsCalculator " +
      "WHERE return IS NOT NULL")
    val positivePeriodPct = dfWithPositivePeriodPct.first.getDouble(0)

    (cagr, returnPct, positivePeriodPct)
  }
}
