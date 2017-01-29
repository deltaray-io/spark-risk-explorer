import java.sql.{Date, Timestamp}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest._

class RiskExplorerSpec extends FlatSpec with BeforeAndAfter with GivenWhenThen with Matchers {
  val spark = SparkSession
    .builder()
    .appName("RiskExplorerSpec")
    .master("local")
    .getOrCreate()

  "Risk Explorer" should "calculate Sharpe ratio" in {
    val df = spark.createDataFrame(Seq(Quote(Date.valueOf("2016-01-25"), 100.0),
                                       Quote(Date.valueOf("2017-01-26"), 100.0*1.1),
                                       Quote(Date.valueOf("2017-01-27"), 100.0*1.1*1.05),
                                       Quote(Date.valueOf("2017-01-28"), 100.0*1.1*1.05*1.13),
                                       Quote(Date.valueOf("2017-01-29"), 100.0*1.1*1.05*1.13*0.93),
                                       Quote(Date.valueOf("2017-01-30"), 100.0*1.1*1.05*1.13*0.93*1.22)))

    When("Calculate Sharpe")
    val sharpeRatio = SharpeCalculator.calculate(spark, df)

    Then("Sharpe is calculated")
    BigDecimal(sharpeRatio).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble should equal(0.8044)

  }

  "Risk Explorer" should "calculate Sortino ratio" in {
    Given("a set of quotes")
    // Based on Red Rock's Sortino Calculation Example:
    // http://www.redrockcapital.com/Sortino__A__Sharper__Ratio_Red_Rock_Capital.pdf
    val df = spark.createDataFrame(Seq(Quote(Date.valueOf("2009-01-21"), 100.0),
                                       Quote(Date.valueOf("2010-01-21"), 100.0*1.17),
                                       Quote(Date.valueOf("2011-01-21"), 100.0*1.17*1.15),
                                       Quote(Date.valueOf("2012-01-21"), 100.0*1.17*1.15*1.23),
                                       Quote(Date.valueOf("2013-01-21"), 100.0*1.17*1.15*1.23*0.95),
                                       Quote(Date.valueOf("2014-01-21"), 100.0*1.17*1.15*1.23*0.95*1.12),
                                       Quote(Date.valueOf("2015-01-21"), 100.0*1.17*1.15*1.23*0.95*1.12*1.09),
                                       Quote(Date.valueOf("2016-01-21"), 100.0*1.17*1.15*1.23*0.95*1.12*1.09*1.13),
                                       Quote(Date.valueOf("2017-01-21"), 100.0*1.17*1.15*1.23*0.95*1.12*1.09*1.13*0.96)
    ))


    When("Calculate Sortino")
    val sortinoRatio = SortinoCalculator.calculate(spark, df)

    Then("Sortino is calculated")
    BigDecimal(sortinoRatio).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble should equal(4.417)
  }

  "Risk Explorer" should "calculate max drawdown" in {
    val df = spark.createDataFrame(Seq(Quote(Date.valueOf("2016-01-25"), 100.0),
      Quote(Date.valueOf("2017-01-26"), 500),
      Quote(Date.valueOf("2017-01-27"), 750),
      Quote(Date.valueOf("2017-01-28"), 400),
      Quote(Date.valueOf("2017-01-29"), 600),
      Quote(Date.valueOf("2017-01-30"), 350),
      Quote(Date.valueOf("2017-01-31"), 800),
      Quote(Date.valueOf("2017-02-01"), 600),
      Quote(Date.valueOf("2017-02-02"), 100)))

    When("Calculate drawdown")
    val maxDrawdown = DrawdownCalculator.calculate(spark, df)

    Then("Drawdown is calculated")
    maxDrawdown should equal(700)

  }


}

