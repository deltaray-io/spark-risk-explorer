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

}

