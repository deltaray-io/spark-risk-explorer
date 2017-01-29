import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.lag

object RiskUtils {
  def dfWithReturnColumn(df: DataFrame) : DataFrame = {
    val partition = Window.orderBy("date")
    val dfWithLagPrice = df.withColumn("prev_price", lag(df("price"), 1).over(partition))
    val dfWithReturn = dfWithLagPrice.withColumn("return", (dfWithLagPrice("price") - dfWithLagPrice("prev_price")) /
                                                            dfWithLagPrice("prev_price"))

    dfWithReturn
  }
}
