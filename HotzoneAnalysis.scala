package cse511

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object HotzoneAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotZoneAnalysis(spark: SparkSession, pointPath: String, rectanglePath: String): DataFrame = {
    // Load point data
    val pointDf = spark.read
      .format("csv")
      .option("delimiter", ";")
      .option("header", "false")
      .load(pointPath)

    pointDf.createOrReplaceTempView("point")

    // Trim point data formats
    spark.udf.register("trim", (string: String) => string.replace("(", "").replace(")", ""))
    val trimmedPointDf = spark.sql("SELECT trim(_c5) AS _c5 FROM point")
    trimmedPointDf.createOrReplaceTempView("point")

    // Load rectangle data
    val rectangleDf = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(rectanglePath)

    rectangleDf.createOrReplaceTempView("rectangle")

    // Join datasets to find points within rectangles
    spark.udf.register("ST_Contains", (queryRectangle: String, pointString: String) => HotzoneUtils.ST_Contains(queryRectangle, pointString))
    val joinDf = spark.sql(
      """
        |SELECT rectangle._c0 AS rectangle, point._c5 AS point
        |FROM rectangle, point
        |WHERE ST_Contains(rectangle._c0, point._c5)
      """.stripMargin)

    joinDf.createOrReplaceTempView("joinResult")

    // Count points per rectangle and return result
    val pointsCountSortedByRectangleDf = spark.sql(
      """
        |SELECT rectangle, COUNT(point) AS numberOfPoints
        |FROM joinResult
        |GROUP BY rectangle
        |ORDER BY rectangle ASC
      """.stripMargin).persist().coalesce(1)

    pointsCountSortedByRectangleDf.createOrReplaceTempView("HotZoneResult")
    pointsCountSortedByRectangleDf.show()

    pointsCountSortedByRectangleDf
  }
}