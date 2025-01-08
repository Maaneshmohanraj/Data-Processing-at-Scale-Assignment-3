package cse511

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read
      .format("csv")
      .option("delimiter", ";")
      .option("header", "false")  // Ensure this matches your data format
      .load(pointPath)

    // Create temp view to use SQL queries on this DataFrame
    pickupInfo.createOrReplaceTempView("nyctaxitrips")

    // Register UDFs to calculate cell coordinates (x, y, z)
    spark.udf.register("CalculateX", (pickupPoint: String) => HotcellUtils.CalculateCoordinate(pickupPoint, 0))
    spark.udf.register("CalculateY", (pickupPoint: String) => HotcellUtils.CalculateCoordinate(pickupPoint, 1))
    spark.udf.register("CalculateZ", (pickupTime: String) => HotcellUtils.CalculateCoordinate(pickupTime, 2))

    // Calculate coordinates for each trip based on pickup points (longitude, latitude) and pickup time
    pickupInfo = spark.sql(
      """
        |SELECT 
        |  CalculateX(nyctaxitrips._c5) AS x,
        |  CalculateY(nyctaxitrips._c5) AS y,
        |  CalculateZ(nyctaxitrips._c1) AS z
        |FROM nyctaxitrips
      """.stripMargin)

    // Define the min and max of x, y, z (the grid bounds)
    val minX = math.floor(-74.50 / HotcellUtils.coordinateStep).toInt
    val maxX = math.ceil(-73.70 / HotcellUtils.coordinateStep).toInt
    val minY = math.floor(40.50 / HotcellUtils.coordinateStep).toInt
    val maxY = math.ceil(40.90 / HotcellUtils.coordinateStep).toInt
    val minZ = 1  // first day of the month
    val maxZ = 31 // last day of the month
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    // Step 1: Aggregate the points in each (x, y, z) cell.
    pickupInfo.createOrReplaceTempView("pickupCoordinates")
    val aggregatedPoints = spark.sql(
      s"""
         |SELECT x, y, z, COUNT(*) AS pointCount
         |FROM pickupCoordinates
         |WHERE x >= $minX AND x <= $maxX
         |AND y >= $minY AND y <= $maxY
         |AND z >= $minZ AND z <= $maxZ
         |GROUP BY x, y, z
       """.stripMargin)

    aggregatedPoints.createOrReplaceTempView("aggregatedPoints")

    // Step 2: Calculate neighborhood sum for each cell.
    val neighborhoodSum = spark.sql(
      s"""
         |SELECT p1.x AS x, p1.y AS y, p1.z AS z,
         |       SUM(p2.pointCount) AS sumPoints, COUNT(p2.pointCount) AS numNeighbors
         |FROM aggregatedPoints p1, aggregatedPoints p2
         |WHERE (p2.x BETWEEN p1.x-1 AND p1.x+1)
         |AND (p2.y BETWEEN p1.y-1 AND p1.y+1)
         |AND (p2.z BETWEEN p1.z-1 AND p1.z+1)
         |GROUP BY p1.x, p1.y, p1.z
       """.stripMargin)

    neighborhoodSum.createOrReplaceTempView("neighborhoodSum")

    // Step 3: Calculate G-score for each cell.
    val totalSum = aggregatedPoints.agg(sum("pointCount")).first().getLong(0).toDouble
    val mean = totalSum / numCells
    val sumOfSquares = aggregatedPoints.select(pow(col("pointCount"), 2)).rdd.map(r => r.getDouble(0)).sum
    val stdDev = HotcellUtils.calculateStdDev(totalSum, sumOfSquares, numCells)

    val gScoreDF = spark.sql(
      s"""
         |SELECT x, y, z,
         |      (sumPoints - ($mean * numNeighbors)) / 
         |      ($stdDev * SQRT((numNeighbors * ($numCells - numNeighbors)) / ($numCells - 1))) AS gScore
         |FROM neighborhoodSum
         |ORDER BY gScore DESC
       """.stripMargin)

    // Step 4: Select the top 50 hottest cells (highest G-score).
    val result = gScoreDF.select("x", "y", "z").limit(50)
    result.show()

    return result
  }
}