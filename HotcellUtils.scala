package cse511

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  // Function to calculate cell coordinates for x, y, and z based on input data
  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int = {
    var result = 0
    coordinateOffset match {
      // X coordinate (longitude)
      case 0 => 
        result = Math.floor((inputString.split(",")(0).replace("(", "").toDouble / coordinateStep)).toInt

      // Y coordinate (latitude)
      case 1 => 
        result = Math.floor(inputString.split(",")(1).replace(")", "").toDouble / coordinateStep).toInt

      // Z coordinate (time, derived from date) - using day of the month as the Z coordinate
      case 2 => 
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Each month has 31 days for simplicity
    }
    return result
  }

  // Timestamp parser to convert date string to SQL Timestamp
  def timestampParser(timestampString: String): Timestamp = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    new Timestamp(parsedDate.getTime)
  }

  // Get the day of the month from a timestamp
  def dayOfMonth(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    calendar.get(Calendar.DAY_OF_MONTH)
  }

  // Function to calculate the G-Score for each cell
  def calculateGScore(
    sumPoints: Int, 
    numNeighbors: Int, 
    totalSum: Double, 
    numCells: Int, 
    mean: Double, 
    stdDev: Double
  ): Double = {
    val numerator = sumPoints.toDouble - (mean * numNeighbors.toDouble)
    val denominator = stdDev * Math.sqrt((numNeighbors * (numCells - numNeighbors)).toDouble / (numCells - 1))
    return numerator / denominator
  }

  // Function to calculate the mean value for G-Score
  def calculateMean(totalSum: Double, numCells: Int): Double = {
    return totalSum / numCells.toDouble
  }

  // Function to calculate the standard deviation for G-Score
  def calculateStdDev(
    totalSum: Double, 
    sumOfSquares: Double, 
    numCells: Int
  ): Double = {
    val meanSquare = totalSum * totalSum / numCells
    Math.sqrt((sumOfSquares - meanSquare) / numCells)
  }
}