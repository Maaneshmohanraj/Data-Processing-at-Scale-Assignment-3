package cse511

object HotzoneUtils {
  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    // Validate input data
    if (Option(queryRectangle).isEmpty || queryRectangle.isEmpty || Option(pointString).isEmpty || pointString.isEmpty) {
      return false
    }

    val rectangleCoordinates = queryRectangle.split(",").map(_.trim)
    val pointCoordinates = pointString.split(",").map(_.trim)

    // Check for valid number of coordinates
    if (rectangleCoordinates.length < 4 || pointCoordinates.length < 2) {
      return false
    }

    val (x1, y1, x2, y2) = (rectangleCoordinates(0).toDouble, rectangleCoordinates(1).toDouble, rectangleCoordinates(2).toDouble, rectangleCoordinates(3).toDouble)
    val (pointX, pointY) = (pointCoordinates(0).toDouble, pointCoordinates(1).toDouble)

    // Check if the rectangle contains the point
    pointX >= Math.min(x1, x2) && pointX <= Math.max(x1, x2) && pointY >= Math.min(y1, y2) && pointY <= Math.max(y1, y2)
  }
}