import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task3 {
  def numRatings(movie:String):Array[(Long,Long)] = {
      return movie.split(",")
                  .zipWithIndex.filter(x => x._2 != 0)
                  .map(x => if (x._1 == "") (x._2.toLong,0.toLong) else (x._2.toLong, 1.toLong))
  }
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))
    val movies = textFile.flatMap(line => line.split("\n"))
    val output = movies.map(numRatings).flatMap(_.toList).reduceByKey(_ + _)
    output.saveAsTextFile(args(1))
  }
}
