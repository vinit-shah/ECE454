import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task2 {
  def numRatings(movie:String):(Long,Int) = {
      return (0.toLong,movie.split(",").zipWithIndex
                             .filter(x => x._2 != 0 && x._1 != "")
                             .length)
  }
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))
    val movies = textFile.flatMap(line => line.split("\n"))
    val output = movies.map(numRatings).reduceByKey(_ + _).map(x => x._2).repartition(1)
    output.saveAsTextFile(args(1))
  }
}
