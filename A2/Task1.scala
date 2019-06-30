import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task1 {

  def topRatings(movie:String):String = {
      val ratings = movie.split(",").zipWithIndex.map(x => (x._2, x._1))
      val title = ratings.filter(x => x._1 == 0)
      val userRatings = ratings.filter(x=> x._1 > 0).map(x => if(x._2 == "") (x._1,0.toLong) else (x._1,x._2.toLong))
      val max = userRatings.maxBy(_._2)._2
      val maxes = userRatings.filter{x => x._2 == max}
      val ret = title.map(x => x._2) ++ maxes.map(x => x._1.toString)
      return ret.mkString(",")
  }
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))
    val movies = textFile.flatMap(line => line.split("\n"))
    val output = movies.map(topRatings)
    output.saveAsTextFile(args(1))
  }
}
