import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task4 {
  def computeSimilarities(pair:(String,String)):String = {
      val movies = pair._1.split(",") zip pair._2.split(",")
      val similarity = movies.filter(x => (x._1 == x._2) && (x._1 != "")).length
      return List(movies(0)._1, movies(0)._2, similarity).mkString(",")
  }
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))
    val movies = sc.broadcast(textFile.flatMap(line => line.split("\n")).collect())
    val pairs = movies.value.combinations(2).toList.map(x => if(x(0) < x(1)) (x(0), x(1)) else (x(1), x(0)))
    // modify this code
    val output = sc.parallelize(pairs.map(computeSimilarities))
    output.saveAsTextFile(args(1))
  }
}
