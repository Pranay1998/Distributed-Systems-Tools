import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)

    val count = sc.textFile(args(0)).flatMap(x => x.split(',').drop(1))
    	.filter(_.length() > 0)
    	.count() 

	sc.parallelize(Seq(count)).coalesce(1, true).saveAsTextFile(args(1))
  }
}
