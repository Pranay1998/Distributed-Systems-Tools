import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    sc.textFile(args(0)).map(x => {
    	val line = x.split(',')
    	var max = '0'
    	var sb = new StringBuilder

    	for (i <- 1 until line.length) {
    		if (line(i).length() > 0) {
    			val t = line(i).charAt(0)
    			if (t > max) {
    				max = t
    				sb.setLength(0)
    				sb.append(i)
    			}
    			else if (t == max) {
    				sb.append(',')
    				sb.append(i)

    			}
    		}
    	}

    	line(0) + ',' + sb.toString()

    }).saveAsTextFile(args(1))
  }
}
