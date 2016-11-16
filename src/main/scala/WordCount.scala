import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shinpei.kamiguchi on 2016/11/15.
  */
object WordCount {
  def main(args : Array[String]) {
    val input = "hdfs://quickstart.cloudera/user/cloudera/input"
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    val file = sc.textFile(input)
    val counts = file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _, 1)
    counts.saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/spark_scala.output")
  }

}
