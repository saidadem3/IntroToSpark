package sparkapp

import org.apache.spark.{SparkConf, SparkContext}

object FirstSparkApp {
  def main(args:Array[String]): Unit ={
    //Create SparkConfig object and SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("First Application")

    val sc = new SparkContext(conf)

    //create RDD
    val rdd1 = sc.makeRDD(Array(1,2,3,4,5,6))
    rdd1.collect.foreach(println)
  }
}
