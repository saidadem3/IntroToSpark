package sparkapp

import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType};
import scala.collection.mutable


object FirstSparkApp {
  def main(args:Array[String]): Unit ={

    val filename = "input.txt"

    val schema = StructType(Array(

      StructField("OrgId", StringType),
      StructField("LineItemId", StringType),
      StructField("SegmentId", StringType),
      StructField("SequenceId", StringType),
      StructField("Action", StringType)))


    //Create SparkConfig object and SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("First Application")

    val sc = new SparkContext(conf)

    //create RDD
    val rdd = sc.textFile(filename).
    rdd.foreach(f=>{
      var line = f.toString()
      println(line.split("|"))

    })
  }
}
