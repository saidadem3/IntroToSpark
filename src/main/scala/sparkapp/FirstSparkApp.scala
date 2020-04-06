package sparkapp

import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source
import collection.mutable


object FirstSparkApp {
  def main(args:Array[String]): Unit ={

    val filename = "input.txt"


    //Create SparkConfig object and SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("First Application")

    val sc = new SparkContext(conf)

    //create RDD
    val rdd = sc.textFile(filename)
    val fileContents = Source.fromFile(filename).getLines.toArray
    val newCont = fileContents.map(x => Array(x.split("|")))

    /*
    Attempts:
    - Tried create a hashmap of the elements in my array
    - When I applied the split method on the strings of each element I would get a return
      of each individual character rather than the words separated by the delimiter
    - If the split worked the way I hoped it would the plan was to use a multimap to get the
      first element and have a guard statement (if Array1[0] == Array2[0]) then map the key Array[0]
      with the values of Array1[1], Array2[1]
    - Do this again for the third ID mapping Array[0] with Array1[2], Array2[2]
    - Finally use flatMap to join the 2 Array and create a new string with ID1|ID2|ID3 with ID2 and ID3
      being a comma separated string

      Day 4: task incomplete

     */
    fileContents.foreach(k => {
      val line = k.split("|")
      for(v <- line){
        println(v)
      }
    })


  }
}
