package cn.spark.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object LineCount {
  
   def main(args: Array[String]){
   val sparkConf = new SparkConf().setMaster("local").setAppName("LineCount")
   val sc = new SparkContext(sparkConf)
   
   val lines = sc.textFile("d:\\hello.txt", 1)
    
   val pairs = lines.map { line => (line,1) }
   
   val lineCounts = pairs.reduceByKey(_ + _)
   
   lineCounts.foreach(linecount => println(linecount._1+"--"+linecount._2))
   
  }
  
}