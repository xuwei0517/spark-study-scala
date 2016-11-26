package cn.spark.spark.core

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object WordCount {
  
     def main(args: Array[String]): Unit = {
       val conf = new SparkConf()
       conf.setAppName("wordCount")
       conf.setMaster("local")
       
       val sc = new SparkContext(conf)
       val lines = sc.textFile("d:\\spark.txt", 1)
       val words = lines.flatMap { line => line.split(" ")}
       val pairs = words.map { word => (word,1) }
       val wordCounts = pairs.reduceByKey{_ + _}
       wordCounts.foreach(wordCount => println(wordCount._1 +"----"+wordCount._2))
     }
}