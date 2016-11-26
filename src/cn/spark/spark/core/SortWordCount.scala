package cn.spark.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SortWordCount {
   def main(args: Array[String]): Unit = {
     val sparkConf = new SparkConf().setAppName("sortWordCount").setMaster("local")
     val sc = new SparkContext(sparkConf)
     
     val lines = sc.textFile("d://spark.txt", 1)
     val words = lines.flatMap { line => line.split(" ") }
     val pairs = words.map( word => (word , 1))
     val wordCount = pairs.reduceByKey(_ + _)
     
     //倒序排序
     val countword = wordCount.map(wordcount => (wordcount._2,wordcount._1))
     val sortcountword = countword.sortByKey(false, 1)
     val sortwordcount = sortcountword.map(wordcount2 => (wordcount2._2,wordcount2._1))
     sortwordcount.foreach(sortwordcount2 => println(sortwordcount2._1+"--"+sortwordcount2._2))
   }
}