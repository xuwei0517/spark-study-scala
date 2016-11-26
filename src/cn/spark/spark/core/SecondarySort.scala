package cn.spark.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SecondarySort {
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("secondarysort").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile("d://sort.txt", 1)
    val pairRDD = lines.map { line => {val splits = line.split(" ");(new SecondarySortKey(splits(0).toInt,splits(1).toInt),line)} }
    val sortpairRDD = pairRDD.sortByKey()
    val sortedRDD = sortpairRDD.map(tuple2 => tuple2._2)
    sortedRDD.foreach { line => println(line) }
  }
  
}