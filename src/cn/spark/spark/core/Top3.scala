package cn.spark.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Top3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("top3").setMaster("local")
    val sc = new SparkContext(sparkConf)
    
    val lines = sc.textFile("d://top3.txt", 1)
    val pairRDD = lines.map { line => (line.toInt,line) }
    val sortKeyRDD = pairRDD.sortByKey(false, 2)
    val sortedRDD = sortKeyRDD.map(sortline => sortline._2)
    val top3num = sortedRDD.take(3);
    for(tnum <- top3num){
      println(tnum)
    }
  }
}