package cn.spark.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object LocalFile {
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("LocalFile").setMaster("local")
    
    val sc = new SparkContext(sparkConf);
    val lines = sc.textFile("d:\\spark.txt", 1)
    val linelength = lines.map { line => line.length() }
    val count  = linelength.reduce(_ + _)
    println("文件大小："+count)
  }
}