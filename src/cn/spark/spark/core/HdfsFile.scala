package cn.spark.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object HdfsFile {
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("HdfsFile")
    
    val sc = new SparkContext(sparkConf);
    val lines = sc.textFile("hdfs://192.168.80.100:9000/spark.txt", 1)
    val linelength = lines.map { line => line.length() }
    val count  = linelength.reduce(_ + _)
    println("文件大小："+count)
  }
}