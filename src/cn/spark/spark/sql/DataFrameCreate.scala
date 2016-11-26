package cn.spark.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object DataFrameCreate {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DataFrameCreate")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    
    val df = sqlContext.read.json("hdfs://192.168.80.100:9000/students.json")
    df.show()
  }
}