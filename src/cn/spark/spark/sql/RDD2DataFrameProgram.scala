package cn.spark.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType

object RDD2DataFrameProgram extends App {
  val sparkConf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameReflection")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  
  val lines = sc.textFile("d://students.txt", 1)
  val splitRDD = lines.map { line => line.split(",")}
  val rowRDD = splitRDD.map { arr => Row(arr(0).trim().toInt,arr(1).toString(),arr(2).trim().toInt)}
  
  val structType = StructType(Array(
    StructField("id",IntegerType,true),
    StructField("name",StringType,true),
    StructField("age",IntegerType,true)
  ))
  val df = sqlContext.createDataFrame(rowRDD, structType)
  df.registerTempTable("students")
  
  val sqlDF = sqlContext.sql("select id,name,age from students where age > 18")
  sqlDF.rdd.collect().foreach { row => println(row) }
  
  
  
}