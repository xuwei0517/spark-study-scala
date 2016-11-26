package cn.spark.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object DataFrameOperation {
   def main(args: Array[String]): Unit = {
     val sparkConf = new SparkConf()
             .setAppName("DataFrameOperation")  
     val sc = new SparkContext(sparkConf)
     val sqlContext = new SQLContext(sc)
     val df = sqlContext.read.json("hdfs://192.168.80.100:9000/students.json")
     df.show()
     df.printSchema()
     df.select("name").show()
     df.select(df.col("name"), df.col("age").plus(1)).show()
     df.filter(df.col("age").gt(18)).show()
     df.groupBy("age").count().show()
  }
}