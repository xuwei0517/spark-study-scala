package cn.spark.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ParquetLoadData {
  
  def main(args: Array[String]): Unit = {
     val sparkConf = new SparkConf()
             .setAppName("DataFrameOperation")
             .setMaster("local")
     val sc = new SparkContext(sparkConf)
     val sqlContext = new SQLContext(sc)
     
     val df = sqlContext.read.parquet("hdfs://192.168.80.100:9000/users.parquet")
     df.registerTempTable("users")
     
     val sqldf = sqlContext.sql("select name from users")
     val collect = sqldf.rdd.map { row => "name:"+row(0) }.collect()
     for(str <- collect){
       println(str)
     }
     
  }
  
  
}