package cn.spark.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
object DailyUV {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DailyUV").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    
    import sqlContext.implicits._
    
    
    val log_data = Array(
        "2016-10-27,1011",
        "2016-10-27,1012",
        "2016-10-27,1012",
        "2016-10-27,1013",
        "2016-10-28,1010",
        "2016-10-28,1011",
        "2016-10-28,1013",
        "2016-10-28,1013"
        )
        
    val logRDD = sc.parallelize(log_data, 3)
    val accessLogRDD = logRDD.map { line => Row(line.split(",")(0),line.split(",")(1).toInt) }
    
    
    val structType = StructType(Array(StructField("date",StringType,true),StructField("userid",IntegerType,true)))
    val logDF = sqlContext.createDataFrame(accessLogRDD, structType)
    
    logDF.groupBy("date")
      .agg('date, countDistinct('userid))
      //为什么包含三列呢？
      .map { row => Row(row(0),row(1),row(2)) }
      .collect()
      .foreach { println}
    
  }
}