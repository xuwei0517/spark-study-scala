package cn.spark.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * 想要实现基于反射的RDD和dataframe的转换的话，必须要继承app
 * 不能使用main方法的形式来实现
 */
object RDD2DataFrameReflection extends App{
  
  val sparkConf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameReflection")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  
  // 在Scala中使用反射方式，进行RDD到DataFrame的转换，需要手动导入一个隐式转换
  import sqlContext.implicits._
  
  
  case class Student(id: Int,name: String,age: Int)
  
  val lines = sc.textFile("d://students.txt", 1)
  val studentDF = lines.map { line=> line.split(",")}
        .map { arr => Student(arr(0).trim().toInt,arr(1),arr(2).trim().toInt)}
        .toDF();
  studentDF.registerTempTable("students")
  
  val sqlDF = sqlContext.sql("select id,name,age from students where age>19")
  
  val resultsRDD = sqlDF.rdd
  
  resultsRDD.map { row => Student(row(0).toString().toInt,row(1).toString(),row(2).toString().toInt) }
            .collect()
            .foreach { stu => println(stu.id+":"+stu.name+":"+stu.age) }
  //也可以使用getAS的方式根据列名获取值
  resultsRDD.map { row => Student(row.getAs[Int]("id"),row(1).toString(),row(2).toString().toInt) }
            .collect()
            .foreach { stu => println(stu.id+":"+stu.name+":"+stu.age) }
  
}