package cn.spark.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode

/**
 * parquet数据源之元数据合并
 */
object ParquetMergeSchema {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DataFrameOperation")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    //导入隐式转换
    import sqlContext.implicits._
    //创建第一个dataframe 学生的基本信息，并写入parquet文件中
    val studentsWithAge = Array(("zs",20),("ls",30))
    val studentsWithAgeDf = sc.parallelize(studentsWithAge, 2).toDF("name","age")
    //指定保存方式为append，保证多次写入同一目录不失败
    studentsWithAgeDf.save("hdfs://192.168.80.100:9000/students", "parquet", SaveMode.Append);
    
    val studentsWithGrade = Array(("zs","A"),("ls","B"))
    val studentsWithGradeDf = sc.parallelize(studentsWithGrade, 2).toDF("name","grade")
    //指定保存方式为append，保证多次写入同一目录不失败
    studentsWithGradeDf.save("hdfs://192.168.80.100:9000/students", "parquet", SaveMode.Append);
    
    //spark默认没有开启元数据合并功能，需要设置mergeSchema开启
    val studf = sqlContext.read.option("mergeSchema", "true").parquet("hdfs://192.168.80.100:9000/students")
    studf.printSchema()
    studf.show()
  }
  
}