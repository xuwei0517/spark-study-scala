package cn.spark.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType

object JSONDataSource {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("JSONDataSource")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    
    val studentScoreDF = sqlContext.read.json("hdfs://192.168.80.100:9000/students.json")
    studentScoreDF.registerTempTable("student_scores")
    val goodStudentScoreSqlDF = sqlContext.sql("select name,score from student_scores where score > 60")
    val studentNamesList = goodStudentScoreSqlDF.rdd.map { stuscore => stuscore.getAs[String]("name") }.collect()
    
    
    val studentInfoJson = Array("{\"name\":\"zs\",\"age\":18}","{\"name\":\"ls\",\"age\":28}","{\"name\":\"ww\",\"age\":38}")
    val studentInfoJsonRDD = sc.parallelize(studentInfoJson, 2)
    val studentInfoJsonDF = sqlContext.read.json(studentInfoJsonRDD)
    studentInfoJsonDF.registerTempTable("student_ages")
    var sqlText = "select name,age from student_ages where name in (";
    for(i<- 0 until studentNamesList.length){
      if(i==0){
        sqlText += "'"+studentNamesList(i)+"'"
      }else{
        sqlText += ",'"+studentNamesList(i)+"'"
      }
    }
    sqlText += ")"
    val goodstudentInfoDF = sqlContext.sql(sqlText)
    
    val joinRDD = goodStudentScoreSqlDF.rdd.map { stuscore => (Tuple2(stuscore.getAs[String]("name"),stuscore.getAs[Long]("score").toString().toInt)) }
        .join(goodstudentInfoDF.map { stuinfo => (Tuple2(stuinfo.getAs[String]("name"),stuinfo.getAs[Long]("age").toString().toInt)) })
    
    val rowRDD = joinRDD.map(info => Row(info._1,info._2._1,info._2._2))
    
    val structFieldArr = Array(StructField("name",StringType,true),StructField("score",IntegerType,true),StructField("age",IntegerType,true))
    val strctType = StructType(structFieldArr)
    val goodstudentInfo = sqlContext.createDataFrame(rowRDD, strctType)
    goodstudentInfo.write.json("hdfs://192.168.80.100:9000/good-student-scala")
  }
}