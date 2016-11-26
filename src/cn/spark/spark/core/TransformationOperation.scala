package cn.spark.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf

object TransformationOperation {
  
  def main(args: Array[String]): Unit={
    //map()
    //filter()
    //flatmap()
    //groupBykey()
    //reducebykey()
    //sortBykey()
    //join()
    cogroup()
  }
  
  
  def cogroup(){
    val sparkConf = new SparkConf().setAppName("sortByKey").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val studnetTupleList = Array(Tuple2(1,"zs"),Tuple2(2,"ls"),Tuple2(3,"ww"),Tuple2(4,"zg"))
    val scoreTupleList = Array(Tuple2(1,80),Tuple2(2,59),Tuple2(3,65),Tuple2(4,100),Tuple2(1,801),Tuple2(2,591),Tuple2(3,651),Tuple2(4,1001))
    
    val studnetTupleRDD = sc.parallelize(studnetTupleList,1);
    val scoreTupleRDD = sc.parallelize(scoreTupleList, 1);
    val joinRDD = studnetTupleRDD.cogroup(scoreTupleRDD);
    joinRDD.foreach(stu_score=>println(stu_score._1+"--{"+stu_score._2._1+"--"+stu_score._2._2+"}"))
  }
  
  def join(){
    val sparkConf = new SparkConf().setAppName("sortByKey").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val studnetTupleList = Array(Tuple2(1,"zs"),Tuple2(2,"ls"),Tuple2(3,"ww"),Tuple2(4,"zg"))
    val scoreTupleList = Array(Tuple2(1,80),Tuple2(2,59),Tuple2(3,65),Tuple2(4,100))
    
    val studnetTupleRDD = sc.parallelize(studnetTupleList,1);
    val scoreTupleRDD = sc.parallelize(scoreTupleList, 1);
    val joinRDD = studnetTupleRDD.join(scoreTupleRDD);
    joinRDD.foreach(stu_score=>println(stu_score._1+"--{"+stu_score._2._1+"--"+stu_score._2._2+"}"))
  }
  
  def sortBykey(){
    val sparkConf = new SparkConf().setAppName("sortByKey").setMaster("local")
    val sc = new SparkContext(sparkConf)
    
    val tupleList = Array(Tuple2(80,"zs"),Tuple2(59,"ls"),Tuple2(65,"ww"),Tuple2(100,"zg"))
    val tupleListRDD = sc.parallelize(tupleList, 2);
    val sortBykeyRDD = tupleListRDD.sortByKey(true, 1);//第一个参数是控制按照什么顺序排序，true为正序，false为倒序
    sortBykeyRDD.foreach(student=>println(student._1+"--"+student._2))
  }
  
  def reducebykey(){
    val sparkConf = new SparkConf().setAppName("groupBykey").setMaster("local")
    val sc = new SparkContext(sparkConf);
    
    val tupleList = Array(Tuple2("class1",80),Tuple2("class2",75),Tuple2("class1",90),Tuple2("class2",65))
    
    val tupleListRDD = sc.parallelize(tupleList, 2)
    
    val reduceByKeyRDD = tupleListRDD.reduceByKey(_ + _);
    
    reduceByKeyRDD.foreach(scoreclass=>println(scoreclass._1+"--"+scoreclass._2))
  }
  
  def groupBykey(){
    val sparkConf = new SparkConf().setAppName("groupBykey").setMaster("local")
    val sc = new SparkContext(sparkConf);
    
    val tupleList = Array(Tuple2("class1",80),Tuple2("class2",75),Tuple2("class1",90),Tuple2("class2",65))
    
    val tupleListRDD = sc.parallelize(tupleList, 2)
    val groupBykeyRDD = tupleListRDD.groupByKey()
    groupBykeyRDD.foreach(score => {
       println(score._1)
       score._2.foreach { singscore => print(singscore+" ") }
       println()
    })
    
    
  }
  
  
  def flatmap(){
    val sparkConf = new SparkConf().setAppName("flatmap").setMaster("local")
    val sc = new SparkContext(sparkConf);
    
    val lines = Array("hello you","hello me")
    val linesRDD = sc.parallelize(lines, 2)
    val flatMapRDD = linesRDD.flatMap { line => line.split(" ") }
    flatMapRDD.foreach { line => println(line) }
  }
  
  def filter(){
    val sparkConf = new SparkConf().setAppName("filter").setMaster("local")
    val sc = new SparkContext(sparkConf);
    
    val nums = Array(1,2,3,4,5,6,7,8,9,10)
    val numsRDD = sc.parallelize(nums, 2)
    
    val filterRDD = numsRDD.filter { num => num%2==0 }
    filterRDD.foreach { num => println(num) }
  }
  
  /**
   * map操作
   */
  def map(): Unit = {
    val sparkConf = new SparkConf().setAppName("map").setMaster("local")
    
    val sc = new SparkContext(sparkConf)
    
    val nums = Array(1,2,3,4,5)
    val numsRDD = sc.parallelize(nums, 1)
    
    val mapRDD = numsRDD.map { num => num *2 }
    
    mapRDD.foreach { map_num => println(map_num) }
  }
  
}