package cn.spark.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ActionOperation {
  
  def main(args: Array[String]): Unit = {
    //reduce()
    //collect()
    //count()
    //take()
    countBykey();
  }
  
  def countBykey(){
    val sparkConf = new SparkConf().setAppName("saveAsTextFile").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val tupleList = Array(Tuple2("class1","zs"),Tuple2("class1","zs"),Tuple2("class2","zs"),Tuple2("class2","zs"),Tuple2("class2","zs"))
    val tupleListRDD = sc.parallelize(tupleList, 2)
    val countBykeyRDD = tupleListRDD.countByKey()
    for(entry1 <- countBykeyRDD){
      println(entry1._1+"--"+entry1._2)
    }
    
  }
  
  
  def saveAsTextFile(){
    val sparkConf = new SparkConf().setAppName("saveAsTextFile")
    val sc = new SparkContext(sparkConf)
    
    val numberList = Array(1,2,3,4,5,6,7,8,9,10);
    val numberListRDD = sc.parallelize(numberList, 2);
    val mapRDD = numberListRDD.map { num => num * 2 }
    mapRDD.saveAsTextFile("hdfs:hadoop100:9000/result")
  }
  
  
  
  def take(){
    val sparkConf = new SparkConf().setAppName("take").setMaster("local")
    val sc = new SparkContext(sparkConf)
    
    val numberList = Array(1,2,3,4,5,6,7,8,9,10);
    val numberListRDD = sc.parallelize(numberList, 2);
    val mapRDD = numberListRDD.map { num => num * 2 }
    val top3 = mapRDD.take(3)
    for(num <- top3){
      print(num+" ")
    }
  }
  
  
  def count(){
    val sparkConf = new SparkConf().setAppName("reduce").setMaster("local")
    val sc = new SparkContext(sparkConf)
    
    val numberList = Array(1,2,3,4,5,6,7,8,9,10);
    val numberListRDD = sc.parallelize(numberList, 2);
    val mapRDD = numberListRDD.map { num => num * 2 }
    val count = mapRDD.count();
    println("总数："+count)
  }
  
  def collect(){
    val sparkConf = new SparkConf().setAppName("reduce").setMaster("local")
    val sc = new SparkContext(sparkConf)
    
    val numberList = Array(1,2,3,4,5,6,7,8,9,10);
    val numberListRDD = sc.parallelize(numberList, 2);
    val mapRDD = numberListRDD.map { num => num * 2 }
    val collect = mapRDD.collect();
    for(num <- collect){
      print(num+" ")
    }
  }
  
  def reduce(){
    val sparkConf = new SparkConf().setAppName("reduce").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val numberList = Array(1,2,3,4,5,6,7,8,9,10)
    val numberListRDD = sc.parallelize(numberList, 2)
    
    val reduceRDD = numberListRDD.reduce(_ + _)
    println("和为："+reduceRDD)
  }
  
}