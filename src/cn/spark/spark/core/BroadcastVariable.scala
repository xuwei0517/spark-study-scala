package cn.spark.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object BroadcastVariable {
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("broadcast").setMaster("local")
    val sc = new SparkContext(sparkConf)
    
    //获取共享变量
    val factor = 2;
    val broadcastFactor = sc.broadcast(factor);
    
    val numberList = Array(1,2,3,4,5);
    val numberListRDD = sc.parallelize(numberList, 2)
    
    val mapRDD = numberListRDD.map { num => num * broadcastFactor.value }
    mapRDD.foreach { num => println(num) }
  }
}