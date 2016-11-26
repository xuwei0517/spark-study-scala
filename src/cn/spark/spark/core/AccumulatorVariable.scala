package cn.spark.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object AccumulatorVariable {
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("AccumulatorVariable").setMaster("local")
    val sc = new SparkContext(sparkConf)
    
    //获取共享变量
    val accumulator = sc.accumulator(0)
    
    val numberList = Array(1,2,3,4,5);
    val numberListRDD = sc.parallelize(numberList, 2)
    
    numberListRDD.foreach { num => accumulator.add(num) }
    println(accumulator)
    
  }
  
}