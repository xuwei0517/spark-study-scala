package cn.spark.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ParallelizeCollection {
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("ParallelizeCollection")
    
    val sc = new SparkContext(sparkConf);
    
    val nums = Array(1,2,3,4,5,6,7,8,9,10)
    val numsrdd = sc.parallelize(nums, 1)
    val sum = numsrdd.reduce(_ + _)
    println("和为："+sum);
  } 
}