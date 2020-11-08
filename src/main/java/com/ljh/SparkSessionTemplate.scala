package com.ljh

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 一个 Spark Session 使用例子
 */

object SparkSessionTemplate {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TmpTest").setMaster("local[2]")

    val spark = SparkSession.
      builder().
      appName("test-1").
      config(sparkConf).
      getOrCreate()

    val myRange = spark.range(10000).toDF("number")

    // 文件保存路径
    myRange.rdd.saveAsTextFile("/home/work/data/test/spark_dir")

    println("成功。。。。")
  }

}
