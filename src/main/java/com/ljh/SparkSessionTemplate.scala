package com.ljh

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
 * 一个 Spark Session 使用例子
 * spark-submit  --class com.ljh.SparkSessionTemplate --master local ./spark-learning-1.0.jar
 */

object SparkSessionTemplate {

  def main(args: Array[String]): Unit = {
    // 输出文件路径
    val saveTestFilePath = "/home/work/data/test/spark_dir"

    val sparkConf = new SparkConf().setAppName("SparkSessionTemplate").setMaster("local[2]")

    val spark = SparkSession.
      builder().
      config(sparkConf).
      getOrCreate()

    val myRange = spark.range(10000).toDF("number")

    deleteDir(new File(saveTestFilePath))

    // 文件保存路径
    myRange.rdd.saveAsTextFile(saveTestFilePath)

    // 打印解析过程
    myRange.explain

    println("成功。。。。")
  }


  def deleteDir(dir: File): Unit = {
    val files = dir.listFiles()
    files.foreach(f => {
      if (f.isDirectory) {
        deleteDir(f)
      } else {
        f.delete()
      }
    })
    dir.delete()
  }

}
