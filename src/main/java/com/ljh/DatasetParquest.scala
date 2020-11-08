package com.ljh

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * parquest 使用
 * spark-submit  --class com.ljh.DatasetParquest --master local ./spark-learning-1.0.jar
 */
object DatasetParquest {

  def main(args: Array[String]): Unit = {
    // 输入 文件路径
    val dataInputPath = "/home/work/data/learn_data/guide-data/data/flight-data/parquet/2010-summary.parquet/"
    // 输出文件路径
    val saveTestFilePath = "/home/work/data/test/spark_dir"

    val sparkConf = new SparkConf().setAppName("SparkSessionTemplate").setMaster("local[2]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val fligthDF = spark.read.parquet(dataInputPath)

    deleteDir(new File(saveTestFilePath))
    fligthDF.rdd.saveAsTextFile(saveTestFilePath)
    println("成功...")
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
