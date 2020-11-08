package com.ljh

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, desc, window}

/**
 * csv 使用
 * spark-submit  --class com.ljh.CsvExample --master local ./spark-learning-1.0.jar
 */
object CsvExample {
  def main(args: Array[String]): Unit = {
    // 输入 文件路径
    val dataInputPath = "/home/work/data/learn_data/guide-data/data/retail-data/by-day/*.csv"
    // 输出文件路径
    val saveTestFilePath = "/home/work/data/test/spark_dir"

    // spark 配置
    val sparkConf = new SparkConf().setAppName("SparkSessionTemplate").setMaster("local[2]")
      .set("spark.sql.shuffle.partitions", "2")   // 设置分区为2个


    // 创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val staticDataframe = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(dataInputPath).toDF()

    val dataFrame = staticDataframe.selectExpr("CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
      .groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day"))
      .sum("total_cost")

    // 展示前5个数据
    dataFrame.show(5)

    deleteDir(new File(saveTestFilePath))

    dataFrame.rdd.saveAsTextFile(saveTestFilePath)

    println("成功..................")
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
