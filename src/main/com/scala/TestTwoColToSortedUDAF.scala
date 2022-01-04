package com.scala.udf

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.spark.sql.SparkSession
import com.kugou.udf.TwoColumnToSortedArrayUDAF
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType

object TestTwoColToSortedUDAF {


  def main(args: Array[String]): Unit = {
    println("==========scala success============")
    val spark = SparkSession.builder().master("local").appName("tfrecords_examples").getOrCreate()
    val df = spark.read.json("file:///F:\\LocalCode\\ScalaUdf\\configs\\test.txt")
    df.show()

    val udfTest = new TwoColumnToSortedArrayUDAF(LongType, StringType)
    //    val spark = SparkSession.builder().master("local[*]").appName("SparkStudy").getOrCreate()
    ////    spark.read.json("data/user").createOrReplaceTempView("v_user")
    //    spark.udf.register("dd",new TwoColumnToSortedArrayUDAF)
    val res = df.groupBy("uid").agg(udfTest(col("score"), col("id")))
    res.show()



  }
}

