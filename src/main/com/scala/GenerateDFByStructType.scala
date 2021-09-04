package com.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * desc:
 * Created by anmoFree on 2018/4/7
 */
object GenerateDFByStructType {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("My Spark Application") //设置 application 的名字
      .master("local")
//      .enableHiveSupport() //增加支持 hive Support, local不要设置, 否则会出现：java.lang.NoSuchFieldError: HIVE_STATS_JDBC_TIMEOUT
      .getOrCreate() //获取或者新建一个 sparkSession

    // 从指定文件创建RDD
    val fileRDD: RDD[String] = spark.sparkContext.textFile("file:///F:/stu.txt")

    println("*****************" + fileRDD.take(1) + "**************")

    // 切分数据
    val lineRDD: RDD[Array[String]] = fileRDD.map(_.split(" "))

    // 通过StructType直接指定每个字段的schema
    val schema = StructType(
      List(
        // true代表不为空
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("scores", FloatType, true)
      )
    )

    // 将RDD映射到rowRDD上
    val rowRDD: RDD[Row] = lineRDD.map(s => Row(s(0).toInt, s(1), s(2).toFloat))


    // 将schema信息应用到rowRDD上
    val stuDF: DataFrame = spark.createDataFrame(rowRDD, schema)

    print("*" * 10 + "dataframe data show:" + "*" * 10)
    stuDF.show(2)
    println("=============================================")

    // 将DataFrame注册为临时表
    stuDF.createOrReplaceTempView("t_student")

    // 对临时表进行操作
    val df: DataFrame = spark.sql("select * from t_student order by scores desc limit 2")
    df.show(2)

    // 停止Spark Context
    spark.stop()
  }
}
