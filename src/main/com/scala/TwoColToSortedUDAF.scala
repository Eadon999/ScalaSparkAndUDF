package com.scala.udf

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable

/***
  * UDAF函数，将两列数据排序，第一列为得分，第二列为对应的id
  * 根据第二列序号将第一列的值排列成有序数组（或拼接成字符串）
 *
  * @param rankType 需要组成排序数据（或字符串）值对应的顺序序号
  * @param valueType 需要排列成有序数组（或拼接成字符串）的值
 */
class TwoColToSortedUDAF(rankType: DataType, valueType: DataType)
  extends UserDefinedAggregateFunction {

  //UDAF与DataFrame列有关的输入样式，StructField的名字并没有特别要求，完全可以认为是两个内部结构的列名占位符。
  //至于UDAF具体要操作DataFrame的哪个列，取决于调用者，但前提是数据类型必须符合事先的设置。
  override def inputSchema: StructType = new StructType().add("rank", rankType).add("value", valueType)


  //定义存储聚合运算时产生的中间数据结果的Schema
  override def bufferSchema: StructType = new StructType().add("middleMap", MapType(LongType, StringType))


  //标明了UDAF函数的返回值类型
  override def dataType: DataType = StringType


  //用以标记针对给定的一组输入,UDAF是否总是生成相同的结果
  override def deterministic: Boolean = true


  //对聚合运算中间结果的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0, mutable.Map[Long, String]())


  //第二个参数input: Row对应的并非DataFrame的行,而是被inputSchema投影了的行。以本例而言，每一个input就应该只有两个Field的值
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val buffMap: mutable.Map[Long, String] = mutable.Map(buffer.getAs[Map[Long, String]](0).toSeq: _*)
    val key = input.getAs[Long](0)
    val value = input.getAs[String](1)

    if (!buffMap.contains(key)) {
      buffMap += (key -> value)
    }

    buffer.update(0, buffMap)
  }


  //负责合并两个聚合运算的buffer，再将其存储到MutableAggregationBuffer中
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //获取map，并将默认的不可变map转换为可变map
    val buffMap1: mutable.Map[Long, String] = mutable.Map(buffer1.getAs[Map[Long, String]](0).toSeq: _*)
    val buffMap2: Map[Long, String] = buffer2.getAs[Map[Long, String]](0)
    val keys = buffMap2.keys

    if (keys != null && keys.nonEmpty) {
      for(key <- keys ) {
        if(!buffMap1.contains(key)) {
          buffMap1 += (key -> buffMap2.getOrElse(key, ""))
        }
      }
    }

    buffer1.update(0, buffMap1)
  }


  //完成对聚合Buffer值的运算,得到最后的结果
  override def evaluate(buffer: Row): String = {
    val aggregateMap = buffer.getAs[Map[Long, String]](0)
    var result: String = ""

    if(aggregateMap != null && aggregateMap.nonEmpty){
      val sortedKeySeq = aggregateMap.keySet.toSeq.sorted

      for(key <- sortedKeySeq){
        if(StringUtils.isNotEmpty(aggregateMap.getOrElse(key, ""))){
          result += "," + aggregateMap.getOrElse(key, "")
        }
      }
    }

    result.stripPrefix(",")
  }

}