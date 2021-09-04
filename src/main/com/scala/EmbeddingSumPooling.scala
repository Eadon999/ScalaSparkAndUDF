package com.scala

import breeze.linalg._
import org.apache.hadoop.hive.ql.exec.UDF


object EmbeddingSumPooling extends UDF{
  """udf需要把object改为class"""
  def evaluate(songSeqEmbeddings: String, seqDim: Int,rowSp:String,valSp:String): String = {
      val spCol = songSeqEmbeddings.split(rowSp)
      val rowNum = spCol.length
      val colNum = seqDim

      val resVec = spCol.map(x => x.split(valSp)).map(x => x.map(x => x.toFloat))
      val resVecFlatten = resVec.flatten
      println(resVecFlatten.toList)
      //由于平铺成一维数组与创建二维矩阵的方式（按行或按列）正好相反，因此在创建二维矩阵时需要交换行数和列数，并在最后把矩阵转置即可
      val mat = new DenseMatrix(colNum, rowNum, resVecFlatten).t

      val sum_res = sum(mat, Axis._0).inner.data

      sum_res.mkString(" ")
  }



  def main(args: Array[String]): Unit = {
    println("======start=======")
    val strInput = "1=5=9;2=5=8"
    val res = evaluate(strInput,3,";","=")
    println("sum res:" + res)
  }

}
