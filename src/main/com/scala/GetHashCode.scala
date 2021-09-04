package com.scala

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hive.ql.exec.UDF


object GetHashCode extends UDF {

  def evaluate(a: String, b: String): String = {
    val cols = checkBlank(a) + checkBlank(b)

    cols.hashCode.toString
  }

  private def checkBlank(str: String): String = {
    if (StringUtils.isBlank(str)) {
      ""
    } else {
      str
    }
  }

  def main(args: Array[String]): Unit = {
    println("==========scala success============")
    val (a, b) = ("test2", "test")
    val result = evaluate(a, b)

    println(result)

    case class Student(name: String, age: Int, weight: Double)

    val s1 = Student("zhang", 23, 65.6)

    println(s1)


  }
}

