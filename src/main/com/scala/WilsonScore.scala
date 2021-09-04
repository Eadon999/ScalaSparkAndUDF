package com.scala

import org.apache.hadoop.hive.ql.exec.UDF


class WilsonScore extends UDF {
  """udf需要把object改为class"""

  def evaluate(pos_input: Int, total: Int, p_z: Double = 2.0): Double = {
    """
    :param pos: 正例数
    :param total: 总数
    :param p_z: 正太分布的分位数,一般取2即
    :return: 威尔逊得分
    """
    val pos = if (pos_input > total) total else pos_input
    val pos_rat = pos * 1.0 / total * 1.0
    val score = (pos_rat + (math.pow(p_z, 2) / (2.0 * total))
      - ((p_z / (2.0 * total)) * math.sqrt(4.0 * total * (1.0 - pos_rat) * pos_rat + math.pow(p_z, 2)))) /
      (1.0 + math.pow(p_z, 2) / total)
    return score
  }
}
