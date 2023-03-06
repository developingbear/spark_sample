package com.kakao.adrec.atom.metric
package common.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf

class CoreUDF extends Serializable {
  val strToMap = udf(_strToMap _)
  val mapToTagMap = udf(_mapToTagMap _)
  val convertIdTypeBitToReadable = udf(_convertIdTypeBitToReadableOrigin _)
  val getRetargetProductCount = udf(_getRetargetProductCount _)

  private def _strToMap(str: String): Map[String, String] = {
    val strings = str.split(";")
    strings(0).split("=").mkString
    str.split(";").map(s => {
      val kv = s.split("=")
      if (kv.size > 1) {
        kv(0) -> kv(1)
      } else {
        kv(0) -> ""
      }
    }).toMap
  }

  private def _mapToTagMap(array: Seq[String]): Map[String, Long] = {
    if (array == null || array.isEmpty) {
      return Map[String, Long]()
    }

    val mapped = array
      .filter(i => i.length != 0)
      .map(i => {
        val model = i.split("#")(1).split("\\.")(0)
        val level = i.split("#")(1).split("\\.")(1).toLong
        model -> level
      })

    mapped.groupBy(_._1)
      .mapValues(arr => arr.map(i => i._2).min)
  }

  private def _convertIdTypeBitToReadableOrigin(input: Integer): String = {
    val m1 = Map(1 -> "aid", 2 -> "adid", 4 -> "acid")

    var exps = List[String]()
    m1.foreach {
      case (bit, expression) => {
        val op = bit & input
        if (op == bit) {
          println(expression)
          exps = exps :+ expression
        }
      }
    }
    exps.mkString("|")
  }

  private def _getRetargetProductCount(pms: Seq[Row]): Int = {
    var retProdCnt = 0
    pms.foreach(p => {
      if (p.getAs[Int]("irt") == 1) {
        retProdCnt = retProdCnt + 1
      }
    })
    retProdCnt
  }
}
