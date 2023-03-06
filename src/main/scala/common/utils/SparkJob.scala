package com.kakao.adrec.atom.metric
package common.utils

import org.apache.spark.sql.SparkSession

abstract class SparkJob {
  protected val spark: SparkSession = SparkSessionFactory.getOrCreate()

  def sparkStop(): Unit = spark.stop()
}
