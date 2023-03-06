package com.kakao.adrec.atom.metric
package common.utils

import org.apache.spark.sql.SparkSession

class SparkBaseJob {
  protected val spark: SparkSession = SparkSessionFactory.getOrCreate()

  def stop(): Unit = spark.stop()
}
