package com.kakao.adrec.atom.metric
package enrich.batch

import common.domain.{Identifier, Ranker}
import common.utils.{LogicalDateTime, SparkJob}
import common.sparkmodule.io.{AtomIO, EnrichIO, RankerIO}
import common.sparkmodule.transformer.EnrichTransformer

object EnrichJob_WinVimpClick extends SparkJob
  with AtomIO with RankerIO
  with EnrichTransformer
  with EnrichIO {

  def main(args: Array[String]): Unit = {
    // init
    // ex: adid win 5000 "2023-03-03 01:10:10"
    val idType = Identifier.withName(args(0))
    val rankerType = Ranker.withName(args(1))
    val resultRepartitionCount = args(2).toInt
    val logicalDateTime = new LogicalDateTime(args(3))

    // spark job
    val atomDF = readAtom(idType, logicalDateTime)
    val rankerDF = readRankerLog(idType, rankerType, logicalDateTime)

    val enrichDF = transformAtomAndRankerToEnrich(idType, rankerType, atomDF, rankerDF)

    writeEnrich(idType, rankerType, enrichDF, resultRepartitionCount, logicalDateTime)

    sparkStop()
  }
}

// spark.sql.shuffle.partitions = 7000
// spark.driver.memory	40g
// spark.executor.cores	5
// spark.executor.memory	14g
// spark.executor.instances	200
// snappy
// 9.4 min

// spark.sql.shuffle.partitions = 6000
// spark.driver.memory	40g
// spark.executor.cores	5
// spark.executor.memory	14g
// spark.executor.instances	200
// gzip
// 11 min

// spark.sql.shuffle.partitions = 3000
// spark.driver.memory	40g
// spark.executor.cores	5
// spark.executor.memory	24g
// spark.executor.instances	60
// gzip
// 29min => instance수가 부족하면 무조건 퍼포먼스 감소, 24g도 spill발생
