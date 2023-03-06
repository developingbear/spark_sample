package com.kakao.adrec.atom.metric
package enrich.deprecated

import common.domain.{Identifier, Ranker}
import common.utils.Enrich

object AtomVimpEnrichAcid {

  def main(args: Array[String]): Unit = {
    val logicalDateTimeStr = args(0)

    val enrich = new Enrich(logicalDateTimeStr)

    val atomLogDF = enrich.readAtomLog(Identifier.ACID)
    val rankerLogDF = enrich.readRankerLog(Identifier.ACID, Ranker.VIMP)
    val atomRankerLogDF = enrich.joinAtomAndRankerLog(atomLogDF, rankerLogDF, Ranker.VIMP)

    enrich.write(Identifier.ACID, Ranker.VIMP, atomRankerLogDF, 3000)

    enrich.stop()
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
