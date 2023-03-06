package com.kakao.adrec.atom.metric
package core.deprecated

import common.domain.{Identifier, Ranker}
import common.utils.{Core, Enrich}

import org.apache.spark.storage.StorageLevel

object AtomWinMetricAcid {

  def main(args: Array[String]): Unit = {
    val logicalDateTimeStr = args(0)

    val enrich = new Enrich(logicalDateTimeStr)
    val core = new Core(logicalDateTimeStr)

    val atomRankerDF = enrich.readEnrichLog(Identifier.ACID, Ranker.WIN)
    val transformedAtomRankerDF = core.transformAtomRankerDF(Identifier.ACID, Ranker.WIN, atomRankerDF)

    transformedAtomRankerDF.persist(StorageLevel.DISK_ONLY)

    core.writeRawMetric(Identifier.ACID, Ranker.WIN, transformedAtomRankerDF, 2000)
    core.writeMetric(Identifier.ACID, Ranker.WIN, transformedAtomRankerDF)

    core.stop()
  }
}
