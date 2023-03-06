package com.kakao.adrec.atom.metric
package core.deprecated

import common.domain.{Identifier, Ranker}
import common.utils.{Core, Enrich}

import org.apache.spark.storage.StorageLevel

object AtomClickMetricAcid {

  def main(args: Array[String]): Unit = {
    val logicalDateTimeStr = args(0)

    val enrich = new Enrich(logicalDateTimeStr)
    val core = new Core(logicalDateTimeStr)

    val atomWinDF = enrich.readEnrichLog(Identifier.ACID, Ranker.CLICK)
    val transformedAtomRankerDF = core.transformAtomRankerDF(Identifier.ACID, Ranker.CLICK, atomWinDF)

    transformedAtomRankerDF.persist(StorageLevel.DISK_ONLY)

    core.writeRawMetric(Identifier.ACID, Ranker.CLICK, transformedAtomRankerDF, 2000)
    core.writeMetric(Identifier.ACID, Ranker.CLICK, transformedAtomRankerDF)

    core.stop()
  }
}
