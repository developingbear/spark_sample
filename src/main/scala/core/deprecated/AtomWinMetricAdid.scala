package com.kakao.adrec.atom.metric
package core.deprecated

import common.domain.{Identifier, Ranker}
import common.utils.{Core, Enrich}

import org.apache.spark.storage.StorageLevel

object AtomWinMetricAdid {

  def main(args: Array[String]): Unit = {
    val logicalDateTimeStr = args(0)

    val enrich = new Enrich(logicalDateTimeStr)
    val core = new Core(logicalDateTimeStr)

    val atomRankerDF = enrich.readEnrichLog(Identifier.ADID, Ranker.WIN)
    val transformedAtomRankerDF = core.transformAtomRankerDF(Identifier.ADID, Ranker.WIN, atomRankerDF)

    transformedAtomRankerDF.persist(StorageLevel.DISK_ONLY)

    core.writeRawMetric(Identifier.ADID, Ranker.WIN, transformedAtomRankerDF, 2000)
    core.writeMetric(Identifier.ADID, Ranker.WIN, transformedAtomRankerDF)

    core.stop()
  }
}
