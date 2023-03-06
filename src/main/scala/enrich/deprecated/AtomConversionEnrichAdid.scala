package com.kakao.adrec.atom.metric
package enrich.deprecated

import common.domain.{Identifier, Ranker}
import common.utils.Enrich

object AtomConversionEnrichAdid {

  def main(args: Array[String]): Unit = {
    val logicalDateTimeStr = args(0)

    val enrich = new Enrich(Identifier.ADID, logicalDateTimeStr)

    val convDF = enrich.readRankerLog(Ranker.CONVERSION)
    val actionDF = enrich.readRankerLog(Ranker.ACTION)
    val convActionDF = convDF.union(actionDF)

    val atomClickForWeekDF = enrich.readEnrichLog(Ranker.CLICK, 0, 240, 7)
    val atomConvDF = enrich.joinAtomClickAndAtomConvLog(atomClickForWeekDF, convActionDF, Ranker.CONVERSION)

    enrich.write(Identifier.ADID, Ranker.CONVERSION, atomConvDF, 1)

    enrich.stop()
  }
}

