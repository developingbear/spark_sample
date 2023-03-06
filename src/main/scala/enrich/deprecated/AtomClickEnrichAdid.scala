package com.kakao.adrec.atom.metric
package enrich.deprecated

import common.domain.{Identifier, Ranker}
import common.utils.Enrich

object AtomClickEnrichAdid {

  def main(args: Array[String]): Unit = {
    val logicalDateTimeStr = args(0)

    val enrich = new Enrich(logicalDateTimeStr)

    val atomLogDF = enrich.readAtomLog(Identifier.ADID)
    val clickLogDF = enrich.readRankerLog(Identifier.ADID, Ranker.CLICK)
    val atomClickLogDF = enrich.joinAtomAndRankerLog(atomLogDF, clickLogDF, Ranker.CLICK)

    enrich.write(Identifier.ADID, Ranker.CLICK, atomClickLogDF, 5)

    enrich.stop()
  }
}

