package com.kakao.adrec.atom.metric
package common.sparkmodule.io

import common.domain.Identifier
import common.utils.LogicalDateTime

import org.apache.spark.sql.{DataFrame, SparkSession}

trait AtomIO{
  private lazy val spark: SparkSession = SparkSession.builder().getOrCreate()

  def readAtom(idType: Identifier.Value, logicalDateTime: LogicalDateTime): DataFrame = {
    val atomBasePath = idType match {
      case Identifier.ADID => "/ns2/bizrec/lookalike/atom_log/bizrec-atom-filter-result/data"
      case Identifier.ACID => "/ns2/bizrec_acid/lookalike/atom_log/bizrec-atom-filter-result-acid/data"
    }

    // ranker log 지연 가능성 존재. 1시간 이전 atom log까지 포함
    val atomLogPaths = List(
      s"${atomBasePath}/date=${logicalDateTime.getDate()}/hour=${logicalDateTime.getHour()}",
      s"${atomBasePath}/date=${logicalDateTime.getMinusHourDate(1)}/hour=${logicalDateTime.getMinusHour(1)}"
    )

    spark.read
      .option("basePath", atomBasePath)
      .text(atomLogPaths: _*)
  }
}
