package com.kakao.adrec.atom.metric
package common.domain

case class AtomUser(
                     aid: String,
                     adid: String,
                     lmt: Boolean,
                     dnt: Boolean,
                     acidDnt: Boolean,
                     potentialPopulation: Boolean
                   )

case class SurvivalDesc(
                         matchTag: List[String],
                         sv: Integer,
                         cms: Boolean,
                         trt: String,
                         cl: Integer,
                         customTarget: List[CustomTarget],
                         cos: Cos
                       )
case class Cos(
                v: Boolean,
                w: Float,
                m: String,
                l: Integer,
                b: String
              )

case class AtomLog(
                    bid: String,
                    acidBid: String,
                    user: AtomUser,
                    ln: String,
                    survivals: Map[String, SurvivalDesc],
                    convBucket: Map[String, Array[String]],
                    trtBucket: Map[String, Map[String, Array[String]]],
                    unknowns: Integer,
                    responseTime: String,
                    requestTime: String,
                    stateInfoDttm: String,
                    reqElapsed: Integer,
                    tagCount: Integer,
                    tcs: Integer,
                    tcc: Integer,
                    tco: Integer,
                    tcr: Integer,
                    tes: Integer,
                    ies: Integer,
                    at: String,
                    version: String,
                    an: String,
                    pm: String,
                    hs: Integer,
                    tcom: Map[String, Integer],
                    tccm: Map[String, Integer],
                    tcrm: Map[String, Integer],
                    hsm: Map[String, Integer]
                  )

case class CustomTarget(
                         m: String,
                         l: Integer,
                         e: Boolean
                       )
