package com.kakao.adrec.atom.metric
package common.domain

case class Ranking(
                    ad: String
                  )

case class Winner(
                   rankerId: String,
                   rankerType: String,
                   trackId: String,
                   adAccountId: Long,
                   pricing: String,
                   price: Double,
                   bidAmount: Double,
                   chargeAmount: Double,
                   campaignType: String,
                   goal: String,
                   objectiveType: String,
                   objectiveDetailType: String,
                   bidStrategy: String,
                   ranking: Ranking,
                   objectiveValue: String,
                   creativeId: Int
                 )

case class Action(
                   actionId: String,
                   actionType: String,
                   actionDttm: String,
                   actionChain: Array[ActionChain],
                   actionLocation: String,
                   click_conv_diff_days: Int
                 )

case class ActionChain(
                        actionId: String,
                        actionType: String,
                        actionDttm: String,
                        actionLocation: String
                      )

case class Rank(
                 metrics: String,
                 at: String
               )

case class Bid(
                bidRequestId: String
              )

case class Win(
                winAmount: Double,
                dspSecondPrice: Double
              )

case class Device(
                   connectionType: String,
                   deviceType: String,
                   adId: String,
                   dnt: Boolean,
                   lmt: Boolean
                 )

case class User(
                 gender: String,
                 ageBand: String,
                 id: String,
                 accid: String,
                 activatedId: String,
                 userDomainPolicy: String
               )

case class Dsp(
                placement: String,
                retargetingMetrics: String
              )

case class RetargetingMetrics(
                               cmm:  Map[String, CreativeMetric]
                             )
case class CreativeMetric (
                            ti: Int,
                            dpc: Int,
                            tn: String,
                            tv: String,
                            bn: String,
                            rmn: String,
                            pms: Array[ProductMetric]
                          )
case class ProductMetric (
                           pi: String,
                           p: Int,
                           r: Int,
                           irt: Int
                         )

case class Ssp(
                dealId: String,
                impCondition: String,
                inhouse: Boolean,
                tagId: String
              )

case class Bcon(
                 event_code: String,
                 params: BconParams,
                 conv_iris_click: ConvIrisClick,
                 props: BconProps
               )

case class BconParams(
                       krw_total_price: Double
                     )

case class BconProps(
                      usable_only_stats: String
                    )

case class ConvIrisClick(
                          click_conv_diff_days: Int
                        )

case class RawMessage(
                       winner: Winner,
                       action: Action,
                       rank: Rank,
                       from: String,
                       bid: Bid,
                       user: User,
                       dsp: Dsp,
                       ssp: Ssp,
                       device: Device,
                       bcon: Bcon
                     )

case class AdLog(
                  adPurpose: String,
                  campaignId: String,
                  adGroupId: String,
                  rawMessage: RawMessage,
                  uniqueId: String,
                  valid: String,
                  processDttm: String
                )
