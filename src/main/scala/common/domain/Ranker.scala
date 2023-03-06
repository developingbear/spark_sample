package com.kakao.adrec.atom.metric
package common.domain

object Ranker extends Enumeration {
    val WIN = Value("win")
    val VIMP = Value("vimp")
    val CLICK = Value("click")
    val CONVERSION = Value("conversion")
    val ACTION = Value("action")
}