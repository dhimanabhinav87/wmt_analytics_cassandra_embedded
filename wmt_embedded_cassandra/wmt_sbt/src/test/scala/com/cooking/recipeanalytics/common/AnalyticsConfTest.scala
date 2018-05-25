package com.cooking.recipeanalytics.common

import org.scalatest.FunSuite

class AnalyticsConfTest extends FunSuite {
  val analyticsConf: AnalyticsConf = new AnalyticsConf
  analyticsConf.run("/home/cloudera/wmt_embedded_cassandra/wmt_sbt/src/test/resources/testParm.cfg")

  test("testRun") {
    assert(analyticsConf.recipeHiveTbl==="wmt_analytics.recipe_hourly")
  }

}
