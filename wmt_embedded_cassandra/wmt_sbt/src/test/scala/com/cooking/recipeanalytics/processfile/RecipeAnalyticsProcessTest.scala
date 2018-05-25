package com.cooking.recipeanalytics.processfile

import java.io.File

import com.cooking.recipeanalytics.common.CommonSetup

class RecipeAnalyticsProcessTest extends CommonSetup {

  test("testRun Method as Whole") {
    val analyticsProces= new RecipeAnalyticsProcess
    analyticsProces.run(analyticsConf,new File("/home/cloudera/wmt_embedded_cassandra/wmt_sbt/src/test/resources/data/2018-01-09_10_recipe_data_raw.csv"),fileOperations)


  }
}
