package com.cooking.recipeanalytics.util

import com.cooking.recipeanalytics.common.CommonSetup
import org.apache.spark.sql.DataFrame

class RecipeAnalyticsHelperTest extends CommonSetup {

  test("testAppendSourceFileTsToDf") {
    import spark.implicits._
    val file_ts= "2018-01-01 11:11:11"
    val df: DataFrame = sc.parallelize(Seq((1, "adhiman"), (2, "sdhiman"))).toDF("id","username")
    val res: DataFrame = RecipeAnalyticsHelper.appendSourceFileTsToDf(file_ts,analyticsConf,df)
    assert(res.collect().length===2)


  }

}
