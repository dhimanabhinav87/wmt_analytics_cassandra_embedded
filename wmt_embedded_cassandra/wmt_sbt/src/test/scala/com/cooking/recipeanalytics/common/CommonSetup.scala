package com.cooking.recipeanalytics.common

import com.cooking.recipeanalytics.processfile.FileOperations
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, Suite}

trait CommonSetup extends FunSuite with BeforeAndAfterAll with SparkTemplate with EmbeddedCassandra{
  this: Suite =>

  val analyticsConf: AnalyticsConf = new AnalyticsConf
  analyticsConf.run("/home/cloudera/wmt_embedded_cassandra/wmt_sbt/src/test/resources/testParm.cfg")

  override def clearCache(): Unit = CassandraConnector.evictCache()
  //Sets up CassandraConfig and SparkContext
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)

  val connector = CassandraConnector(defaultConf)
  override def beforeAll(): Unit = {
    super.beforeAll()
    connector.withSessionDo { session =>
      session.execute("CREATE KEYSPACE IF NOT EXISTS wmt_test WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")
      session.execute("CREATE TABLE wmt_test.recipes(recipe_name text PRIMARY KEY);")
      session.execute("CREATE TABLE wmt_test.recipe_ingredients (recipe_name text,ingredient text, primary key (recipe_name,ingredient));")
    }
  }
  val fileOperations = new FileOperations

  implicit val spark: SparkSession = sparkSession

  override def afterAll(): Unit = {
    spark.close()

  }
}
