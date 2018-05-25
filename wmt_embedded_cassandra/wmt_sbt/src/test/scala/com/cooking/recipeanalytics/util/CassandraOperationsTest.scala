package com.cooking.recipeanalytics.util

import com.cooking.recipeanalytics.common.CommonSetup
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.YamlTransformations
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.types._

class CassandraOperationsTest extends CommonSetup {

  override def clearCache(): Unit = CassandraConnector.evictCache()

  //Sets up CassandraConfig and SparkContext
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)

  override def beforeAll(): Unit = {
    super.beforeAll()
    connector.withSessionDo { session =>
      session.execute("CREATE KEYSPACE IF NOT EXISTS wmt_test WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")
      session.execute("CREATE TABLE wmt_test.sample(id int, username text, PRIMARY KEY(id));")
    }
  }

  test("testSaveDfToCassandra") {
    import spark.implicits._
    val testSchema = StructType(Array(StructField("id", IntegerType, true), StructField("username", StringType, true)))
    val df: DataFrame = sc.parallelize(Seq((1, "adhiman"), (2, "sdhiman"))).toDF("id","username")
    val res2: Boolean = CassandraOperations.saveDfToCassandra("sample",analyticsConf,df)
    val cassandraData = spark.read.cassandraFormat("sample", "wmt_test").load()
    cassandraData.show()

    assert(cassandraData.count()===2)


  }

}