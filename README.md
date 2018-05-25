# wmt_analytics_cassandra_embedded
This shows how to integrate spark with embedded cassandra
Example implementation is for  cooking.com recipe big data pipeline solution The data is of the format :

recipe_id, recipe_name, description, ingredient, active, updated_date, created_date 1, pasta, Italian pasta, tomato sauce, true, 2018-01-09 10:00:57, 2018-01-10 13:00:57 1, pasta, null, cheese, true, 2018-01-09 10:10:57, 2018-01-10 13:00:57 2, lasagna, layered lasagna, cheese, true, 2018-01-09 10:00:57, 2018-01-10 13:00:57 2, lasagna, layered lasagna, blue cheese, false, 2018-01-09 10:00:57, 2018-01-10 13:00:57	…

Expected file size is 1TB per hour

This uses spark-cassandra-connector-embedded from com.datastax.spark for unit testing and integation testing. Please look at build.sbt for more information .
