import org.apache.spark.SparkContext._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by user9 on 21/09/16.
  */
object testML {

  import org.apache.spark.{SparkContext, _}
  import org.apache.spark.streaming._
  import org.apache.spark.streaming.kafka._

  def main(args: Array[String]) {

    //sc.stop
    val jarpath = Array("~/spark-cassandra-connector-assembly-1.6.0.jar")
    //val conf = new SparkConf(true).setAppName("scc").set("spark.cassandra.connection.host", "10.1.254.51,10.1.254.62,10.1.254.116")
    val conf = new SparkConf(true).setAppName("scc").setMaster("local[2]").set("spark.cassandra.connection.host", "10.1.254.51,10.1.254.62,10.1.254.116")
    //val conf = new SparkConf(true).setAppName("scc").setMaster("local[2]").set("spark.cassandra.connection.host", "10.1.254.51")
    //val conf = new SparkConf(true).setAppName("scc").setMaster("spark://10.1.254.62:7077").set("spark.cassandra.connection.host", "10.1.254.51,10.1.254.62,10.1.254.116")
    val sc = new SparkContext(conf.setJars(jarpath))
    //println(sc)
    println("******1")


    val test_spark_rdd = sc.cassandraTable("system", "peers")
    println(test_spark_rdd)
    test_spark_rdd.foreach(println)

    println("******2")

    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS demo3 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3 }")
      session.execute(s"CREATE TABLE IF NOT EXISTS demo3.wordcount3 (word TEXT PRIMARY KEY, count COUNTER)")
      session.execute(s"TRUNCATE demo.wordcount")
    }

    sc.textFile("/home/user14/Documents/Idea")
      .flatMap(_.split("\\s+"))
      .map(word => (word.toLowerCase, 1))
      .reduceByKey(_ + _)
      .saveToCassandra("demo3", "wordcount3")

    // print out the data saved from Spark to Cassandra
    sc.cassandraTable("demo3", "wordcount3").collect.foreach(println)

    sc.stop()

  }
}