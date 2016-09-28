import kafka.serializer.StringDecoder

/**
  * Created by user9 on 21/09/16.
  */
object Consumer_sauvegarde {

  import com.datastax.spark.connector._
  import com.datastax.spark.connector.cql.CassandraConnector
  import org.apache.spark.streaming._
  import org.apache.spark.streaming.kafka._
  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]) {
    val jarpath = Array("~/spark-cassandra-connector-assembly-1.6.0.jar")

    //    val conf = new SparkConf().setMaster("local[2]").setAppName(getClass.getSimpleName)
    //      .set("spark.executor.memory", "1g").set("spark.cassandra.connection.host", "127.0.0.1")
    val conf = new SparkConf(true).setAppName("scc").setMaster("local[2]").set("spark.cassandra.connection.host", "10.1.254.51,10.1.254.62,10.1.254.116")


    val sc = new SparkContext(conf.setJars(jarpath))
    val ssc = new StreamingContext(sc, Seconds(10))

    //    val conf = new SparkConf(true)
    //      .setMaster("local[*]")
    //      .setAppName(getClass.getSimpleName)
    //      .set("spark.executor.memory", "lg")
    //      .set("spark.cores.max", "1")
    //      .set("spark.connection.cassandra.host", "127.0.0.1")


    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS kafka1 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3 }")
      session.execute(s"CREATE TABLE IF NOT EXISTS kafka1.longitude1 (X TEXT PRIMARY KEY)")
      session.execute(s"TRUNCATE kafka1.longitude1")
    }

    // kafka
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val topics = Set("clickstream")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)

//    val msg_sc = messages.context.sparkContext

    messages.print()

    //val nn = msg_sc.parallelize(messages)


    //messages.map(_).countByValue().saveToCassandra("demo", "wordcount")
    messages.foreachRDD(rdd => rdd.saveToCassandra("kafka1", "longitude1"))

    ssc.start()

    //    msg_sc
    //      .parallelize(1 to 1,1)
    //      .saveToCassandra("kafka1", "longitude1")//.cassandraTable("kafka1", "longitude1").collect.foreach(println)

    //    japi.CassandraStreamingJavaUtil.javaFunctions(messages)
    //      .writerBuilder("kafka1", "longitude1", RowWriterFactory().withColumnSelector(SomeColumns("X")).saveToCassandra()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
    //    val tt = sc.textFile("/home/user9/IdeaProjects/kafkaConsumerSpark/src/main/scala-2.10/words")

    //
    //    sc.textFile("/home/user9/IdeaProjects/kafkaConsumerSpark/src/main/scala-2.10/words")
    //      .flatMap(_.split("\\s+"))
    //      .map(word => (word.toLowerCase, 1))
    //      .reduceByKey(_ + _)
    //      .saveToCassandra("demo3", "wordcount3")
    //    // print out the data saved from Spark to Cassandra
    //    sc.cassandraTable("demo3", "wordcount3").collect.foreach(println)

    //sc.stop()


  }

}