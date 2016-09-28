import com.datastax.spark.connector.writer.RowWriterFactory
import kafka.serializer.StringDecoder

/**
  * Created by user9 on 21/09/16.
  */
object Consumer {

  import java.awt.Color
  import scala.io.Source
  import javax.swing.JFrame
  import javax.swing.JPanel
  import java.awt.Dimension
  import java.awt.Toolkit
  import javax.swing.JFrame
  import java.awt.GridLayout
  import javax.swing.JPanel
  import org.apache.spark.SparkContext
  import org.apache.spark.rdd.RDD
  import org.apache.spark._
  import org.apache.spark.streaming._
  import org.apache.spark.streaming.StreamingContext._
  import org.apache.spark.streaming.kafka._
  import org.apache.spark.SparkContext._
  import com.datastax.spark.connector.cql.CassandraConnector
  import com.datastax.spark.connector._
  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]) {
    val jarpath = Array("~/spark-cassandra-connector-assembly-1.6.0.jar")
    val conf = new SparkConf(true).setAppName("scc").setMaster("local[2]").set("spark.cassandra.connection.host", "10.1.254.51,10.1.254.62,10.1.254.116")
    val sc = new SparkContext(conf.setJars(jarpath))
    val ssc = new StreamingContext(sc, Seconds(10))
//
//    CassandraConnector(conf).withSessionDo { session =>
//      session.execute(s"CREATE KEYSPACE IF NOT EXISTS kafka2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3 }")
//      session.execute(s"CREATE TABLE IF NOT EXISTS kafka2.longitude2 (X TEXT PRIMARY KEY)")
//      session.execute(s"TRUNCATE kafka2.longitude2")
//    }

    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS sf WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3 }")
      session.execute(s"CREATE TABLE IF NOT EXISTS sf.sfpdtotal (IncidntNum INT PRIMARY KEY, Category TEXT, Descript TEXT, DayOfWeek TEXT, Date TEXT, Time TEXT, PdDistrict TEXT, Resolution TEXT, Address TEXT, X TEXT, Y TEXT, Location TEXT, PdId TEXT, ZipCode TEXT, X1 INT, American_Indian_population FLOAT, Asian_population FLOAT, Average_Adjusted_Gross_Income_AGI_in_2012 FLOAT, Average_household_size FLOAT, Black_population FLOAT, Estimated_zip_code_population_in_2013 FLOAT, Females FLOAT, Hispanic_or_Latino_population FLOAT, Houses_and_condos FLOAT, Land_area FLOAT, Males FLOAT, Mar_2016_cost_of_living_index_in_zip_code FLOAT, Median_resident_age FLOAT, Native_Hawaiian_and_Other_Pacific_Islander_population FLOAT, Population_density_people_per_square_mile FLOAT, Renter_occupied_apartments FLOAT, Residents_with_income_below_50_of_the_poverty_level_in_2013 FLOAT, Salary_wage FLOAT, Some_other_race_population FLOAT, Two_or_more_races_population FLOAT, Water_area FLOAT, White_population FLOAT, Zip_code_population_in_2000 FLOAT, Zip_code_population_in_2010 FLOAT)")
      session.execute(s"TRUNCATE sf.sfpdtotal")

    }

    //[int -> INT,str -> TEXT,str,str,str,str,str,str,str,str,str,str,int,str,int,float -> FLOAT,float,float,float,float,float,float,float,float,float,float,float,float,float,float,float,float,float,float,float,float,float,float,float,int,int]

    //val varNames_col = SomeColumns("IncidntNum", "Category", "Descript", "DayOfWeek", "Date", "Time", "PdDistrict", "Resolution", "Address", "X", "Y", "Location", "PdId", "ZipCode", "X1", "American_Indian_population", "Asian_population", "Average_Adjusted_Gross_Income_AGI_in_2012", "Average_household_size", "Black_population", "Estimated_zip_code_population_in_2013", "Females", "Hispanic_or_Latino_population", "Houses_and_condos", "Land_area", "Males", "Mar_2016_cost_of_living_index_in_zip_code", "Median_resident_age", "Native_Hawaiian_and_Other_Pacific_Islander_population", "Population_density_people_per_square_mile", "Renter_occupied_apartments", "Residents_with_income_below_50_of_the_poverty_level_in_2013", "Salary_wage", "Some_other_race_population", "Two_or_more_races_population", "Water_area", "White_population", "Zip_code_population_in_2000", "Zip_code_population_in_2010")


    // kafka
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val topics = Set("clickstream")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)


    messages.foreachRDD { rdd => rdd.map(Tuple1(_)).saveToCassandra("kafka2", "longitude2") }
    messages.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate

//    val sc1 = new SparkContext(conf)
//
//        CassandraConnector(conf).withSessionDo { session =>
//          session.execute(s"CREATE KEYSPACE IF NOT EXISTS kafka2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3 }")
//          session.execute(s"CREATE TABLE IF NOT EXISTS kafka2.longitude2 (X TEXT PRIMARY KEY)")
//          session.execute(s"TRUNCATE kafka2.longitude2")
//        }



//    val msg_sc = messages.context.sparkContext



    //val nn = msg_sc.parallelize(messages)


    //messages.map(_).countByValue().saveToCassandra("demo", "wordcount")


    //messages.foreachRDD(rdd => rdd.saveToCassandra("kafka1", "longitude1"))

//    msg_sc
//      .parallelize(1 to 1,1)
//      .saveToCassandra("kafka1", "longitude1")//.cassandraTable("kafka1", "longitude1").collect.foreach(println)

//    japi.CassandraStreamingJavaUtil.javaFunctions(messages)
//      .writerBuilder("kafka1", "longitude1", RowWriterFactory().withColumnSelector(SomeColumns("X")).saveToCassandra()

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