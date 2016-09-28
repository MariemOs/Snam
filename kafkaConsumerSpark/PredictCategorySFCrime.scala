/**
  * Created by user9 on 27/09/16.
  */
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
import com.datastax.spark.connector.types.TimestampType
import jdk.nashorn.internal.runtime.regexp.joni.constants.StringType
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}


object PredictCategorySFCrime {

  def main(args: Array[String]) {


    //  sc.cassandraTable("demo3", "wordcount3").collect.foreach(println)
    /*
    Features :

    Dates: timestamp of the crime incident in the PST timezone
    Category : class
    Descript: detailed description of the crime incident (only in the training dataset)
    DayOfWeek: the day of the week of the crime incident
    PdDistrict: name of the police department district which handled the crime incident
    Resolution: how the crime incident was resolved (it’s also only in the training dataset)
    Address: the approximate street address of the incident
    X: longitude of the incident
    Y: latitude of the incident

  */
    val jarpath = Array("~/spark-cassandra-connector-assembly-1.6.0.jar")
    val conf = new SparkConf(true).setAppName("scc").setMaster("local[2]").set("spark.cassandra.connection.host", "10.1.254.51,10.1.254.62,10.1.254.116")
    val sc = new SparkContext(conf.setJars(jarpath))
    //val sqlContext = new SQLContext(sc)

    val path = "/home/user9/ProjetFinal/Data/"
    val file_name1 = "train.csv"
    val file_name2 = "test.csv"
    val data_source = scala.io.Source.fromFile(path + file_name1).getLines.filter(!_.isEmpty()).map(_.split(",")).toArray
    val test_source = scala.io.Source.fromFile(path + file_name2).getLines.drop(1).map(_.split(":")(0)).toArray

    val line = sc.textFile(path + file_name1)

    val dataheader = line.map(l => l.split(",").map(_.trim))
    val first = dataheader.first
    val data = dataheader.filter(_(0) != first(0))


    val Category = data_source.map(x => x.toList(1)).toSet.toList.sorted
    val DayOfWeek = data_source.map(x => x.toList(3)).toSet.toList.sorted
    val PdDistrict = data_source.map(x => x.toList(4)).toSet.toList.sorted

    val intIteratordow = Iterator.from(1)
    val DOW_int = DayOfWeek.map(x => (x, intIteratordow.next().toDouble)).toMap

    val intIteratorcat = Iterator.from(1)
    val cat_int = Category.map(x => (x, intIteratorcat.next().toDouble)).toMap

    val intIteratorpd = Iterator.from(1)
    val PdDistrict_int = PdDistrict.map(x => (x, intIteratorpd.next().toDouble)).toMap




    first.foreach(println)


//    val data_vect = data_source.map(x => Vectors.dense(x(0).toDouble,protocol_type_valsInt(x(1)), service_valsInt(x(2)), flag_valsInt(x(3)), x(4).toDouble, x(5).toDouble, x(6).toDouble, x(7).toDouble, x(8).toDouble, x(9).toDouble, x(10).toDouble, x(11).toDouble, x(12).toDouble, x(13).toDouble, x(14).toDouble, x(15).toDouble, x(16).toDouble, x(17).toDouble, x(18).toDouble, x(19).toDouble, x(20).toDouble, x(21).toDouble, x(22).toDouble, x(23).toDouble, x(24).toDouble, x(25).toDouble, x(26).toDouble, x(27).toDouble, x(28).toDouble, x(29).toDouble, x(30).toDouble, x(31).toDouble, x(32).toDouble, x(33).toDouble, x(34).toDouble, x(35).toDouble, x(36).toDouble, x(37).toDouble, x(38).toDouble, x(39).toDouble, x(40).toDouble ))
//    val dataRDD = sc.parallelize(data_vect)
// Kmeans
    // val clusters_kmeans = org.apache.spark.mllib.clustering.KMeans.train(dataRDD, numClusters, nbIterations)

    /*

    val labels_kmeans = clusters_kmeans.predict(dataRDD).collect()
val labels_kmeansRDD = clusters_kmeans.predict(dataRDD)
val centers = clusters_kmeans.clusterCenters.map( x => x.toArray)
//val win = plot(dataArray, // matrice des coordonnées
//                  labels_kmeans,  // labels calculés des clusters
//                  '#', // parametres de visualisations pour chaque claster
//                  Palette.rainbow(numClusters) // couleurs des clusters
                  // valeurs de Palette possibles : topo, terrain, jet, redgreen, redblue, heat, rainbow
//                 )
//smile.plot.ScatterPlot.plot(centers,'O', Color.BLACK)
//              win.canvas.points(centers,'*',Color.BLACK)
val mat: RowMatrix = new RowMatrix(dataRDD)

// Compute the top 10 principal components.
val pc: Matrix = mat.computePrincipalComponents(3) // Principal components are stored in a local dense matrix.

// Project the rows to the linear space spanned by the top 10 principal components.
val projected = mat.multiply(pc)

val rdd = projected.rows.collect()
println(rdd)
val ss = rdd.map{x => Array(x(0), x(1), x(2))}
//val tt = Array(Array(1.9, 2.0), Array(1.9, 2.9))
val win = smile.plot.plot(ss, labels_kmeans, '#',Palette.rainbow(numClusters))



     */
  }
}