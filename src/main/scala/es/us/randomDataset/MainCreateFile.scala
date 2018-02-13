package es.us.randomDataset

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create a random dataset with row and column number given
  *
  * @author José María Luna
  * @version 1.0
  * @since v1.0 Dev
  */
object MainCreateFile {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)

    val conf = new SparkConf()
      .setAppName("Generate Random Dataset")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    var dimensions = 3      //Number of features (columns)
    var clusters = 5        //Number of clusters
    var instances = 100     //Instances per cluster
    var standDev = 0.05f    //Standard deviation for the gaussian distribution
    val withClass = false

    RandomDataset.createFile(sc, dimensions, clusters, instances, standDev, "", withClass)

    sc.stop()
  }


}

