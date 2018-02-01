package es.us.randomDataset

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Create a random dataset with row and column number given
  *
  * @author José María Luna
  * @version 1.0
  * @since v1.0 Dev
  */
object MainCreateFile {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Generate Random Dataset")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    var dimensions = 3
    var clusters = 5
    var instances = 100
    var standDev = 0.05f

    RandomDataset.createFile(sc, dimensions, clusters, instances, standDev, "")

    sc.stop()
  }




}

