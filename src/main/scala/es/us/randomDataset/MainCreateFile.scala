package es.us.randomDataset

import org.apache.spark.{SparkContext, SparkConf}

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
    val examples = 800000
    val columns = 7
    val rows = examples * columns
    var i = 0
    var j = 0
    var z = 0
    //Position advance after examples created
    var avance = 0

    var res = ofDim[Double](rows, columns)
    var matrix = ofDim[Integer](columns, 2)

    //Filling matrix with the range of random number
    for (i <- 0 to columns - 1) {
      var ini = 1 * Math.pow(10, i).toInt
      var fin = 2 * ini
      matrix(i)(0) = ini
      matrix(i)(1) = fin
    }


    for (i <- 0 to rows - 1) {
      //If we have completed the examples, we move the index avance one
      if (z == examples) {
        avance = avance + 1
        z = 0
      }
      for (j <- 0 to columns - 1) {
        var pos = j + avance
        //if we are out of boundary, we subtract the number of colums to set inside the border
        if (pos >= columns) {
          pos = pos - columns
        }
        res(i)(j) = getRandom(matrix(pos)(0), matrix(pos)(1))
      }
      z = z + 1
    }
    val data = sc.parallelize(res).map(x => x.mkString(";"))

    data.coalesce(1, shuffle = true).saveAsTextFile(Utils.whatTimeIsIt())

    sc.stop()
  }

  def getRandom(from: Integer, to: Integer): Double = {
    val rnd = new Random()
    return rnd.nextInt((to - from) + 1) + from
  }

}

