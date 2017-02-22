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

    var dimensions = 20
    var clusters = 9
    var instances = 1000000
    var desvTip = 0.1f

    if (args.length > 2) {
      dimensions = args(0).toInt
      clusters = args(1).toInt
      instances = args(2).toInt
      desvTip = args(3).toFloat
    }

    val valor = scala.math.pow(2, dimensions).toInt
    var marcados = 0
    var marcadosArray = Array.fill(valor) {
      0
    }

    var indicesSelected = Array[Int]()

    while (marcados < clusters) {
      val randomNumber = getRandom(0, valor - 1).toInt
      if (marcadosArray(randomNumber) == 0) {
        indicesSelected = indicesSelected :+ randomNumber
        marcadosArray(randomNumber) = 1
        marcados += 1
      }
    }
    println(indicesSelected.mkString(";"))
    val binarySelected = indicesSelected.map(toBinary(_, dimensions))
    println(binarySelected.mkString(";"))

    val indices = binarySelected
      .map(x => x.toArray.map(giveMeNumber(_)))

    val indicesRDD = sc.parallelize(indices).cache()

    val dataset = indicesRDD.flatMap(x =>
      for {i <- 0 until instances} yield {
        dimensions match {
          case 2 => (getGaussian(x(0), desvTip), getGaussian(x(1), desvTip))
          case 3 => (getGaussian(x(0), desvTip), getGaussian(x(1), desvTip), getGaussian(x(2), desvTip))
          case 4 => (getGaussian(x(0), desvTip), getGaussian(x(1), desvTip), getGaussian(x(2), desvTip), getGaussian(x(3), desvTip))
          case 5 => (getGaussian(x(0), desvTip), getGaussian(x(1), desvTip), getGaussian(x(2), desvTip), getGaussian(x(3), desvTip), getGaussian(x(4), desvTip))
          case 6 => (getGaussian(x(0), desvTip), getGaussian(x(1), desvTip), getGaussian(x(2), desvTip), getGaussian(x(3), desvTip), getGaussian(x(4), desvTip), getGaussian(x(5), desvTip))
          case 7 => (getGaussian(x(0), desvTip), getGaussian(x(1), desvTip), getGaussian(x(2), desvTip), getGaussian(x(3), desvTip), getGaussian(x(4), desvTip), getGaussian(x(5), desvTip), getGaussian(x(6), desvTip))
          case 8 => (getGaussian(x(0), desvTip), getGaussian(x(1), desvTip), getGaussian(x(2), desvTip), getGaussian(x(3), desvTip), getGaussian(x(4), desvTip), getGaussian(x(5), desvTip), getGaussian(x(6), desvTip), getGaussian(x(7), desvTip))
          case 9 => (getGaussian(x(0), desvTip), getGaussian(x(1), desvTip), getGaussian(x(2), desvTip), getGaussian(x(3), desvTip), getGaussian(x(4), desvTip), getGaussian(x(5), desvTip), getGaussian(x(6), desvTip), getGaussian(x(7), desvTip), getGaussian(x(8), desvTip))
          case 10 => (getGaussian(x(0), desvTip), getGaussian(x(1), desvTip), getGaussian(x(2), desvTip), getGaussian(x(3), desvTip), getGaussian(x(4), desvTip), getGaussian(x(5), desvTip), getGaussian(x(6), desvTip), getGaussian(x(7), desvTip), getGaussian(x(8), desvTip), getGaussian(x(9), desvTip))
          case 20 => new Dim20(getGaussian(x(0), desvTip), getGaussian(x(1), desvTip), getGaussian(x(2), desvTip), getGaussian(x(3), desvTip), getGaussian(x(4), desvTip), getGaussian(x(5), desvTip), getGaussian(x(6), desvTip), getGaussian(x(7), desvTip), getGaussian(x(8), desvTip), getGaussian(x(9), desvTip), getGaussian(x(10), desvTip), getGaussian(x(11), desvTip), getGaussian(x(12), desvTip), getGaussian(x(13), desvTip), getGaussian(x(14), desvTip), getGaussian(x(15), desvTip), getGaussian(x(16), desvTip), getGaussian(x(17), desvTip), getGaussian(x(18), desvTip), getGaussian(x(19), desvTip))
        }
      }
    )

    println(dataset.count())

    dataset.map(_.toString).coalesce(1).saveAsTextFile(s"C$clusters-D$dimensions-I$instances-${Utils.whatTimeIsIt()}")

    sc.stop()
  }

  def getRandom(from: Integer, to: Integer): Float = {
    val rnd = new Random()
    rnd.nextInt((to - from) + 1) + from
  }

  def getGaussian(average: Float, desv: Float): Float = {
    val rnd = new Random()
    rnd.nextGaussian().toFloat * desv + average
  }

  def toBinary(i: Int, digits: Int = 8) =
    String.format("%" + digits + "s", i.toBinaryString).replace(' ', '0')

  def giveMeNumber(i: Int): Float =
    if (i == 48) 0.25f else 0.75f


}

