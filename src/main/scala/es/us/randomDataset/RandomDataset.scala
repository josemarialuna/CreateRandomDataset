package es.us.randomDataset

import org.apache.spark.SparkContext

import scala.util.Random

/**
  * Created by Josem on 13/06/2017.
  */
object RandomDataset {
  def createFile(sc: SparkContext, features: Int, clusters: Int, instances: Int, standDev: Float, path: String) = {


    val valor = scala.math.pow(2, features).toInt
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
    val binarySelected = indicesSelected.map(toBinary(_, features))
    println(binarySelected.mkString(";"))

    val indices = binarySelected
      .map(x => x.toArray.map(giveMeNumber(_)))

    val indicesRDD = sc.parallelize(indices).cache()

    val dataset = indicesRDD.flatMap(x =>
      for {i <- 0 until instances} yield {
        features match {
          case 2 => (getGaussian(x(0), standDev), getGaussian(x(1), standDev))
          case 3 => (getGaussian(x(0), standDev), getGaussian(x(1), standDev), getGaussian(x(2), standDev))
          case 4 => (getGaussian(x(0), standDev), getGaussian(x(1), standDev), getGaussian(x(2), standDev), getGaussian(x(3), standDev))
          case 5 => (getGaussian(x(0), standDev), getGaussian(x(1), standDev), getGaussian(x(2), standDev), getGaussian(x(3), standDev), getGaussian(x(4), standDev))
          case 6 => (getGaussian(x(0), standDev), getGaussian(x(1), standDev), getGaussian(x(2), standDev), getGaussian(x(3), standDev), getGaussian(x(4), standDev), getGaussian(x(5), standDev))
          case 7 => (getGaussian(x(0), standDev), getGaussian(x(1), standDev), getGaussian(x(2), standDev), getGaussian(x(3), standDev), getGaussian(x(4), standDev), getGaussian(x(5), standDev), getGaussian(x(6), standDev))
          case 8 => (getGaussian(x(0), standDev), getGaussian(x(1), standDev), getGaussian(x(2), standDev), getGaussian(x(3), standDev), getGaussian(x(4), standDev), getGaussian(x(5), standDev), getGaussian(x(6), standDev), getGaussian(x(7), standDev))
          case 9 => (getGaussian(x(0), standDev), getGaussian(x(1), standDev), getGaussian(x(2), standDev), getGaussian(x(3), standDev), getGaussian(x(4), standDev), getGaussian(x(5), standDev), getGaussian(x(6), standDev), getGaussian(x(7), standDev), getGaussian(x(8), standDev))
          case 10 => (getGaussian(x(0), standDev), getGaussian(x(1), standDev), getGaussian(x(2), standDev), getGaussian(x(3), standDev), getGaussian(x(4), standDev), getGaussian(x(5), standDev), getGaussian(x(6), standDev), getGaussian(x(7), standDev), getGaussian(x(8), standDev), getGaussian(x(9), standDev))
          case 11 => (getGaussian(x(0), standDev), getGaussian(x(1), standDev), getGaussian(x(2), standDev), getGaussian(x(3), standDev), getGaussian(x(4), standDev), getGaussian(x(5), standDev), getGaussian(x(6), standDev), getGaussian(x(7), standDev), getGaussian(x(8), standDev), getGaussian(x(9), standDev), getGaussian(x(10), standDev))
          case 12 => (getGaussian(x(0), standDev), getGaussian(x(1), standDev), getGaussian(x(2), standDev), getGaussian(x(3), standDev), getGaussian(x(4), standDev), getGaussian(x(5), standDev), getGaussian(x(6), standDev), getGaussian(x(7), standDev), getGaussian(x(8), standDev), getGaussian(x(9), standDev), getGaussian(x(10), standDev), getGaussian(x(11), standDev))
          case 13 => (getGaussian(x(0), standDev), getGaussian(x(1), standDev), getGaussian(x(2), standDev), getGaussian(x(3), standDev), getGaussian(x(4), standDev), getGaussian(x(5), standDev), getGaussian(x(6), standDev), getGaussian(x(7), standDev), getGaussian(x(8), standDev), getGaussian(x(9), standDev), getGaussian(x(10), standDev), getGaussian(x(11), standDev), getGaussian(x(12), standDev))
          case 14 => (getGaussian(x(0), standDev), getGaussian(x(1), standDev), getGaussian(x(2), standDev), getGaussian(x(3), standDev), getGaussian(x(4), standDev), getGaussian(x(5), standDev), getGaussian(x(6), standDev), getGaussian(x(7), standDev), getGaussian(x(8), standDev), getGaussian(x(9), standDev), getGaussian(x(10), standDev), getGaussian(x(11), standDev), getGaussian(x(12), standDev), getGaussian(x(13), standDev))
          case 15 => (getGaussian(x(0), standDev), getGaussian(x(1), standDev), getGaussian(x(2), standDev), getGaussian(x(3), standDev), getGaussian(x(4), standDev), getGaussian(x(5), standDev), getGaussian(x(6), standDev), getGaussian(x(7), standDev), getGaussian(x(8), standDev), getGaussian(x(9), standDev), getGaussian(x(10), standDev), getGaussian(x(11), standDev), getGaussian(x(12), standDev), getGaussian(x(13), standDev), getGaussian(x(14), standDev))
          case 16 => (getGaussian(x(0), standDev), getGaussian(x(1), standDev), getGaussian(x(2), standDev), getGaussian(x(3), standDev), getGaussian(x(4), standDev), getGaussian(x(5), standDev), getGaussian(x(6), standDev), getGaussian(x(7), standDev), getGaussian(x(8), standDev), getGaussian(x(9), standDev), getGaussian(x(10), standDev), getGaussian(x(11), standDev), getGaussian(x(12), standDev), getGaussian(x(13), standDev), getGaussian(x(14), standDev), getGaussian(x(15), standDev))
          case 17 => (getGaussian(x(0), standDev), getGaussian(x(1), standDev), getGaussian(x(2), standDev), getGaussian(x(3), standDev), getGaussian(x(4), standDev), getGaussian(x(5), standDev), getGaussian(x(6), standDev), getGaussian(x(7), standDev), getGaussian(x(8), standDev), getGaussian(x(9), standDev), getGaussian(x(10), standDev), getGaussian(x(11), standDev), getGaussian(x(12), standDev), getGaussian(x(13), standDev), getGaussian(x(14), standDev), getGaussian(x(15), standDev), getGaussian(x(16), standDev))
          case 18 => (getGaussian(x(0), standDev), getGaussian(x(1), standDev), getGaussian(x(2), standDev), getGaussian(x(3), standDev), getGaussian(x(4), standDev), getGaussian(x(5), standDev), getGaussian(x(6), standDev), getGaussian(x(7), standDev), getGaussian(x(8), standDev), getGaussian(x(9), standDev), getGaussian(x(10), standDev), getGaussian(x(11), standDev), getGaussian(x(12), standDev), getGaussian(x(13), standDev), getGaussian(x(14), standDev), getGaussian(x(15), standDev), getGaussian(x(16), standDev), getGaussian(x(17), standDev))
          case 19 => (getGaussian(x(0), standDev), getGaussian(x(1), standDev), getGaussian(x(2), standDev), getGaussian(x(3), standDev), getGaussian(x(4), standDev), getGaussian(x(5), standDev), getGaussian(x(6), standDev), getGaussian(x(7), standDev), getGaussian(x(8), standDev), getGaussian(x(9), standDev), getGaussian(x(10), standDev), getGaussian(x(11), standDev), getGaussian(x(12), standDev), getGaussian(x(13), standDev), getGaussian(x(14), standDev), getGaussian(x(15), standDev), getGaussian(x(16), standDev), getGaussian(x(17), standDev), getGaussian(x(18), standDev))
          case 20 => new Dim20(getGaussian(x(0), standDev), getGaussian(x(1), standDev), getGaussian(x(2), standDev), getGaussian(x(3), standDev), getGaussian(x(4), standDev), getGaussian(x(5), standDev), getGaussian(x(6), standDev), getGaussian(x(7), standDev), getGaussian(x(8), standDev), getGaussian(x(9), standDev), getGaussian(x(10), standDev), getGaussian(x(11), standDev), getGaussian(x(12), standDev), getGaussian(x(13), standDev), getGaussian(x(14), standDev), getGaussian(x(15), standDev), getGaussian(x(16), standDev), getGaussian(x(17), standDev), getGaussian(x(18), standDev), getGaussian(x(19), standDev))
        }
      }
    )

    println(dataset.count())

    dataset.map(_.toString).coalesce(1).saveAsTextFile(s"C$clusters-D$features-I$instances-${Utils.whatTimeIsIt()}")


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
