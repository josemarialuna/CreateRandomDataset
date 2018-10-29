package es.us.randomDataset

import org.apache.spark.SparkContext

import scala.util.Random

/**
  * Created by Josem on 13/06/2017.
  */
object RandomDataset {
  /**
    * It generates and saves into disk a dataset with the given configuration.
    *
    * @param sc SparkContext from the MainClass.
    * @param features The number of features (columns) of the instances.
    * @param clusters The number of clusters of the dataset.
    * @param instances The number of instances per cluster.
    * @param standDev The standard deviation of the gaussian distribution.
    * @param path The path into with the dataset is going to be saved.
    * @param withClass True if the instances include the class.
    * @example createFile(sc, 2, 4, 1000, 0.05, "", true)
    */
  def createFile(sc: SparkContext, features: Int, clusters: Int, instances: Int, standDev: Float, path: String, withClass: Boolean) = {

    println("*******************************")
    println("*******DATASET GENERATOR*******")
    println("*******************************")
    println("Configuration:")
    println("\tClusters: " + clusters)
    println("\tFeatures: " + features)
    println("\tInstances: " + instances)
    println("\tStdev: " + standDev)
    println("\tSave directory: " + path)
    println("Running...\n")


    val valor = scala.math.pow(2, features).toInt
    var marcados = 0
    val marcadosArray = Array.fill(valor) {
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
    val rdd_binarySelected = sc.parallelize(indicesSelected.map(x => (toBinary(x, features), x))).map(_.swap)
    //println(binarySelected.mkString(";"))

    val rdd_indices = rdd_binarySelected
      .mapValues(x => x.toArray.map(giveMeNumber(_)))

    val dataset = rdd_indices.flatMapValues(x =>
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

    println("Dataset generation finished with: " + dataset.count() + " instances.")

    println("Saving dataset into disk..")

    if (withClass) {
      dataset.map(x => x._1 + "," + x._2.toString.replace("(", "").replace(")", ""))
        .coalesce(1, shuffle = true)
        .saveAsTextFile(s"$path\\C$clusters-D$features-I$instances-${Utils.whatTimeIsIt()}")
    } else {
      dataset.map(x => x._2.toString.replace("(", "").replace(")", ""))
        .coalesce(1, shuffle = true)
        .saveAsTextFile(s"$path\\C$clusters-D$features-I$instances-${Utils.whatTimeIsIt()}")
    }

  }

  /**
    * It generates and saves into disk a dataset with the given configuration.
    *
    * @param sc SparkContext from the MainClass.
    * @param clusters The number of clusters of the dataset.
    * @param instances The number of instances per cluster.
    * @param path The path into with the dataset is going to be saved.
    * @example createFileWithClass(sc, 3, 43, 1000, "")
    */
  def createFileWithClass(sc: SparkContext, classes: Int, clusters: Int, instances: Int, path: String) = {

    println("*******************************")
    println("*******DATASET GENERATOR*******")
    println("*******************************")
    println("Configuration:")
    println("\tClusters: " + clusters)
    println("\tInstances: " + instances)
    println("\tClasses: " + classes)
    println("\tSave directory: " + path)
    println("Running...\n")




  }

  /**
    * It generates a random Float number in the input range
    *
    * @param from The start point
    * @param to The last point
    * @example getRandom(0, 10)
    */
  def getRandom(from: Integer, to: Integer): Float = {
    val rnd = new Random()
    rnd.nextInt((to - from) + 1) + from
  }

  /**
    * It generates a random number in a gaussian distribution with the given mean and standard deviation
    *
    * @param average The start point
    * @param desv The last point
    * @example getGaussian(0.5, 0.05)
    */
  def getGaussian(average: Float, desv: Float): Float = {
    val rnd = new Random()
    rnd.nextGaussian().toFloat * desv + average
  }

  /**
    * Return a String with the binarization of a given number
    *
    * @param number The number to be binarized.
    * @param digits The number of digits to express the binary number.
    * @example toBinary(4, 3)
    */
  def toBinary(number: Int, digits: Int = 8) =
    String.format("%" + digits + "s", number.toBinaryString).replace(' ', '0')


  /**
    * Return a 0.25 if a zero is given or 0.75 in other case
    *
    * @param number The input number
    * @example giveMeNumber(1)
    */
  def giveMeNumber(number: Int): Float =
    if (number == 48) 0.25f else 0.75f


}
