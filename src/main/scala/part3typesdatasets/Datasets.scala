package part3typesdatasets

import org.apache.spark.sql.functions.{avg, col}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

import java.sql.Date

object Datasets extends App {
 val spark = SparkSession.builder()
   .appName("Datasets")
   .config("spark.master", "local")
   .getOrCreate()

  val numbersDF: DataFrame = spark.read
    .format("csv")
    .option("header", "true")
    .option("interSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  // dataset of a complex type

  // 1 - define your case class
  case class Car(
                Name: String,
                Miles_per_Gallon: Option[Double],
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Option[Long],
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: String,
                Origin: String
                )

  // 2 - read the DF from the file
  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  // 3 define an econder
  import spark.implicits._ // imports all the encoders you will ever one, including   implicit val carEncoder = Encoders.product[Car]
  val carsDF = readDF("cars.json")

 // 4 - convert the DF to DS
  val carsDS = carsDF.as[Car]

  // DS collection functions
  numbersDS.filter(_ < 100)

  // map, flatMap, fold, reduce, for comprehensions ...
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())

  /**
    * Exercises
    *
    * 1. Count how many cars we have
    * 2. Count how many POWERFUL cars we have (HP > 140)
    * 3. Average HP for the entire dataset
    */

  // 1. Count how many cars we have
  val carsCount = carsDS.count
  println(carsCount)

  // 2. Count how many POWERFUL cars we have (HP > 140)


  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count)

  // 3. Average HP for the entire dataset

  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ +_) / carsCount)

  // also use the DF functions!
  carsDS.select(avg(col("Horsepower"))).show

}
