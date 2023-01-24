package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {
  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // counting all non-null
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))) // all the values except null
  moviesDF.selectExpr("count(Major_genre)")
  genresCountDF.show()


  // counting all
  moviesDF.select(count("*")).show()

  // counting distinct
  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  // approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre")))

  //  min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)")

  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show()

  // Grouping

  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .count() // equivalent to SQL "select count(*) from moviesDF group by Major_Genre"

  countByGenreDF.show()

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

  aggregationsByGenreDF.show()




  /**
    * Exercises
    * 1. Sum up ALL the profits of ALL the movies in the DF
    */

  val moviesColsDF= moviesDF.select(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_DVD_Sales",
    "IMDB_Rating",
    "Director"
  ).na.fill(0)

  val moviesWithTotalProfit = moviesColsDF.withColumn(
    "Total_Profit",
    col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")
  )

  moviesWithTotalProfit.select(sum(col("Total_Profit"))).show()


  /**
    * 2. Count how many distinct directors we have
    */

  val numberOfDirectorsDF = moviesDF.select(
    countDistinct(col("Director"))
  )

  numberOfDirectorsDF.show()


  /**
    * 3. Show the mean and standard deviation of US gross revenue for the movies
    */

  moviesWithTotalProfit.select(
    mean(col("Total_Profit")),
    stddev(col("Total_Profit"))
  ).show()


  /**
    * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
    */

  val aggregationByDirectorDF = moviesWithTotalProfit.groupBy(col("Director"))
    .agg(
      avg("IMDB_Rating").as("Avg_IMDB_Rating"),
      avg("Total_Profit").as("Avg_Total_Gross")
    ).orderBy(col("Avg_IMDB_Rating").desc)

  aggregationByDirectorDF.show()
}
