package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, max}

object Joins extends App {
  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // joins

  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsBF = guitaristsDF.join(bandsDF, joinCondition, "inner")

  // out joins
  // left outer = everything in the inner join + all the rows in the LEFT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")

  // right outer = everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")

  // outer join = everything in the inner join + all the rows in BOTH tables, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  // semi-joins - left rows where there is a matching right row, only showing left columns
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")

  // anti-joins - lefts rows where there is NOT a matching right, only showing left columns
  guitaristsDF.join(bandsDF, joinCondition, "left_anti").show

  // things to keep in mind
  //  guitaristsBandsBF.select("id", "band").show // will crash

  // option 1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dupe column
  guitaristsBandsBF.drop(bandsDF.col("id"))

  // option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

  // using complex types
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))

  /**
    * Exercises
    *
    * - show all employees and their max salary
    * - show all employees who were never managers
    * - find the job titles of the best paid 10 employees
    */

  // Exercise 1: Show all employees and their max salary
  // join salaries to employees by employee number, left join, delete join duped row
  // group by employee number

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()

  val salariesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.salaries")
    .load()


  val salariesWithEmployeeInfoJC = salariesDF.col("emp_no") === employeesDF.col("emp_no")
  val salariesWithEmployeeInfoDF = salariesDF
    .join(employeesDF, salariesWithEmployeeInfoJC, "left_outer")
    .drop(employeesDF.col("emp_no"))
  salariesWithEmployeeInfoDF.show

  val employeeMaxSalaryDF = salariesWithEmployeeInfoDF
    .groupBy(col("emp_no"))
    .max("salary")
    .orderBy(col("max(salary)").desc)

  employeeMaxSalaryDF.show

  // alternative:
  val maxSalariesPerEmpNoDF = salariesDF.groupBy("emp_no").agg(max("salary").as("maxSalary"))
  val employeesSalariesDF = employeesDF.join(maxSalariesPerEmpNoDF, "emp_no")

  // Exercise 2: Show all employees who were never managers
  // join employees to dept_manager using anti-join

  val deptManagersDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.dept_manager")
    .load()

  val employeesNotManagersJC = employeesDF.col("emp_no") === deptManagersDF.col("emp_no")
  val employeesNotManagersDF = employeesDF.join(deptManagersDF, employeesNotManagersJC, "left_anti")

  // Exercise 3: Find the job titles of the best paid 10 employees
  // join titles with salaries on emp_no and from_date
  // order descending

  val titlesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.titles")
    .load()


  val titlesWithSalariesJC =
    titlesDF.col("emp_no") === salariesDF.col("emp_no") &&
      titlesDF.col("from_date") === salariesDF.col("from_date")
  val titlesWithSalariesDF = titlesDF
    .join(salariesDF, titlesWithSalariesJC)
    .drop(salariesDF.col("emp_no"))
    .drop(salariesDF.col("from_date"))
    .drop(salariesDF.col("to_date"))
    .orderBy(col("salary").desc)

  titlesWithSalariesDF.show

  //alternative
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDF = employeesSalariesDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitlesDF, "emp_no")

  bestPaidJobsDF.show
}
