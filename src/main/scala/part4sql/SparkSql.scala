package part4sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SparkSql extends App {

  val spark = SparkSession.builder()
    .appName("Spark SQL PRactice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // regular DF API
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  // use Spark SQL
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin
  )

  // we can run ANY SQL statement
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  val databasesDF = spark.sql("show databases")

  databasesDF.show()

  // transfer tables from a DB to Spark
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false) = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)

    if (shouldWriteToWarehouse) {
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }

  transferTables(List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager")
  )

  // read DF from loaded Spark tables
  val employeesDF2 = spark.read.table("employees")

  /**
    * Exercises
    *
    * 1. Read the movies DF and store it as a Spark table in the rtjvm database.
    * 2. Count how many employees we re hired between Jan 1 2000 and Jan 1 2001.
    * 3. Show the average salaries for the employees hired in between those dates, grouped by department.
    * 4. Show the name of the best-paying department for employees hired in between those dates.
    */

  // for exercises 2 - 4
//  spark.sql(
//    """
//      |select *
//      |from employees e, dept_emp d
//      |where e.emp_no = d.emp_no
//      |""".stripMargin
//  )

  // Read the movies DF and store it as a Spark table in the rtjvm database.

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

//  moviesDF.createOrReplaceTempView("movies")
//  moviesDF.write
//    .mode(SaveMode.Overwrite)
//    .saveAsTable("movies")

//  val moviesTableDF = spark.read.table("movies")
//
//  moviesTableDF.show()

  // Count how many employees we re hired between Jan 1 2000 and Jan 1 2001.

  spark.sql(
    """
      |select count(*)
      |from employees
      |where hire_date > '1997-01-01' and hire_date < '2000-01-01'
      |""".stripMargin
  )

  // Show the average salaries for the employees
  // hired in between those dates,
  // grouped by department.

  spark.sql(
    """
      |select de.dept_no, avg(s.salary)
      |from employees e, dept_emp de, salaries s
      |where e.hire_date > '1997-01-01' and e.hire_date < '2000-01-01'
      | and e.emp_no = de.emp_no
      | and e.emp_no = s.emp_no
      |group by de.dept_no
      |""".stripMargin
  )

  // Show the name of the best-paying department
  // for employees hired in between those dates.

  spark.sql(
    """
      |select avg(s.salary) payments, d.dept_name
      |from employees e, dept_emp de, salaries s, departments d
      |where e.hire_date > '1997-01-01' and e.hire_date < '2000-01-01'
      | and e.emp_no = de.emp_no
      | and e.emp_no = s.emp_no
      | and de.dept_no = d.dept_no
      |group by d.dept_name
      |order by payments desc
      |limit 1
      |""".stripMargin
  ).show()

}
