package spark.demo.sparkstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import spark.utils.CommUtil

/**
  * Spark Sql官网样例Demo，DataSet和DataFrame
  */
object DatasetsAndDataFramesDemo1 {

  // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
  // you can use custom classes that implement the Product interface
  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[2]")
      //      .config("spark.some.config.option", "some-value")

      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    // For implicit conversions like converting RDDs to DataFrames(用于隐式转换，如将RDDs转换为DataFrames)
    import spark.implicits._
    val peopleJsonPath = CommUtil.getResourcePath + "people.json"
    val df = spark.read.json(peopleJsonPath)

    // Displays the content of the DataFrame to stdout
    df.show()
    // This import is needed to use the $-notation
    //    import spark.implicits._
    // Print the schema in a tree format
    df.printSchema()

    // Select only the "name" column
    df.select("name").show()

    // Select everybody, but increment the age by 1
    df.select($"name", $"age" + 1).show()

    // Select people older than 21
    df.filter($"age" > 21).show()

    // Count people by age
    df.groupBy("age").count().show()

    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()

    // Register the DataFrame as a global temporary view
    df.createGlobalTempView("people")

    // Global temporary view is tied to a system preserved database `global_temp`
    spark.sql("SELECT * FROM global_temp.people").show()

    // Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.people").show()


    // Encoders are created for case classes
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()

    // Encoders for most common types are automatically provided by importing spark.implicits._
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val peopleDS = spark.read.json(peopleJsonPath).as[Person]
    peopleDS.show()
    val peopleTxtPath = CommUtil.getResourcePath + "people.txt"
    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val peopleDF = spark.sparkContext
      .textFile(peopleTxtPath)
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    // Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by Spark
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

    // The columns of a row in the result can be accessed by field index
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()

    // or by field name
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()

    // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    // Primitive types and case classes can be also defined as
    // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()


    // The schema is encoded in a string
    val schemaString = "name age"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // Create an RDD
    val peopleRDD = spark.sparkContext.textFile(peopleTxtPath)

    // Convert records of the RDD (people) to Rows
    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    // Apply the schema to the RDD
    val peopleDF2 = spark.createDataFrame(rowRDD, schema)

    // Creates a temporary view using the DataFrame
    peopleDF2.createOrReplaceTempView("people2")

    // SQL can be run over a temporary view created using DataFrames
    val results = spark.sql("SELECT name FROM people2")

    // The results of SQL queries are DataFrames and support all the normal RDD operations
    // The columns of a row in the result can be accessed by field index or by field name
    results.map(attributes => "Name: " + attributes(0)).show()
    val userParquetPath = CommUtil.getResourcePath + "users.parquet"
    val usersDF = spark.read.load(userParquetPath)
    usersDF.select("name", "favorite_color").write.save(CommUtil.getResourcePath + "namesAndFavColors.parquet")

    val peopleDF3 = spark.read.format("json").load(peopleJsonPath)
    peopleDF3.select("name", "age").write.format("parquet").save("namesAndAges.parquet")
    // 也可以使用 SQL 直接查询该文件
    val sqlDF2 = spark.sql("SELECT * FROM parquet.'" + userParquetPath + "'")
    sqlDF2.write.mode("overwrite").saveAsTable("sqlDF2")
    // Parquet Files--Loading Data Programmatically
    val peopleDF4 = spark.read.json(peopleJsonPath)

    // DataFrames can be saved as Parquet files, maintaining the schema information
    peopleDF4.write.parquet("people.parquet")

    // Read in the parquet file created above
    // Parquet files are self-describing so the schema is preserved
    // The result of loading a Parquet file is also a DataFrame
    val parquetFileDF = spark.read.parquet("people.parquet")

    // Parquet files can also be used to create a temporary view and then used in SQL statements
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
    namesDF.map(attributes => "Name: " + attributes(0)).show()
  }
}
