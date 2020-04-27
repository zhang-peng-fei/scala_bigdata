package spark.demo.sparkstreaming

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import spark.SparkHiveExample.Record
import spark.utils.CommUtil

/*

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
//      .config("spark.sql.warehouse.dir", "warehouseLocation")
//      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    //    Logger.getRootLogger.setLevel(Level.WARN)

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
    usersDF.select("name", "favorite_color").write.mode("ignore").save(CommUtil.getResourcePath + "namesAndFavColors.parquet")

    val peopleDF3 = spark.read.format("json").load(peopleJsonPath)
    peopleDF3.select("name", "age").write.mode("ignore").format("parquet").save(CommUtil.dataPath + "namesAndAges.parquet")
    // 也可以使用 SQL 直接查询该文件
    val sqlDF2 = spark.sql("SELECT * FROM parquet.`" + userParquetPath + "`")
    sqlDF2.write.mode("ignore").saveAsTable("sqlDF2")
    // Parquet Files--Loading Data Programmatically
    val peopleDF4 = spark.read.json(peopleJsonPath)

    // DataFrames can be saved as Parquet files, maintaining the schema information
    peopleDF4.select("*").show()
    peopleDF4.write.mode("ignore").parquet(CommUtil.dataPath + "parquet/people.parquet")

    // Read in the parquet file created above
    // Parquet files are self-describing so the schema is preserved
    // The result of loading a Parquet file is also a DataFrame
    val parquetFileDF = spark.read.schema(schema).parquet(CommUtil.dataPath + "parquet/people.parquet/")

    // Parquet files can also be used to create a temporary view and then used in SQL statements
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
//    namesDF.map(attributes => "Name: " + attributes(0)).show()

    // Create a simple DataFrame, store into a partition directory
    val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
    squaresDF.write.mode("ignore").parquet(CommUtil.dataPath + "test_table/key=1/")

    // Create another DataFrame in a new partition directory,
    // adding a new column and dropping an existing column
    val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
    cubesDF.write.mode("ignore").parquet(CommUtil.dataPath + "test_table/key=2/")

    // Read the partitioned table
    val mergedDF = spark.read.option("mergeSchema", "true").schema(schema).parquet(CommUtil.dataPath + "test_table")
    mergedDF.printSchema()
    mergedDF.show()

    // spark is an existing SparkSession
//    spark.catalog.refreshTable("my_table")

    val peopleDF5 = spark.read.json(peopleJsonPath)

    // The inferred schema can be visualized using the printSchema() method
    peopleDF5.printSchema()

    // Creates a temporary view using the DataFrame
    peopleDF5.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by spark
    val teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
    teenagerNamesDF.show()

    // Alternatively, a DataFrame can be created for a JSON dataset represented by
    // an RDD[String] storing one JSON object per string
    val otherPeopleRDD = spark.sparkContext.makeRDD(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val otherPeople = spark.read.json(otherPeopleRDD)
    otherPeople.show()

    //    Hive Tables
    import spark.sql
    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    sql("SELECT * FROM src").show()

    // Aggregation queries are also supported.
    sql("SELECT COUNT(*) FROM src").show()

    // The results of SQL queries are themselves DataFrames and support all normal functions.
    val sqlDF3 = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    // The items in DaraFrames are of type Row, which allows you to access each column by ordinal.
    val stringsDS = sqlDF3.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    stringsDS.show()

    // You can also use DataFrames to create temporary views within a SparkSession.
    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")

    // Queries can then join DataFrame data with data stored in Hive.
    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()


    // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    // Loading data from a JDBC source
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .load()

    val connectionProperties = new Properties()
    connectionProperties.put("user", "username")
    connectionProperties.put("password", "password")
    val jdbcDF2 = spark.read
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

    // Saving data to a JDBC source
    jdbcDF.write
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .save()

    jdbcDF2.write
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)




    // In 1.3.x, in order for the grouping column "department" to show up,
    // it must be included explicitly as part of the agg function call.
    //    df.groupBy("department").agg($"department", max("age"), sum("expense"))

    // In 1.4+, grouping column "department" is included automatically.
    //    df.groupBy("department").agg(max("age"), sum("expense"))

    // Revert to 1.3 behavior (not retaining grouping column) by:
    //    sqlContext.setConf("spark.sql.retainGroupColumns", "false")


  }
}
