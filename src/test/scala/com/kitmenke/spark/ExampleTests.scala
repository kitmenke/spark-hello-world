package com.kitmenke.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}
import org.scalatest.FunSuite

case class ExampleData(col_a: Int, col_b: Int, col_c: String)

class ExampleTests extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._

  test("check if column contains a value, if so return value from a different column") {
    // create some sample data to run through our program
    val input = sc.parallelize(
      List[String](
        "{ \"parameter_type\": \"gender\", \"column_match\":[ \"aa\",\"bb\", \"cc\"]}",
        "{ \"parameter_type\": \"bbbbbb\", \"column_match\":[ \"bb\", \"cc\"]}",
        "{ \"parameter_type\": \"aaaaaa\", \"column_match\":[ \"aa\"]}",
      )).toDF("value")
    input.printSchema()
    input.show()
    // define our JSONs schema
    val schema = new StructType()
      .add("parameter_type", StringType, nullable = true)
      .add("column_match", ArrayType(StringType), nullable = true)

    val step1 = input.select(sql.functions.from_json(input("value"), schema).as("parsed_value"))
    step1.show(truncate = false)
    val step2 = step1.withColumn("output", sql.functions.when(sql.functions.array_contains(step1("parsed_value.column_match"), "aa"), step1("parsed_value.parameter_type")).otherwise("not found"))
    step2.show(truncate = false)
  }

  test("should parse dates") {
    // create some sample data to run through our program
    val df = sc.parallelize(List[String]("20200401", "20200501", "20200601"))
      .toDF("dates")
    // using the to_date spark sql function, convert the string value into a
    import org.apache.spark.sql.functions.to_date
    val result = df.select(to_date(df("dates"), "yyyyMMdd"))
    result.printSchema()
    result.show()
  }

  test("can create a dataframe using a case class instead of using a schema") {
    // note: case class must be defined OUTSIDE of this test and test class otherwise
    // you will have issues with serialization
    val rdd = sc.parallelize(Seq(
      ExampleData(1, 2, "cat"),
      ExampleData(2, 4, "dog"),
      ExampleData(3, 6, "elephant")
    ))
    val df = sqlContext.createDataFrame(rdd)
    df.printSchema()
    df.show()
  }

  test("should filter with multiple conditions") {
    val schema = new StructType()
      .add("col_a", IntegerType, nullable = true)
      .add("col_b", IntegerType, nullable = true)
      .add("col_c", StringType, nullable = true)
    // create some sample data to run through our program
    val rdd = sc.parallelize(Seq(
      Row(1, 2, "cat"),
      Row(2, 4, "dog"),
      Row(3, 6, "elephant")
    ))
    val df = sqlContext.createDataFrame(rdd, schema)
    df.printSchema()
    val result = df.filter(df("col_a") >= 2 && df("col_b") <= 4)
    result.show()
  }

  test("should drop duplicates") {
    val schema = new StructType()
      .add("row", StringType, nullable = false)
      .add("code", StringType, nullable = false)
    // create some sample data to run through our program
    val rdd = sc.parallelize(Seq(
      Row("row1", "XFH"),
      Row("row2", "ABC"),
      Row("row3", "XFH")
    ))
    val df = sqlContext.createDataFrame(rdd, schema)
    df.printSchema()
    println(df.columns.toList)


    df.show()
    val result = df.dropDuplicates("code")
    result.printSchema()
    result.show()
  }

  test("should join two dataframes") {
    val schema1 = new StructType()
      .add("ColumnA", StringType, nullable = false)
      .add("ColumnB", StringType, nullable = false)
    // create some sample data to run through our program
    val rdd1 = sc.parallelize(Seq(
      Row("a1", "b1"),
      Row("a2", "b2"),
      Row("a3", "b3")
    ))
    val df1 = sqlContext.createDataFrame(rdd1, schema1)
    val schema2 = new StructType()
      .add("ColumnA", StringType, nullable = false)
      .add("ColumnC", StringType, nullable = false)
    val rdd2 = sc.parallelize(Seq(
      Row("a1", "c1"),
      Row("a2", "c2"),
      Row("a4", "c3"),
    ))
    val df2 = sqlContext.createDataFrame(rdd2, schema2)

    val result = df1.join(df2, df1("ColumnA") === df2("ColumnA"), "outer")
    result.printSchema()
    result.show()
  }

  // https://stackoverflow.com/questions/58310088/apache-spark-scala-how-do-i-grab-a-single-element-and-sub-elements-from-a-jso/
  test("select nested json") {
    val events_sep22 = spark.read.json("src/test/resources/data.json")
    events_sep22.printSchema()
    events_sep22.show()
    val df = events_sep22.select("data.*")
    df.printSchema()
    df.show()
  }

  test("udf question") {
    val upper = (s: String) => {
      s.toUpperCase
    }: String
    val upperUDF: UserDefinedFunction = spark.udf.register("upper", upper)
    def extractNames(schema: StructType): Seq[String] = {
      schema.fields.flatMap { field =>
        field.dataType match {
          case structType: StructType =>
            extractNames(structType).map(field.name + "." + _)
          case _: StringType =>
            field.name :: Nil
          case s: ArrayType if (s.elementType == StringType) =>
            field.name + "." + "element" :: Nil
          case _ =>
            Nil
        }
      }
    }

    val schema = new StructType()
      .add("row", StringType, nullable = false)
      .add("codes", ArrayType(StringType), nullable = false)
    // create some sample data to run through our program
    val rdd = sc.parallelize(Seq(
      Row("row1", Array("XFH", "XAA", "XBB")),
      Row("row2", Array("AAA", "aaa", "abb")),
      Row("row3", Array("caa", "cbb", "ccc"))
    ))
    val df = sqlContext.createDataFrame(rdd, schema)

    val names = extractNames(df.schema)
    println(names)
    val result = names
      .foldLeft(df)({ (memoDF, colName) =>
        memoDF.withColumn(colName, upperUDF(col(colName)))
      })

    result.printSchema()
    result.show()
  }
}
