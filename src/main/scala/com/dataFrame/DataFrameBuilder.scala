package com.dataFrame

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/***
 * Dependencies used : Spark core , External Avro library
 * Class creates DataFrames and prints the schema
 */
class DataFrameBuilder {
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Builder")
    .getOrCreate()

  /***
   * Creates DataFrames and calls the printSchema function and if something goes wrong it triggers Declared Exceptions.
   * @param choice = choice to create DataFrames Between csv, json, avro etc
   * @param filepath = path for the respective files
   * @return DataFrame
   */
  def createDataFrame(choice:Int, filepath:String):DataFrame = {
    try {
      val readDataFrame = choice match {
        case 1 => spark.read.option("header", "true").option("inferSchema","true").csv(filepath)
        case 2 => spark.read.option("multiline","true").json(filepath)
        case 3 => spark.read.parquet(filepath)
        case 4 => spark.read.format("avro").load(filepath)
      }
      printSchema(readDataFrame)
      readDataFrame
    }
    catch {
      case _ : org.apache.spark.sql.AnalysisException =>
        throw new DataFrameBuilderException(DataFrameBuilderExceptionEnum.sparkSqlException)
      case _ : org.apache.avro.AvroRuntimeException =>
        throw new DataFrameBuilderException(DataFrameBuilderExceptionEnum.avroFileException)
      case _ : org.apache.spark.SparkException =>
        throw new DataFrameBuilderException(DataFrameBuilderExceptionEnum.sparkException)
    }
  }

  // prints the schema wrt to DataFrame
  def printSchema(readDataFrame: DataFrame):Unit  = {
    readDataFrame.show()
  }

  // function passes filepath to create DataFrame from CSV
  def createCSVDataFrame(filepath: String):DataFrame = {
    createDataFrame(1,filepath)
  }

  // function passes filepath to create DataFrame from JSON
  def createJSONDataFrame(filepath: String):DataFrame = {
    createDataFrame(2,filepath)
  }

  // function passes filepath to create DataFrame from Parquet
  def createParquetDataFrame(filepath: String):DataFrame = {
    createDataFrame(3,filepath)
  }

  // function passes filepath to create DataFrame from Avro
  def createAvroDataFrame(filepath:String):DataFrame = {
    createDataFrame(4,filepath)
  }

  /***
   * Creates DataFrame from List and if anything goes wrong it triggers an Exception
   * @param inputArray = List type data structure
   * @param schema = provides column type using StructType
   * @return DataFrame
   */
  def createListDataFrame(inputArray:List[Row],schema:StructType):DataFrame = {
    try {
      if(inputArray.isEmpty) {
        throw new DataFrameBuilderException(DataFrameBuilderExceptionEnum.emptyArray)
      }

      val rdd = spark.sparkContext.parallelize(inputArray)
      val dataFrame = spark.createDataFrame(rdd,schema)
      printSchema(dataFrame)
      dataFrame
    }
    catch {
      case  _ : NullPointerException => throw new DataFrameBuilderException(DataFrameBuilderExceptionEnum.emptyArray)
      case _ : org.apache.spark.SparkException =>
        throw new DataFrameBuilderException(DataFrameBuilderExceptionEnum.sparkException)
    }
  }
}

