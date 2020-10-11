import com.dataFrame.{DataFrameBuilder, DataFrameBuilderExceptionEnum, DataFrameComparison}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

/***
 * Test class extended FunSuite and Dependency is added in Build.sbt
 * Validates each functions
 * imported Spark dependencies
 */

class DataFrameCreationTest extends FunSuite {
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Builder")
    .getOrCreate()
  val CSV = "./src/test/Resources/sample.csv"
  val wrongPathCSV = "./src/test/Resources/Wrong.csv"
  val Json = "./src/test/Resources/sample.json"
  val wrongJson = "./src/test/Resources/wrong.json"
  val userDataParquet = "./src/test/Resources/userdata2.parquet"
  val wrongUserDataParquet = "./src/test/Resources/wrong.parquet"
  val userdataAvro = "./src/test/Resources/userdata2.avro"
  val wrongUserdataAvro = "./src/test/Resources/wrong.avro"
  val compareObj = new DataFrameComparison()
  val creationObj = new DataFrameBuilder()

  test("givenInputCSVFileWhenEqualWithOtherDataFrameShouldReturnTrue") {
    val expected = creationObj.createCSVDataFrame(CSV)
    val actual = spark.read.option("header", "true").option("inferSchema", "true").csv(CSV)
    assert(compareObj.compareDataFrames(expected, actual) === true)
  }

  test("givenInputCSVFileAndCreatingDataFramesWhenValidShouldReturnFalse") {
    val expected = creationObj.createCSVDataFrame(CSV)
    val actual = spark.read.option("header", "true").option("inferSchema", "true").csv(wrongPathCSV)
    assert(compareObj.compareDataFrames(expected, actual) === false)
  }

  test ("givenWrongFilePathShouldThrowAnException") {
    val thrown = intercept[Exception] {
      creationObj.createCSVDataFrame("abc.txt")
    }
    assert(thrown.getMessage === DataFrameBuilderExceptionEnum.sparkSqlException.toString)
  }

  test("givenInputJSONFileWhenEqualWithOtherDataFrameShouldReturnTrue") {
   val expected = creationObj.createJSONDataFrame(Json)
    val actual = spark.read.option("multiline","true").json(Json)
    assert(compareObj.compareDataFrames(expected,actual) === true)
  }

  test("givenInputJsonFileWithWrongFileToCompareEquality") {
    val expected = creationObj.createJSONDataFrame(Json)
    val actual = spark.read.option("multiline","true").json(wrongJson)
    assert(compareObj.compareDataFrames(expected,actual) === false)
  }

  test("givenInputAvroFileWhenEqualWithOtherDataFrameShouldReturnTrue") {
    val expected = spark.read.format("avro").load(userdataAvro)
    val actual = creationObj.createAvroDataFrame(userdataAvro)
    assert(compareObj.compareDataFrames(actual,expected) === true)
  }

  test ("givenWrongAvroFileWhenNotEqualShouldReturnFalse") {
    val expected = spark.read.format("avro").load(wrongUserdataAvro)
    val actual = creationObj.createAvroDataFrame(userdataAvro)
    assert(compareObj.compareDataFrames(actual,expected) === false)
  }

  test("givenInputParquetWhenEqualWithOtherDataFrameShouldReturnTrue") {
    val expected = spark.read.format("parquet").load(userDataParquet)
    val actual = creationObj.createParquetDataFrame(userDataParquet)
    assert(compareObj.compareDataFrames(actual,expected) === true)
  }

  test ("givenWrongParquetFileWhenNotEqualShouldReturnFalse") {
    val expected = spark.read.parquet(wrongUserDataParquet)
    val actual = creationObj.createParquetDataFrame(userDataParquet)
    assert(compareObj.compareDataFrames(actual,expected) === false)
  }

  test("givenRDDAndSchemaToCreateDataFrameWhenCompareWithActualShouldReturnTrue") {
    val listData = List(Row("Vesha","100"),Row("Shiva","101"))
    val column = List("Name","ID")
    val schema = StructType(column
      .map(field => StructField("Name", StringType, nullable = true)))
    val expected = creationObj.createListDataFrame(listData,schema)
    val rdd = spark.sparkContext.parallelize(listData)
    val actual = spark.createDataFrame(rdd,schema)
    assert(compareObj.compareDataFrames(actual,expected))
  }

}

