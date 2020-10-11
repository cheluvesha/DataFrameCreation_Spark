package com.dataFrame

/***
 * Class extends Enumeration and Declares value type of Custom Exception message
 *
 * @param message = specific custom declared Exceptions
 */
class DataFrameBuilderException(message:DataFrameBuilderExceptionEnum.Value) extends Exception(message.toString) {}

  object DataFrameBuilderExceptionEnum extends Enumeration {

    type  DataFrameBuilderException = Value
    val sparkSqlException: DataFrameBuilderExceptionEnum.Value = Value("Spark SQL Exception!!")
    val avroFileException: DataFrameBuilderExceptionEnum.Value = Value("Avro File Exception!!")
    val sparkException: DataFrameBuilderExceptionEnum.Value = Value("Spark Exception!!")
    val emptyArray: DataFrameBuilderExceptionEnum.Value = Value("List is Empty!!!")

  }

