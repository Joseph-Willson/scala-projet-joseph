package fr.mosef.scala.template.processor.impl


import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.lit

class ProcessorImpl() extends Processor {

  def process(inputDF: DataFrame, val1: String, val2: String): DataFrame = {
    inputDF.groupBy(val1).sum(val2)
  }

  def countRowsInDataFrame(dataFrame: DataFrame): DataFrame = {
    val rowCount = dataFrame.count()
    val spark = dataFrame.sparkSession
    import spark.implicits._
    val countDF = Seq(rowCount).toDF("rowCount")
    countDF
  }

  def sumColumn(dataFrame: DataFrame, columnName: String): DataFrame = {
    val sumResult = dataFrame.agg(functions.sum(columnName)).toDF("sumResult")
    sumResult
  }

}
