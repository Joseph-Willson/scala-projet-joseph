package fr.mosef.scala.template.processor

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

trait Processor {

  def process(inputDF: DataFrame) : DataFrame

  def countRowsInDataFrame(dataFrame: DataFrame): DataFrame

  def sumColumn(dataFrame: DataFrame, columnName: String): DataFrame

}
