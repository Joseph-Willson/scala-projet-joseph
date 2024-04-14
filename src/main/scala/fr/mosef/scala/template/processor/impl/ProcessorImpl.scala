package fr.mosef.scala.template.processor.impl


import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class ProcessorImpl() extends Processor {

  def process(inputDF: DataFrame): DataFrame = {
    inputDF.groupBy("industry").sum("value")
  }

  def countRowsInDataFrame(dataFrame: DataFrame): DataFrame = {
    val rowCount = dataFrame.count()
    val spark = dataFrame.sparkSession
    import spark.implicits._
    val countDF = Seq(rowCount).toDF("rowCount")
    countDF
  }

}
