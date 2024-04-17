package fr.mosef.scala.template.writer

import org.apache.spark.sql.DataFrame

import java.io.FileInputStream
import java.util.Properties
class Writer(propertiesFilePath: String) {

  val properties: Properties = new Properties()
  properties.load(new FileInputStream(propertiesFilePath))

  def write(df: DataFrame, mode: String = "overwrite", path: String): Unit = {
    df
      .write
      .option("header", properties.getProperty("write_header"))
      .mode(mode)
      .csv(path)
  }

  def writeParquet(df: DataFrame, mode: String = "overwrite", path: String): Unit = {
    df.write
      .mode(mode)
      .parquet(path)
  }

  def writeTable(df: DataFrame, tableName: String, mode: String = "overwrite", tablePath: String): Unit = {
    df.write
      .mode(mode)
      .option("path", tablePath)
      .saveAsTable(tableName)
  }

}
