package fr.mosef.scala.template.writer

import org.apache.spark.sql.DataFrame
class Writer {

  def write(df: DataFrame, mode: String = "overwrite", path: String): Unit = {
    df
      .write
      .option("header", "true")
      .mode(mode)
      .csv(s"$path/lecture.csv")
  }

  def writeParquet(df: DataFrame, mode: String = "overwrite", path: String): Unit = {
    df.write
      .mode(mode)
      .parquet(s"$path/nb_ligne.parquet")
  }

}
