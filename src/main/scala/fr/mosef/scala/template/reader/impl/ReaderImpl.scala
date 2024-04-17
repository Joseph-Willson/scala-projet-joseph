package fr.mosef.scala.template.reader.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.reader.Reader

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.types._

class ReaderImpl(sparkSession: SparkSession, propertiesFilePath: String) extends Reader {

  val properties: Properties = new Properties()
  properties.load(new FileInputStream(propertiesFilePath))

  def read(format: String, options: Map[String, String], path: String): DataFrame = {
    sparkSession
      .read
      .options(options)
      .format(format)
      .load(path)
  }

  def read(path: String): DataFrame = {
    val df = sparkSession
      .read
      .option("sep", properties.getProperty("read_separator"))
      .option("inferSchema", properties.getProperty("schema"))
      .option("header", properties.getProperty("read_header"))
      .format(properties.getProperty("read_format_csv"))
      .load(path)

    // Si aucun en-tête n'est présent, définir un schéma automatiquement
    if (df.columns(0) == "_c0") {
      // Utiliser la première ligne de données pour définir le schéma
      val headerRow = df.head()

      // Créer un schéma à partir du nombre de colonnes dans le DataFrame
      val schema = StructType((0 until headerRow.length).map { index =>
        StructField(s"col${index + 1}", headerRow.schema(index).dataType, nullable = true)
      })

      // Supprimer la première ligne des données
      val data = df.filter(_ != headerRow)

      // Appliquer le schéma défini
      val customDF = sparkSession.createDataFrame(data.rdd, schema)

      // Afficher le schéma du DataFrame
      customDF.printSchema()

      customDF
    } else {

      df
    }
  }

  def readParquet(path: String): DataFrame = {
    sparkSession
      .read
      .format(properties.getProperty("read_format_parquet"))
      .load(path)
  }

  def readTable(tableName: String, location: String): DataFrame = {
    sparkSession
      .read
      .format(properties.getProperty("read_format_parquet"))
      .option("basePath", location)
      .load(location + "/" + tableName)
  }



  def read(): DataFrame = {
    sparkSession.sql("SELECT 'Empty DataFrame for unit testing implementation")
  }

}