package fr.mosef.scala.template

import fr.mosef.scala.template.job.Job
import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.impl.ReaderImpl
import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.writer.Writer
import org.apache.spark.SparkConf
import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode


object Main extends App with Job {


  val cliArgs = args
  val MASTER_URL: String = try {
    cliArgs(0)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => "local[1]"
  }



  val SRC_PATH: String = try {
    cliArgs(1)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./src/main/ressources/data.csv"
    }
  }

  val SRC_PATH_PARQUET: String = try {
    cliArgs(2)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./src/main/ressources/cities.parquet"
    }
  }




  val DST_PATH: String = try {
    cliArgs(3)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./default/output-writer-csv"
    }
  }

  val DST_PATH_PARQUET: String = try {
    cliArgs(4)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./default/output-writer-parquet"
    }
  }

  val path_properties: String = try {
    cliArgs(5)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./src/main/ressources/application.properties"
    }
  }

  val conf: SparkConf = new SparkConf()
  conf.set("spark.testing.memory", "471859200")

  val sparkSession = SparkSession
    .builder
    .master(MASTER_URL)
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  sparkSession.sparkContext.hadoopConfiguration.setClass("fs.file.impl",  classOf[ BareLocalFileSystem], classOf[FileSystem])



  val reader: Reader = new ReaderImpl(sparkSession, path_properties)
  val processor: Processor = new ProcessorImpl()
  val writer: Writer = new Writer(path_properties)

  val src_path = SRC_PATH
  val src_path_parquet = SRC_PATH_PARQUET


  val dst_path = DST_PATH
  val dst_path_parquet = DST_PATH_PARQUET

  // lecture du CSV et Parquet
  val inputDF = reader.read(src_path)
  inputDF.show(50)
  val inputDFparquet = reader.readParquet(src_path_parquet)
  inputDFparquet.show(50)


  // Process CSV et Parquet (grouby et compte le nombre de ligne)
  val val1 = "industry"
  val val2 = "value"
  val processedDF = processor.process(inputDF, val1, val2)
  val processedDF_parquet = processor.countRowsInDataFrame(inputDFparquet)

  // Charge dans les locations respectives -> dst_path pour CSV et dst_path_parquet pour parquet
  writer.write(processedDF, "overwrite", dst_path)
  writer.writeParquet(processedDF_parquet, "overwrite", dst_path_parquet)


  // Création de la table Hive
  // récupère le CSV Proccessé et le transforme en table
  val tableName = "my_table"
  val tableLocation = "./src/main/ressources"
  processedDF.write
    .mode(SaveMode.Overwrite)
    .option("path", tableLocation)
    .saveAsTable(tableName)

  // lecture de la table
  val hiveTableName = "my_table"
  val hiveTableLocation = "./src/main/ressources"
  val hiveTableDF = reader.readTable(hiveTableName, hiveTableLocation)
  hiveTableDF.show(50)

  // Somme la colonne sum(value)
  val columnName = "sum(value)"
  val processedDF_hive = processor.sumColumn(hiveTableDF, columnName)

  // charge dans output-writer-hive
  val tableNametoload = "table_loaded"
  val tablePath = "./default/output-writer-hive"
  writer.writeTable(processedDF_hive, tableNametoload, tablePath = tablePath)


}