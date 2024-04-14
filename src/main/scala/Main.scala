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
    cliArgs(1)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./src/main/ressources/cities.parquet"
    }
  }

  val DST_PATH: String = try {
    cliArgs(2)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./default/output-writer-csv"
    }
  }

  val DST_PATH_PARQUET: String = try {
    cliArgs(2)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./default/output-writer-parquet"
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

  val spark = SparkSession.builder()
    .appName("CreateHiveTable")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  val data = Seq(
    (1, "John", 30),
    (2, "Alice", 25),
    (3, "Bob", 35)
  )

  val df = data.toDF("id", "name", "age")

  val tableName = "my_table"
  val tableLocation = "./src/main/ressources/my_table"


  df.write
    .mode("overwrite")
    .option("header", "true")
    .option("path", tableLocation)
    .format("csv")
    .saveAsTable(tableName)
    .csv(s"$path/lecture.csv")



  val reader: Reader = new ReaderImpl(sparkSession)
  val processor: Processor = new ProcessorImpl()
  val writer: Writer = new Writer()
  val src_path = SRC_PATH
  val src_path_parquet = SRC_PATH_PARQUET
  val dst_path = DST_PATH
  val dst_path_parquet = DST_PATH_PARQUET

  val inputDF = reader.read(src_path)
  inputDF.show(50)
  val inputDFparquet = reader.readParquet(src_path_parquet)
  inputDFparquet.show(50)


  val processedDF = processor.process(inputDF)
  val processedDF_parquet = processor.countRowsInDataFrame(inputDFparquet)


  writer.write(processedDF, "overwrite", dst_path)
  writer.writeParquet(processedDF_parquet, "overwrite", dst_path_parquet)


}