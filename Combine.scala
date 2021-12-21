package CombineCsv

import com.cibc.fctp.ita.utils.SparkApp
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class Combined
(
  sourceIP: String,
  environment: String
)

object CombinedCsv extends SparkApp {
  def main(args: Array[String]): Unit = {
    implicit val (spark, environment) = prepareEnvironment(args)
    import spark.implicits._

    val path = environment.fileRoot

    val df = spark.read.option("header", "true").csv(path + "/*.csv")
      .withColumn("fileName", input_file_name())

    val result = transform(path)

    result.write.mode("overwrite").csv(path + "/Combined.csv")
  }

  def transform (df: DataFrame): Dataset[Combined] = {
    import df.sparkSession.implicits._

    df
      .where(!'fileName.contains("Combined"))
      .withColumn("Environment", 'fileName.substr('fileName.toString.lastIndexOf("/"), 'fileName.toString.lastIndexOf(".")))
      .withColumn("Environment", regexp_replace('fileName, " [0-9]", ""))
      .select(df("Source IP"), 'Environment).distinct
      .cast[Combined]
  }
}
