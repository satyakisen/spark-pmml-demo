package org.ssen

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.{Dataset, Row}
import org.jpmml.sparkml.PMMLBuilder
import org.ssen.utils.SparkSessionBuilder

import java.io.File
import scala.util.{Failure, Success, Try}

object Driver {
  def main(args: Array[String]): Unit = {

    val sparkConfigurations: Map[String, String] = Try(args(0)) match {
      case Success(value) => fetchConfiguration(value)
      case Failure(_) => devConfiguration()
    }

    val spark = SparkSessionBuilder()
      .configs(sparkConfigurations)
      .getOrCreate()

    val rawDf = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv("/Users/satyakisen/Downloads/iris/Iris.csv")

    val schema = rawDf.schema

    val split = rawDf.randomSplit(Array(0.8, 0.2))

    val trainDf = split(0)

    val model = initializeTraining(trainDf)

    new PMMLBuilder(schema, model).buildFile(new File("/Users/satyakisen/Downloads/pipeline.pmml"))
    spark.stop()
  }

  private[this] def fetchConfiguration(config: String) : Map[String, String] =  {
    val conf = s"Parse Config: $config"
    print(conf)
    Map("spark.app.name" -> "Prod App")
  }

  private[this] def devConfiguration() : Map[String, String] = Map(
    "spark.app.name" -> "DevApp",
    "spark.master" -> "local[*]"
  )

  private[this] def initializeTraining(value: Dataset[Row]) : PipelineModel = {
    val labelCol = "Species"
    val featuresCol = value.columns.filter(column => !column.equals(labelCol) && !column.equals("Id"))
    print(featuresCol.mkString(","))
    val labelIndexer = new StringIndexer().setInputCol(labelCol).setOutputCol("class")
    val vectorAssembler = new VectorAssembler().setInputCols(featuresCol).setOutputCol("features")
    val lr = new LogisticRegression().setFeaturesCol("features").setLabelCol("class")

    val pipeline = new Pipeline().setStages(Array(labelIndexer, vectorAssembler, lr))
    pipeline.fit(value)
  }
}
