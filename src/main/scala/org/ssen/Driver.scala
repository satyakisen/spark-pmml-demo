package org.ssen

import org.ssen.utils.SparkSessionBuilder

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

    println(spark.sparkContext.version)

    spark.stop()
  }

  private[this] def fetchConfiguration(config: String) : Map[String, String] =  {
    // TODO: Add parsing from arguments.
    val conf = s"Parse Config: $config"
    print(conf)
    Map("spark.app.name" -> "Prod App")
  }

  private[this] def devConfiguration() : Map[String, String] = Map(
    "spark.app.name" -> "DevApp",
    "spark.master" -> "local[*]"
  )
}
