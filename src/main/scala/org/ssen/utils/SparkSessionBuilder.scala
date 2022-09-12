package org.ssen.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class SparkSessionBuilder() {

  private lazy val sparkConf: SparkConf = new SparkConf()

  def configs(configs: Map[String, String]): SparkSessionBuilder = {
    for (config <- configs) {
      sparkConf.set(config._1, config._2)
    }
    this
  }

  def getOrCreate() : SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
}