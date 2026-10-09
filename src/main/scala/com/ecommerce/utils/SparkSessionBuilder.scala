package com.ecommerce.utils

import org.apache.spark.sql.SparkSession

/** Création de la SparkSession à partir de application.conf. */
object SparkSessionBuilder {

  def build(): SparkSession = {
    val builder = SparkSession.builder()
      .appName(ConfigLoader.string("name", "EcommerceAnalytics"))
      .config("spark.sql.shuffle.partitions", ConfigLoader.int("spark.shuffle.partitions", 8))

    // Le master vient d'application.conf, sauf si spark-submit en a déjà fourni un.
    val spark =
      if (sys.props.contains("spark.master")) builder.getOrCreate()
      else builder.master(ConfigLoader.string("spark.master", "local[*]")).getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark
  }
}
