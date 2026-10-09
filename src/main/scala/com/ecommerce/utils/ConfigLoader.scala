package com.ecommerce.utils

import com.typesafe.config.{Config, ConfigFactory}

/** Chargement de application.conf, avec une valeur par défaut si une clé est absente (Partie 7). */
object ConfigLoader {

  private lazy val root: Config = ConfigFactory.load()
  lazy val config: Config =
    if (root.hasPath("app")) root.getConfig("app") else ConfigFactory.empty()

  def string(path: String, default: String): String =
    if (config.hasPath(path)) config.getString(path) else default
  def int(path: String, default: Int): Int =
    if (config.hasPath(path)) config.getInt(path) else default
  def double(path: String, default: Double): Double =
    if (config.hasPath(path)) config.getDouble(path) else default
  def bool(path: String, default: Boolean): Boolean =
    if (config.hasPath(path)) config.getBoolean(path) else default

  def inputPath(name: String): String =
    string(s"data.input.$name", s"src/main/resources/data/$name")

  def outputPath: String = string("data.output.path", "output/")
}
