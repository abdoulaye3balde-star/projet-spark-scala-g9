package com.ecommerce.analytics

import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, TextStyle}
import java.util.Locale
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

/** Structure renvoyée par l'UDF (éclatée ensuite en colonnes simples). */
case class TimeFeatureOutput(
  hour: Int,
  day_of_week: String,
  month: String,
  is_weekend: Int,
  day_period: String,
  is_working_hours: Int)

/** UDF extractTimeFeatures (Q3.1, Membre B). */
object TimeFeatures {

  private val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  /**
   * Fonction pure, testable sans Spark. Robuste : une chaîne nulle, vide ou mal formée
   * (longueur incorrecte, caractères non numériques, date impossible) renvoie None
   * au lieu de faire échouer le job.
   */
  def compute(ts: String): Option[TimeFeatureOutput] = {
    if (ts == null) None
    else {
      val t = ts.trim
      if (t.length != 14 || !t.forall(_.isDigit)) None
      else {
        try {
          val d    = LocalDateTime.parse(t, formatter)
          val hour = d.getHour
          val dow  = d.getDayOfWeek
          val weekend = if (dow.getValue >= 6) 1 else 0 // 6 = samedi, 7 = dimanche
          val period =
            if (hour >= 6 && hour < 12) "Morning"
            else if (hour >= 12 && hour < 18) "Afternoon"
            else if (hour >= 18 && hour < 22) "Evening"
            else "Night"
          val working = if (hour >= 9 && hour <= 17) 1 else 0
          Some(TimeFeatureOutput(
            hour,
            dow.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
            d.getMonth.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
            weekend, period, working))
        } catch {
          case _: Exception => None
        }
      }
    }
  }

  val extractTimeFeatures: UserDefinedFunction = udf((ts: String) => compute(ts))
}
