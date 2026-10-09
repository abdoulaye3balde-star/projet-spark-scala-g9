package com.ecommerce.analytics

import com.ecommerce.models._
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder}
import org.apache.spark.sql.functions._

/** Règles de validation (Membre A). Chaque fonction renvoie (lignes valides, lignes rejetées). */
object DataValidation {

  // Une condition NULL (valeur manquante) est considérée comme NON respectée.
  private def ok(c: Column): Column = coalesce(c, lit(false))

  private def split[T: Encoder](df: DataFrame, reasons: Column): (Dataset[T], DataFrame) = {
    val tagged   = df.withColumn("rejection_reason", reasons)
    val valid    = tagged.filter(col("rejection_reason") === "").drop("rejection_reason").as[T]
    val rejected = tagged.filter(col("rejection_reason") =!= "")
    (valid, rejected)
  }

  def validateTransactions(ds: Dataset[Transaction]): (Dataset[Transaction], DataFrame) = {
    import ds.sparkSession.implicits._
    val len = ConfigLoader.int("validation.transaction.timestamp-length", 14)
    val reasons = concat_ws(" | ",
      when(!ok(col("amount") > 0), lit("amount <= 0")),
      when(!ok(length(col("timestamp")) === len), lit("timestamp_invalide")))
    split[Transaction](ds.toDF(), reasons)
  }

  def validateUsers(ds: Dataset[User]): (Dataset[User], DataFrame) = {
    import ds.sparkSession.implicits._
    val minAge = ConfigLoader.int("validation.user.min-age", 16)
    val maxAge = ConfigLoader.int("validation.user.max-age", 100)
    val reasons = concat_ws(" | ",
      when(!ok(col("age").between(minAge, maxAge)), lit("age_hors_intervalle")),
      when(!ok(col("annual_income") > 0), lit("income <= 0")))
    split[User](ds.toDF(), reasons)
  }

  def validateProducts(ds: Dataset[Product]): (Dataset[Product], DataFrame) = {
    import ds.sparkSession.implicits._
    val minR = ConfigLoader.double("validation.product.min-rating", 1.0)
    val maxR = ConfigLoader.double("validation.product.max-rating", 5.0)
    val reasons = concat_ws(" | ",
      when(!ok(col("price") > 0), lit("price <= 0")),
      when(!ok(col("rating").between(minR, maxR)), lit("rating_hors_intervalle")))
    split[Product](ds.toDF(), reasons)
  }

  def validateMerchants(ds: Dataset[Merchant]): (Dataset[Merchant], DataFrame) = {
    import ds.sparkSession.implicits._
    val minC = ConfigLoader.double("validation.merchant.min-commission", 0.0)
    val maxC = ConfigLoader.double("validation.merchant.max-commission", 1.0)
    val reasons = concat_ws(" | ",
      when(!ok(col("commission_rate").between(minC, maxC)), lit("commission_hors_intervalle")))
    split[Merchant](ds.toDF(), reasons)
  }
}
