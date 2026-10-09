package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Analytique business (Partie 4, Membre C). Toutes les méthodes partent de transactions_enrichies. */
class Analytics(spark: SparkSession) {

  private def caAgeGroup(group: String) =
    round(sum(when(col("age_group") === group, col("amount")).otherwise(0.0)), 2)

  // ------------------------------------------------------------------
  // Q4.1 : KPI par marchand (+ bonus : taux de transactions suspectes)
  // ------------------------------------------------------------------
  def merchantKpis(enriched: DataFrame): DataFrame = {
    val base = enriched
      .filter(col("merchant_name").isNotNull)   // une ligne par marchand existant dans le référentiel
      .groupBy("merchant_id", "merchant_name", "merchant_category", "region")
      .agg(
        round(sum("amount"), 2).as("chiffre_affaires"),
        count(lit(1)).as("nb_transactions"),
        countDistinct("user_id").as("nb_clients_uniques"),
        round(avg("amount"), 2).as("montant_moyen"),
        round(sum(col("amount") * col("commission_rate")), 2).as("commission_totale"),
        caAgeGroup(AgeGroup.Jeune).as("ca_jeune"),
        caAgeGroup(AgeGroup.Adulte).as("ca_adulte"),
        caAgeGroup(AgeGroup.AgeMoyen).as("ca_age_moyen"),
        caAgeGroup(AgeGroup.Senior).as("ca_senior"),
        round(avg("is_suspicious") * 100, 2).as("taux_suspectes_pct"))

    val byCategory = Window.partitionBy("merchant_category").orderBy(col("chiffre_affaires").desc)
    val byRegion   = Window.partitionBy("region").orderBy(col("chiffre_affaires").desc)

    base
      .withColumn("rang_ca_categorie", rank().over(byCategory))
      .withColumn("rang_ca_region", rank().over(byRegion))
      .select("merchant_id", "merchant_name", "merchant_category", "region",
              "chiffre_affaires", "nb_transactions", "nb_clients_uniques", "montant_moyen",
              "rang_ca_categorie", "rang_ca_region", "commission_totale",
              "ca_jeune", "ca_adulte", "ca_age_moyen", "ca_senior", "taux_suspectes_pct")
      .orderBy("merchant_id")
  }

  // ------------------------------------------------------------------
  // Q4.2 : cohortes utilisateurs (+ bonus : CA par cohorte et par période)
  // ------------------------------------------------------------------
  def cohortRetention(enriched: DataFrame): DataFrame = {
    val tx = enriched
      .select("user_id", "transaction_date", "amount")
      .filter(col("user_id").isNotNull && col("transaction_date").isNotNull)

    val firstTx = tx.groupBy("user_id").agg(min("transaction_date").as("first_date"))
      .withColumn("cohort_month", date_format(col("first_date"), "yyyy-MM"))

    val sizes = firstTx.groupBy("cohort_month").agg(countDistinct("user_id").as("taille_cohorte"))

    tx.join(firstTx, Seq("user_id"))
      .withColumn("period_index",
        months_between(trunc(col("transaction_date"), "month"),
                       trunc(col("first_date"), "month")).cast("int"))
      .groupBy("cohort_month", "period_index")
      .agg(countDistinct("user_id").as("nb_utilisateurs_actifs"),
           round(sum("amount"), 2).as("chiffre_affaires"))
      .join(sizes, Seq("cohort_month"))
      .withColumn("taux_retention",
        round(col("nb_utilisateurs_actifs") / col("taille_cohorte") * 100, 2))
      .withColumn("revenu_moyen_par_utilisateur",
        round(col("chiffre_affaires") / col("nb_utilisateurs_actifs"), 2))
      .select("cohort_month", "period_index", "taille_cohorte", "nb_utilisateurs_actifs",
              "taux_retention", "chiffre_affaires", "revenu_moyen_par_utilisateur")
      .orderBy("cohort_month", "period_index")
  }

  /** Cohorte présentant la meilleure rétention à 3 mois (une seule ligne). */
  def bestCohortAt3Months(retention: DataFrame): DataFrame =
    retention
      .filter(col("period_index") === 3)
      .orderBy(col("taux_retention").desc, col("cohort_month"))
      .limit(1)
      .select(col("cohort_month"), col("taille_cohorte"),
              col("nb_utilisateurs_actifs").as("nb_utilisateurs_actifs_m3"),
              col("taux_retention").as("taux_retention_m3"))

  // ------------------------------------------------------------------
  // Q4.4 (bonus) : produits, catégories, paiements
  // ------------------------------------------------------------------
  def top10Products(enriched: DataFrame): DataFrame = {
    val top = enriched
      .filter(col("product_name").isNotNull)
      .groupBy("product_id")
      .agg(first("product_name", ignoreNulls = true).as("product_name"),
           first("category", ignoreNulls = true).as("category"),
           round(sum("amount"), 2).as("chiffre_affaires"),
           count(lit(1)).as("nb_transactions"),
           first("rating", ignoreNulls = true).as("rating"),
           first("stock", ignoreNulls = true).as("stock"))
      .orderBy(col("chiffre_affaires").desc, col("product_id"))
      .limit(10)
    top.withColumn("rang", row_number().over(Window.orderBy(col("chiffre_affaires").desc, col("product_id"))))
      .select("rang", "product_id", "product_name", "category",
              "chiffre_affaires", "nb_transactions", "rating", "stock")
      .orderBy("rang")
  }

  def caByCategoryAndRegion(enriched: DataFrame): DataFrame = {
    val agg = enriched.filter(col("region").isNotNull && col("category").isNotNull)
      .groupBy("region", "category")
      .agg(round(sum("amount"), 2).as("chiffre_affaires"), count(lit(1)).as("nb_transactions"))
    agg.withColumn("part_ca_region_pct",
        round(col("chiffre_affaires") / sum("chiffre_affaires").over(Window.partitionBy("region")) * 100, 2))
      .orderBy("region", "category")
  }

  def caByPaymentAndPeriod(enriched: DataFrame): DataFrame = {
    val agg = enriched.filter(col("payment_method").isNotNull && col("day_period").isNotNull)
      .groupBy("payment_method", "day_period")
      .agg(round(sum("amount"), 2).as("chiffre_affaires"), count(lit(1)).as("nb_transactions"))
    val total = agg.agg(sum("chiffre_affaires")).head.getDouble(0)
    agg.withColumn("part_ca_pct", round(col("chiffre_affaires") / lit(total) * 100, 2))
      .orderBy("payment_method", "day_period")
  }
}
