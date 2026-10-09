package com.ecommerce.analytics

import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._

/** Libellés des tranches d'âge (le "Â" est écrit en unicode pour éviter tout souci d'encodage). */
object AgeGroup {
  val Jeune    = "Jeune"
  val Adulte   = "Adulte"
  val AgeMoyen = "\u00c2ge Moyen"
  val Senior   = "Senior"
}

/** Transformations avancées (Partie 3, Membre B). */
class DataTransformation(
  spark: SparkSession,
  broadcastEnabled: Boolean = SparkOptimizations.broadcastEnabled) {

  private val windowDays  = ConfigLoader.int("analytics.window-days", 7)
  private val activeDays  = ConfigLoader.int("analytics.active-user-min-days", 5)
  private val amountPct   = ConfigLoader.double("analytics.suspicious-amount-pct", 300.0)
  private val delayMin    = ConfigLoader.double("analytics.suspicious-delay-min", 5.0)
  private val minConds    = ConfigLoader.int("analytics.suspicious-min-conditions", 2)

  // Ordre des colonnes de la sortie transactions_enrichies
  private val outputColumns = Seq(
    "transaction_id", "user_id", "product_id", "merchant_id", "amount", "timestamp",
    "transaction_date", "location", "payment_method", "category",
    "age", "annual_income", "city", "customer_segment", "age_group",
    "product_name", "product_price", "rating", "stock",
    "merchant_name", "merchant_category", "region", "commission_rate",
    "hour", "day_of_week", "month", "is_weekend", "day_period", "is_working_hours",
    "transaction_rank", "total_transactions_user", "montant_cumule_7j",
    "is_active_user", "jours_depuis_achat_precedent",
    "ecart_panier_moyen_pct", "is_suspicious")

  private def byUserOrdered: WindowSpec =
    Window.partitionBy("user_id").orderBy(col("transaction_date"), col("transaction_id"))

  private def ageGroupColumn: Column =
    when(col("age").isNull, lit(null).cast("string"))
      .when(col("age") < 25, AgeGroup.Jeune)
      .when(col("age") <= 44, AgeGroup.Adulte)   // 25 ans rattaché à "Adulte" (trou du sujet : <25 puis 26-44)
      .when(col("age") <= 64, AgeGroup.AgeMoyen)
      .otherwise(AgeGroup.Senior)

  /**
   * Q3.2 + Q3.3 (+ colonnes bonus Q3.4) : jointures, UDF temporelle, fenêtrage.
   *
   * Jointures : LEFT JOIN sur users, products et merchants. Le sujet demande une ligne par
   * transaction valide ; un left join conserve donc toute transaction valide, même dont
   * l'utilisateur / le produit / le marchand est orphelin ou a été rejeté (colonnes à null).
   * Les petites tables sont diffusées avec broadcast() (Q5.2).
   */
  def enrichTransactionData(transactions: DataFrame, users: DataFrame,
                            products: DataFrame, merchants: DataFrame): DataFrame = {

    val u = users.select("user_id", "age", "annual_income", "city", "customer_segment")
    val p = products.select(
      col("product_id"), col("name").as("product_name"), col("price").as("product_price"),
      col("rating"), col("stock"))
    val m = merchants.select(
      col("merchant_id"), col("name").as("merchant_name"),
      col("category").as("merchant_category"), col("region"), col("commission_rate"))

    val joined = transactions
      .join(SparkOptimizations.maybeBroadcast(u, broadcastEnabled), Seq("user_id"), "left")
      .join(SparkOptimizations.maybeBroadcast(p, broadcastEnabled), Seq("product_id"), "left")
      .join(SparkOptimizations.maybeBroadcast(m, broadcastEnabled), Seq("merchant_id"), "left")

    val withTime = joined
      .withColumn("transaction_date", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
      .withColumn("age_group", ageGroupColumn)
      .withColumn("time_features", TimeFeatures.extractTimeFeatures(col("timestamp")))
      .select(col("*"), col("time_features.*"))   // éclatement de la struct en colonnes simples
      .drop("time_features")

    val allUser  = Window.partitionBy("user_id")
    val lastDays = Window.partitionBy("user_id")
      .orderBy(col("transaction_date").cast("long"))
      .rangeBetween(-windowDays.toLong * 24 * 3600, 0)

    val windowed = withTime
      .withColumn("transaction_rank", row_number().over(byUserOrdered))
      .withColumn("total_transactions_user", count(lit(1)).over(allUser))
      .withColumn("montant_cumule_7j", round(sum("amount").over(lastDays), 2))
      .withColumn("is_active_user",
        when(size(collect_set(to_date(col("transaction_date"))).over(lastDays)) >= activeDays, 1).otherwise(0))
      .withColumn("jours_depuis_achat_precedent",
        datediff(to_date(col("transaction_date")),
                 to_date(lag(col("transaction_date"), 1).over(byUserOrdered))))

    withSuspicion(windowed).select(outputColumns.map(col): _*)
  }

  /**
   * Q3.4 (bonus) : ajoute panier moyen, écart au panier moyen, délai, les 4 conditions
   * et le flag is_suspicious.
   */
  private def withSuspicion(df: DataFrame): DataFrame = {
    val allUser = Window.partitionBy("user_id")
    df.withColumn("panier_moyen_user", avg("amount").over(allUser))
      .withColumn("ecart_raw", (col("amount") - col("panier_moyen_user")) / col("panier_moyen_user") * 100)
      .withColumn("delai_precedente_minutes",
        (col("transaction_date").cast("long") -
          lag(col("transaction_date").cast("long"), 1).over(byUserOrdered)) / 60.0)
      .withColumn("cond_montant", when(col("ecart_raw") > amountPct, 1).otherwise(0))
      .withColumn("cond_night",   when(col("day_period") === "Night", 1).otherwise(0))
      .withColumn("cond_delai",   when(col("delai_precedente_minutes") < delayMin, 1).otherwise(0))
      .withColumn("cond_crypto",  when(col("payment_method") === "CRYPTO", 1).otherwise(0))
      .withColumn("nb_conditions",
        col("cond_montant") + col("cond_night") + col("cond_delai") + col("cond_crypto"))
      .withColumn("is_suspicious", when(col("nb_conditions") >= minConds, 1).otherwise(0))
      .withColumn("ecart_panier_moyen_pct", round(col("ecart_raw"), 2))
      .withColumn("panier_moyen_user", round(col("panier_moyen_user"), 2))
      .withColumn("delai_precedente_minutes", round(col("delai_precedente_minutes"), 2))
      .drop("ecart_raw")
  }

  /** Q3.4 (bonus) : transactions suspectes, triées par montant décroissant. */
  def suspiciousTransactions(enriched: DataFrame): DataFrame =
    withSuspicion(enriched)
      .filter(col("is_suspicious") === 1)
      .select("transaction_id", "user_id", "merchant_id", "transaction_date", "amount",
              "panier_moyen_user", "ecart_panier_moyen_pct", "day_period",
              "delai_precedente_minutes", "payment_method",
              "cond_montant", "cond_night", "cond_delai", "cond_crypto",
              "nb_conditions", "is_suspicious")
      .orderBy(col("amount").desc)
}
