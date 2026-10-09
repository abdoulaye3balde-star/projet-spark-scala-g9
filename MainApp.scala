package com.ecommerce.analytics

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import com.ecommerce.utils.{ConfigLoader, DataFrameWriterUtils, SparkSessionBuilder, StepTimer}
import com.ecommerce.utils.DataFrameWriterUtils.writeCsvAndParquet
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Point d'entrée de l'application (Partie 6, Membre C).
 *
 * Usage : MainApp [ingestion | transformation | analytics | all]   (défaut : all)
 *   ingestion      -> ingestion + validation + rapport de qualité
 *   transformation -> ingestion + validation + enrichissement
 *   analytics / all-> pipeline complet
 */
object MainApp {

  private val ValidSteps = Seq("ingestion", "transformation", "analytics", "all")

  /** Données validées, prêtes pour la transformation. */
  private case class Validated(transactions: DataFrame, users: DataFrame,
                               products: DataFrame, merchants: DataFrame)

  def main(args: Array[String]): Unit = {
    val step = args.headOption.map(_.trim.toLowerCase).getOrElse("all")
    if (!ValidSteps.contains(step)) {
      println(s"Argument inconnu : '${args.head}'")
      println("Valeurs acceptees : " + ValidSteps.mkString(" | ") + "  (defaut : all)")
      println("Exemple : spark-submit --class com.ecommerce.analytics.MainApp ecommerce-analytics.jar ingestion")
      return
    }

    val spark = SparkSessionBuilder.build()
    val optimizations = new SparkOptimizations()
    var failed = false
    try {
      run(spark, optimizations, step)
      println("=== TRAITEMENT TERMINE ===")
    } catch {
      case e: Throwable =>
        failed = true
        System.err.println(s"[ERREUR FATALE] ${e.getClass.getSimpleName} : ${e.getMessage}")
        e.printStackTrace()
    } finally {
      optimizations.releaseAll()   // unpersist() explicite
      spark.stop()                 // arrêt propre même en cas d'échec
    }
    if (failed) sys.exit(1)
  }

  private def run(spark: SparkSession, opt: SparkOptimizations, step: String): Unit = {
    val validated = StepTimer.time("1-ingestion+validation") { ingestAndValidate(spark, opt) }

    if (step != "ingestion") {
      val enriched = StepTimer.time("2-transformation") { transform(spark, opt, validated) }
      if (step == "analytics" || step == "all") {
        StepTimer.time("3-analytique") { analyse(spark, enriched) }
      }
    }
    printTimings()
  }

  // ------------------------------------------------------------------
  // Phase 1 : ingestion, validation, rapport de qualité (Membre A)
  // ------------------------------------------------------------------
  private def ingestAndValidate(spark: SparkSession, opt: SparkOptimizations): Validated = {
    val ingestion = new DataIngestion(spark)
    val tx = opt.cache(ingestion.readTransactions())
    val us = opt.cache(ingestion.readUsers())
    val pr = opt.cache(ingestion.readProducts())
    val me = opt.cache(ingestion.readMerchants())

    val (txOk, txKo) = DataValidation.validateTransactions(tx)
    val (usOk, usKo) = DataValidation.validateUsers(us)
    val (prOk, prKo) = DataValidation.validateProducts(pr)
    val (meOk, meKo) = DataValidation.validateMerchants(me)
    Seq(txOk, usOk, prOk, meOk).foreach(opt.cache(_))

    val entries = Seq(
      QualityEntry("transactions", tx.count(), txOk.count(), DataQuality.countNulls(tx.toDF()),
        DataQuality.countOrphans(tx.toDF(), us.toDF(), "user_id"),
        DataQuality.countOrphans(tx.toDF(), pr.toDF(), "product_id"),
        DataQuality.countOrphans(tx.toDF(), me.toDF(), "merchant_id")),
      QualityEntry("users",     us.count(), usOk.count(), DataQuality.countNulls(us.toDF())),
      QualityEntry("products",  pr.count(), prOk.count(), DataQuality.countNulls(pr.toDF())),
      QualityEntry("merchants", me.count(), meOk.count(), DataQuality.countNulls(me.toDF())))

    entries.foreach(e =>
      println(s"[${e.dataset}] lignes lues = ${e.lues} | lignes valides = ${e.valides}"))

    val report = DataQuality.buildReport(spark, entries)
    println("=== RAPPORT DE QUALITE DES DONNEES ===")
    report.show(false)

    val out  = ConfigLoader.outputPath.stripSuffix("/")
    val date = LocalDate.now.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    DataFrameWriterUtils.writeSingleCsv(report, s"$out/rapport_qualite_$date.csv")
    writeCsvAndParquet(txKo, "rejets_transactions")
    writeCsvAndParquet(usKo, "rejets_users")
    writeCsvAndParquet(prKo, "rejets_products")
    writeCsvAndParquet(meKo, "rejets_merchants")

    Validated(txOk.toDF(), usOk.toDF(), prOk.toDF(), meOk.toDF())
  }

  // ------------------------------------------------------------------
  // Phase 2 : transformation (Membre B)
  // ------------------------------------------------------------------
  private def transform(spark: SparkSession, opt: SparkOptimizations, v: Validated): DataFrame = {
    val transformation = new DataTransformation(spark)
    val enriched = opt.persistLarge(
      transformation.enrichTransactionData(v.transactions, v.users, v.products, v.merchants)).toDF()

    println(s"=== TRANSACTIONS ENRICHIES : ${enriched.count()} lignes ===")
    enriched.show(5, truncate = false)
    writeCsvAndParquet(enriched, "transactions_enrichies")

    val suspicious = opt.cache(transformation.suspiciousTransactions(enriched))
    println(s"=== TRANSACTIONS SUSPECTES : ${suspicious.count()} detectees ===")
    println("Top 20 des montants les plus eleves :")
    suspicious.select("transaction_id", "user_id", "amount", "day_period", "payment_method", "nb_conditions")
      .show(20, truncate = false)
    writeCsvAndParquet(suspicious, "transactions_suspectes")
    enriched
  }

  // ------------------------------------------------------------------
  // Phase 3 : analytique (Membre C)
  // ------------------------------------------------------------------
  private def analyse(spark: SparkSession, enriched: DataFrame): Unit = {
    val analytics = new Analytics(spark)

    val kpi = analytics.merchantKpis(enriched)
    println("=== KPI MARCHANDS (apercu) ===")
    kpi.show(10, truncate = false)
    writeCsvAndParquet(kpi, "kpi_marchands")

    val retention = analytics.cohortRetention(enriched)
    println("=== MATRICE DE RETENTION PAR COHORTE (apercu) ===")
    retention.show(15, truncate = false)
    writeCsvAndParquet(retention, "cohortes_retention")

    val best = analytics.bestCohortAt3Months(retention)
    println("=== MEILLEURE COHORTE A 3 MOIS ===")
    best.show(truncate = false)
    writeCsvAndParquet(best, "meilleure_cohorte_3m")

    // Bonus Q4.4
    val top10 = analytics.top10Products(enriched)
    println("=== TOP 10 PRODUITS ===")
    top10.show(truncate = false)
    writeCsvAndParquet(top10, "top10_produits")
    writeCsvAndParquet(analytics.caByCategoryAndRegion(enriched), "ca_categorie_region")
    writeCsvAndParquet(analytics.caByPaymentAndPeriod(enriched), "ca_paiement_periode")
  }

  private def printTimings(): Unit = {
    println("=== DUREE DES ETAPES ===")
    StepTimer.durations.foreach { case (k, ms) => println(f"  $k%-28s ${ms / 1000.0}%7.1f s") }
  }
}
