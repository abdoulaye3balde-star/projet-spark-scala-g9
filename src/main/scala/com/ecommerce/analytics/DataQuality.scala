package com.ecommerce.analytics

import com.ecommerce.utils.DataFrameWriterUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Une ligne du rapport de qualité (Q2.4 + compteurs d'orphelins de la Q2.5). */
case class QualityEntry(
  dataset: String,
  lues: Long,
  valides: Long,
  nulles: Long,
  userOrphelins: Long = 0L,
  productOrphelins: Long = 0L,
  merchantOrphelins: Long = 0L)

/** Rapport de qualité des données (Membre A). */
object DataQuality {

  /** Total des valeurs nulles, toutes colonnes confondues. */
  def countNulls(df: DataFrame): Long = {
    val exprs = df.columns.map(c => sum(when(col(s"`$c`").isNull, 1L).otherwise(0L)))
    val row   = df.agg(exprs.head, exprs.tail: _*).head
    (0 until row.length).map(i => if (row.isNullAt(i)) 0L else row.getLong(i)).sum
  }

  /**
   * Q2.5 : nombre de transactions dont l'identifiant (non nul) n'existe pas dans le référentiel.
   * Une valeur nulle est une valeur manquante (comptée dans nb_valeurs_nulles), pas une référence orpheline.
   */
  def countOrphans(transactions: DataFrame, reference: DataFrame, key: String): Long =
    transactions.filter(col(key).isNotNull)
      .select(key)
      .join(reference.select(key).distinct(), Seq(key), "left_anti")
      .count()

  def buildReport(spark: SparkSession, entries: Seq[QualityEntry]): DataFrame = {
    import spark.implicits._
    entries.map { e =>
      val rejetees = e.lues - e.valides
      (e.dataset, e.lues, e.valides, rejetees,
       DataFrameWriterUtils.tauxRejet(e.lues, rejetees), e.nulles,
       e.userOrphelins, e.productOrphelins, e.merchantOrphelins)
    }.toDF("dataset", "nb_lignes_lues", "nb_lignes_valides", "nb_lignes_rejetees",
           "taux_rejet", "nb_valeurs_nulles",
           "nb_user_id_orphelins", "nb_product_id_orphelins", "nb_merchant_id_orphelins")
  }
}
