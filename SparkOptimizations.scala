package com.ecommerce.analytics

import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ArrayBuffer

/**
 * Optimisations Spark (Partie 5, Membre C), pilotées par application.conf :
 *   - cache() pour les DataFrame réutilisés (Q5.1)
 *   - persist(MEMORY_AND_DISK_SER) pour les gros DataFrame (Q5.1)
 *   - unpersist() explicite en fin de traitement (Q5.1)
 *   - broadcast() des petites tables lors des jointures (Q5.2)
 */
class SparkOptimizations(cacheEnabled: Boolean = ConfigLoader.bool("optimization.enable-cache", true)) {

  private val registry = ArrayBuffer.empty[Dataset[_]]

  /** cache() d'un DataFrame/Dataset réutilisé plusieurs fois. */
  def cache[T](ds: Dataset[T]): Dataset[T] =
    if (cacheEnabled) { registry += ds; ds.cache() } else ds

  /** persist(MEMORY_AND_DISK_SER) pour un DataFrame trop volumineux pour la mémoire seule. */
  def persistLarge[T](ds: Dataset[T]): Dataset[T] =
    if (cacheEnabled) { registry += ds; ds.persist(StorageLevel.MEMORY_AND_DISK_SER) } else ds

  /** unpersist() de tout ce qui a été mis en cache. */
  def releaseAll(): Unit = {
    registry.foreach(_.unpersist())
    registry.clear()
  }
}

object SparkOptimizations {

  def broadcastEnabled: Boolean = ConfigLoader.bool("optimization.enable-broadcast", true)

  /** Applique broadcast() à une petite table si l'option est activée dans application.conf. */
  def maybeBroadcast(df: DataFrame, enabled: Boolean = broadcastEnabled): DataFrame =
    if (enabled) broadcast(df) else df
}
