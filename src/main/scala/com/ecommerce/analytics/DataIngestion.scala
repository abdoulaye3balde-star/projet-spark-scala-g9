package com.ecommerce.analytics

import com.ecommerce.models._
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

/** Centralise la lecture des 4 sources de données (Membre A). */
class DataIngestion(spark: SparkSession) {
  import spark.implicits._

  // Schéma explicite pour transactions.csv
  private val transactionSchema = StructType(Seq(
    StructField("transaction_id", StringType), StructField("user_id", StringType),
    StructField("product_id", StringType),     StructField("merchant_id", StringType),
    StructField("amount", DoubleType),         StructField("timestamp", StringType),
    StructField("location", StringType),       StructField("payment_method", StringType),
    StructField("category", StringType)))

  // Schéma explicite pour users.json (aucun champ imbriqué dans ce jeu de données ;
  // s'il y en avait, on ajouterait un StructType et on les lirait avec col("a.b")).
  private val userSchema = StructType(Seq(
    StructField("user_id", StringType),   StructField("age", IntegerType),
    StructField("annual_income", DoubleType), StructField("city", StringType),
    StructField("customer_segment", StringType),
    StructField("preferred_categories", ArrayType(StringType)),
    StructField("registration_date", StringType)))

  /** Capture et affiche les erreurs de lecture (fichier introuvable, structure incorrecte...). */
  private def safeRead[T](label: String, path: String)(body: => Dataset[T]): Dataset[T] =
    try body
    catch {
      case e: Exception =>
        println(s"[ERREUR] Lecture de '$label' impossible (chemin : $path) : ${e.getMessage}")
        throw e
    }

  def readTransactions(): Dataset[Transaction] = {
    val path = ConfigLoader.inputPath("transactions")
    safeRead("transactions", path) {
      spark.read.option("header", "true").schema(transactionSchema).csv(path).as[Transaction]
    }
  }

  def readUsers(): Dataset[User] = {
    val path = ConfigLoader.inputPath("users")
    safeRead("users", path) {
      spark.read.schema(userSchema).json(path).as[User]
    }
  }

  def readProducts(): Dataset[Product] = {
    val path = ConfigLoader.inputPath("products")
    safeRead("products", path) {
      // cast explicites : protège contre un type Parquet différent (ex : stock en long)
      spark.read.parquet(path).select(
        col("product_id").cast("string").as("product_id"),
        col("name").cast("string").as("name"),
        col("category").cast("string").as("category"),
        col("price").cast("double").as("price"),
        col("merchant_id").cast("string").as("merchant_id"),
        col("rating").cast("double").as("rating"),
        col("stock").cast("int").as("stock")).as[Product]
    }
  }

  def readMerchants(): Dataset[Merchant] = {
    val path = ConfigLoader.inputPath("merchants")
    safeRead("merchants", path) {
      // schéma inféré par Spark, puis cast (establishment_date serait inféré en entier)
      spark.read.option("header", "true").option("inferSchema", "true").csv(path).select(
        col("merchant_id").cast("string").as("merchant_id"),
        col("name").cast("string").as("name"),
        col("category").cast("string").as("category"),
        col("region").cast("string").as("region"),
        col("commission_rate").cast("double").as("commission_rate"),
        col("establishment_date").cast("string").as("establishment_date")).as[Merchant]
    }
  }
}
