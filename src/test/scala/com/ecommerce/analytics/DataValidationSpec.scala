package com.ecommerce.analytics

import com.ecommerce.models._
import com.ecommerce.utils.DataFrameWriterUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

/** Tests des règles de validation (Q2.2) et du taux de rejet (Q2.4) sur de petits jeux de données. */
class DataValidationSpec extends AnyFunSuite {

  private lazy val spark = SparkSession.builder()
    .master("local[1]").appName("tests")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  test("transactions : une ligne valide, trois rejets avec leur motif") {
    import spark.implicits._
    val ds = Seq(
      Transaction("T1", "U1", "P1", "M1", Some(10.0),  "20240101120000", "Paris", "CARD", "Books"),
      Transaction("T2", "U1", "P1", "M1", Some(-5.0),  "20240101120000", "Paris", "CARD", "Books"),
      Transaction("T3", "U1", "P1", "M1", Some(10.0),  "2024",           "Paris", "CARD", "Books"),
      Transaction("T4", "U1", "P1", "M1", Some(0.0),   "2024",           "Paris", "CARD", "Books")
    ).toDS()
    val (valid, rejected) = DataValidation.validateTransactions(ds)
    assert(valid.count() == 1)
    val reasons = rejected.select("transaction_id", "rejection_reason").as[(String, String)].collect().toMap
    assert(reasons("T2") == "amount <= 0")
    assert(reasons("T3") == "timestamp_invalide")
    assert(reasons("T4") == "amount <= 0 | timestamp_invalide")
  }

  test("users : age et revenu, valeurs manquantes rejetees") {
    import spark.implicits._
    val ds = Seq(
      User("U1", Some(30), Some(1000.0), "Paris", "Standard", Some(Seq("Books")), "20230101"),
      User("U2", Some(12), Some(1000.0), "Paris", "Standard", Some(Seq("Books")), "20230101"),
      User("U3", Some(30), Some(-1.0),   "Paris", "Standard", Some(Seq("Books")), "20230101"),
      User("U4", None,     Some(1000.0), "Paris", "Standard", None,               "20230101")
    ).toDS()
    val (valid, rejected) = DataValidation.validateUsers(ds)
    assert(valid.count() == 1)
    assert(rejected.count() == 3)
  }

  test("taux de rejet arrondi a deux decimales") {
    assert(DataFrameWriterUtils.tauxRejet(138047L, 1890L) == 1.37)
    assert(DataFrameWriterUtils.tauxRejet(0L, 0L) == 0.0)
  }
}
