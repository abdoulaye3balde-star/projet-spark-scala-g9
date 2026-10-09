package com.ecommerce.models

/*
 * Case classes des 4 jeux de données (Membre A).
 * Les champs numériques sont des Option : les fichiers contiennent volontairement
 * des valeurs manquantes, qui doivent pouvoir être lues puis rejetées par la validation.
 */

case class Transaction(
  transaction_id: String,
  user_id: String,
  product_id: String,
  merchant_id: String,
  amount: Option[Double],
  timestamp: String,
  location: String,
  payment_method: String,
  category: String)

case class User(
  user_id: String,
  age: Option[Int],
  annual_income: Option[Double],
  city: String,
  customer_segment: String,
  preferred_categories: Option[Seq[String]],
  registration_date: String)

case class Product(
  product_id: String,
  name: String,
  category: String,
  price: Option[Double],
  merchant_id: String,
  rating: Option[Double],
  stock: Option[Int])

case class Merchant(
  merchant_id: String,
  name: String,
  category: String,
  region: String,
  commission_rate: Option[Double],
  establishment_date: String)
