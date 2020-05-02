package model

import com.google.cloud.datastore.Value

import scala.collection.mutable.ListBuffer

/**
 *
 * @param id: customer id
 * @param balances_norm: balance normalized in 12 months
 * @param balances_raw: balance raw in 12 months
 * @param balances_future: balance raw in n months. it divided to two part, the first part is balances_raw, second part for forecasting
 */
case class Customer(id: Int, balances_norm: ListBuffer[Double], balances_raw: ListBuffer[Double], balances_future: ListBuffer[Double])
