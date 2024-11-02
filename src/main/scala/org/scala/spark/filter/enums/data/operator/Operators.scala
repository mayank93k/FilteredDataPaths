package org.scala.spark.filter.enums.data.operator

/**
 * Enumeration of operators for numeric data comparisons.
 *
 * The following operators are available:
 *              - lt: Less than ("<")
 *              - lteq: Less than or equal to ("<=")
 *              - gt: Greater than (">")
 *              - gteq: Greater than or equal to (">=")
 *              - in: Checks if a value is in a given list of values
 *              - equalTo: Checks for equality ("=")
 *
 * These operators can be used in conjunction with various numeric constraints
 * for filtering or querying data based on numeric values.
 */
object NumericDataOperator extends Enumeration {
  type NumericOperator = Value
  val lt, lteq, gt, gteq, in, equalTo = Value
}

/**
 * Enumeration of operators for string data comparisons.
 *
 * The following operators are available:
 *      - in: Checks if a string value is included in a specified list of values.
 *      - equalTo: Checks if a string value is equal to another string.
 *
 * These operators can be utilized in string predicates to filter or query data based on string values,
 * enabling flexible data handling in various applications.
 */
object StringDataOperator extends Enumeration {
  type StringOperator = Value
  val in, equalTo = Value
}
