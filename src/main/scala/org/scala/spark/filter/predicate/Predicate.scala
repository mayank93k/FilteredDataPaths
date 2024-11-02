package org.scala.spark.filter.predicate

import org.scala.spark.filter.enums.data.operator.NumericDataOperator.NumericOperator
import org.scala.spark.filter.enums.data.operator.StringDataOperator.StringOperator

/**
 * Represents the possible numeric predicates, including:
 *                  - [[IntConstraint]]
 *                  - [[LongConstraint]]
 *                  - [[FloatConstraint]]
 *                  - [[DoubleConstraint]]
 *
 * Each predicate can utilize the following operators for comparison:
 *                  - Less than ("<")
 *                  - Less than or equal to ("<=")
 *                  - Greater than (">")
 *                  - Greater than or equal to (">=")
 *                  - In ("in")
 *                  - Equal to ("=")
 */
sealed trait NumericConstraint {
  def operator: NumericOperator

  def <(columnValue: String): Boolean

  def <=(columnValue: String): Boolean

  def >(columnValue: String): Boolean

  def >=(columnValue: String): Boolean

  def in(columnValue: String): Boolean

  def ===(columnValue: String): Boolean
}

/**
 * Represents a constraint for filtering integer values based on a specified operator.
 *
 * @param operator     The numeric operator to be applied for filtering.
 *                     Possible operators include:
 *                     - "<"   : Less than
 *                     - "<="  : Less than or equal to
 *                     - ">"   : Greater than
 *                     - ">="  : Greater than or equal to
 *                     - "in"  : Matches if the value is in the provided list
 *                     - "="   : Equal to
 * @param columnValues A variable number of integer values on which the filter is to be applied.
 *                     This represents the set of values to evaluate against the specified operator.
 */
case class IntConstraint(operator: NumericOperator, columnValues: Int*) extends NumericConstraint {
  def <(columnValue: String): Boolean = columnValue.toInt > columnValues.head

  def <=(columnValue: String): Boolean = columnValue.toInt >= columnValues.head

  def >(columnValue: String): Boolean = columnValue.toInt < columnValues.head

  def >=(columnValue: String): Boolean = columnValue.toInt <= columnValues.head

  def in(columnValue: String): Boolean = columnValues.contains(columnValue.toInt)

  def ===(columnValue: String): Boolean = columnValue.toInt == columnValues.head
}

/**
 * Represents a constraint for filtering long integer values based on a specified operator.
 *
 * @param operator     The numeric operator to be applied for filtering.
 *                     Possible operators include:
 *                     - "<"   : Less than
 *                     - "<="  : Less than or equal to
 *                     - ">"   : Greater than
 *                     - ">="  : Greater than or equal to
 *                     - "in"  : Matches if the value is in the provided list
 *                     - "="   : Equal to
 * @param columnValues A variable number of long integer values on which the filter is to be applied.
 *                     This represents the set of values to evaluate against the specified operator.
 */
case class LongConstraint(operator: NumericOperator, columnValues: Long*) extends NumericConstraint {
  def <(columnValue: String): Boolean = columnValue.toLong > columnValues.head

  def <=(columnValue: String): Boolean = columnValue.toLong >= columnValues.head

  def >(columnValue: String): Boolean = columnValue.toLong < columnValues.head

  def >=(columnValue: String): Boolean = columnValue.toLong <= columnValues.head

  def in(columnValue: String): Boolean = columnValues.contains(columnValue.toLong)

  def ===(columnValue: String): Boolean = columnValue.toLong == columnValues.head
}

/**
 * Represents a constraint for filtering float values based on a specified operator.
 *
 * @param operator     The numeric operator to be applied for filtering.
 *                     Possible operators include:
 *                     - "<"   : Less than
 *                     - "<="  : Less than or equal to
 *                     - ">"   : Greater than
 *                     - ">="  : Greater than or equal to
 *                     - "in"  : Matches if the value is in the provided list
 *                     - "="   : Equal to
 * @param columnValues A variable number of float values on which the filter is to be applied.
 *                     This represents the set of values to evaluate against the specified operator.
 */
case class FloatConstraint(operator: NumericOperator, columnValues: Float*) extends NumericConstraint {
  def <(columnValue: String): Boolean = columnValue.toFloat > columnValues.head

  def <=(columnValue: String): Boolean = columnValue.toFloat >= columnValues.head

  def >(columnValue: String): Boolean = columnValue.toFloat < columnValues.head

  def >=(columnValue: String): Boolean = columnValue.toFloat <= columnValues.head

  def in(columnValue: String): Boolean = columnValues.contains(columnValue.toFloat)

  def ===(columnValue: String): Boolean = columnValue.toFloat == columnValues.head
}

/**
 * Represents a constraint for filtering double values based on a specified operator.
 *
 * @param operator     The numeric operator to be applied for filtering.
 *                     Possible operators include:
 *                     - "<"   : Less than
 *                     - "<="  : Less than or equal to
 *                     - ">"   : Greater than
 *                     - ">="  : Greater than or equal to
 *                     - "in"  : Matches if the value is in the provided list
 *                     - "="   : Equal to
 * @param columnValues A variable number of double values on which the filter is to be applied.
 *                     This represents the set of values to evaluate against the specified operator.
 */
case class DoubleConstraint(operator: NumericOperator, columnValues: Double*) extends NumericConstraint {
  def <(columnValue: String): Boolean = columnValue.toDouble > columnValues.head

  def <=(columnValue: String): Boolean = columnValue.toDouble >= columnValues.head

  def >(columnValue: String): Boolean = columnValue.toDouble < columnValues.head

  def >=(columnValue: String): Boolean = columnValue.toDouble <= columnValues.head

  def in(columnValue: String): Boolean = columnValues.contains(columnValue.toDouble)

  def ===(columnValue: String): Boolean = columnValue.toDouble == columnValues.head
}

/**
 * Represents a predicate for filtering string values based on a specified operator.
 *
 * @param operator     The string operator to be applied for filtering.
 *                     Possible operators include:
 *                     - "in"  : Matches if the string is in the provided list of values.
 *                     - "="   : Checks for equality with the provided string value.
 * @param columnValues A variable number of string values to evaluate against the specified operator.
 *                     - For operators other than "in", the length of columnValues should be 1,
 *                       indicating a single value to compare.
 *                     - For the "in" operator, you can provide a list of possible matching values.
 */
case class StringConstraint(operator: StringOperator, columnValues: String*) {
  def ===(columnValue: String): Boolean = columnValue equals columnValues.head

  def in(columnValue: String): Boolean = columnValues.contains(columnValue)
}
