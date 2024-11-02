package org.scala.spark.filter.paths

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.scala.spark.filter.enums.data.operator.{NumericDataOperator, StringDataOperator}
import org.scala.spark.filter.predicate.{IntConstraint, StringConstraint}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PredicateFilteredReaderTest extends AnyFlatSpec with Matchers {

  // Create an actual Spark session
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("FilteredDataReaderSpec")
    .master("local[*]") // Use local mode for testing
    .getOrCreate()

  // Define some sample paths for testing
  val path1 = new Path("src/test/resources/data/version=1.0.0/partn_key_yr_hr=20230304/")
  val path2 = new Path("src/test/resources/data/version=1.0.0/partn_key_yr_hr=20230305/")
  val path3 = new Path("src/test/resources/data/version=1.0.0/partn_key_yr_hr=20230306/")
  val basePath = "file:/Users/mayank/Documents/Projects/FilteredDataPaths/"
  val paths: Array[Path] = Array(path1, path2, path3)

  "extractFilteredPaths" should "return all paths when no predicates are applied" in {
    val numericPredicates = Map.empty[String, List[IntConstraint]]
    val stringPredicates = Map.empty[String, List[StringConstraint]]

    val result = PredicateFilteredReader.extractFilteredPaths(spark, paths, numericPredicates, stringPredicates)

    result shouldEqual paths
  }

  it should "filter paths based on numeric predicates" in {
    val numericPredicates = Map(
      "partn_key_yr_hr" -> List(IntConstraint(NumericDataOperator.gt, 20230304))
    )
    val stringPredicates = Map.empty[String, List[StringConstraint]]

    val result = PredicateFilteredReader.extractFilteredPaths(spark, paths, numericPredicates, stringPredicates)

    // Expecting to filter out path1, should only have path2 and path3
    result.mkString shouldEqual Array(basePath + path2 + "/source_system=xyz", basePath + path3 + "/source_system=abc").mkString
  }

  it should "filter paths based on string predicates" in {
    val numericPredicates = Map.empty[String, List[IntConstraint]]
    val stringPredicates = Map(
      "source_system" -> List(StringConstraint(StringDataOperator.equalTo, "xyz"))
    )

    val result = PredicateFilteredReader.extractFilteredPaths(spark, paths, numericPredicates, stringPredicates)

    // Expecting to keep path1 and path2, and filter out path3
    result.mkString shouldEqual Array(basePath + path1 + "/source_system=xyz/abcc1.json",
      basePath + path2 + "/source_system=xyz/abbb.json").mkString
  }

  it should "apply both numeric and string predicates" in {
    val numericPredicates = Map(
      "partn_key_yr_hr" -> List(IntConstraint(NumericDataOperator.gt, 20230304))
    )
    val stringPredicates = Map(
      "source_system" -> List(StringConstraint(StringDataOperator.equalTo, "xyz"))
    )

    val result = PredicateFilteredReader.extractFilteredPaths(spark, paths, numericPredicates, stringPredicates)

    // Expecting only path2 should be returned
    Array(result.mkString.replace(""""""", """""")) shouldEqual Array(basePath + path2 + "/source_system=xyz/" + "abbb.json")
  }

  it should "handle paths with no child directories correctly" in {
    val emptyPath = Array(new Path("src/test/resources/data/version=1.0.0/partn_key_yr_hr=20230304/source_system=xyz/abcc1.json"))

    val result = PredicateFilteredReader.extractImmediateChildPaths(emptyPath, spark)

    result shouldEqual Array.empty[Path] // Should return empty array
  }

  it should "not include paths that should be filtered out" in {
    val pathToExclude = new Path("src/test/resources/data/version=1.0.0/_common_metadata")
    val pathToInclude = new Path("src/test/resources/data/version=1.0.0/partn_key_yr_hr=20230305/source_system=xyz")
    val allPaths = Array(pathToExclude, pathToInclude)

    val result = PredicateFilteredReader.extractImmediateChildPaths(allPaths, spark)

    // Should filter out the `_common_metadata` path
    result.mkString shouldEqual Array(basePath + pathToInclude + "/abbb.json").mkString
  }
}
