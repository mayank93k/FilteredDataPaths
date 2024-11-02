package org.scala.spark.filter.paths

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.scala.spark.filter.enums.data.operator.{NumericDataOperator, StringDataOperator}
import org.scala.spark.filter.predicate.{NumericConstraint, StringConstraint}

import scala.annotation.tailrec

object PredicateFilteredReader {

  /**
   * A generic method that filters an array of paths based on the specified predicates.
   *
   * This method applies both numeric and string predicates to the input paths, returning an array of paths that
   * meet the filtering criteria.
   * It is useful for processing datasets where only specific paths are required.
   *
   * @param spark             The SparkSession used to manage the Spark application.
   * @param paths             An array of string paths to be filtered based on the provided predicates.
   * @param numericPredicates A list of numeric predicates that can include values of types Int, Long, Float, and Double.
   *                          These predicates will be used to filter the paths based on numeric conditions.
   * @param stringPredicates  A list of string predicates that will be applied to filter the paths based on string conditions.
   * @return Array[String]    An array of filtered paths that satisfy the given numeric and string predicates.
   */
  @tailrec
  def extractFilteredPaths(spark: SparkSession, paths: Array[Path], numericPredicates: Map[String, List[NumericConstraint]],
                           stringPredicates: Map[String, List[StringConstraint]]): Array[Path] = {
    if ((numericPredicates.isEmpty && stringPredicates.isEmpty) || paths.isEmpty) {
      paths
    } else {
      val colName: String = getLastColumnNameFromPath(paths.head)

      numericPredicates.get(colName) match {
        case Some(predicateList: List[NumericConstraint]) =>
          val pathAfterApplyingFilters: Array[Path] =
            filterPathsUsingNumericPredicates(extractColValueFromPath(paths), predicateList).map(_._2)

          extractFilteredPaths(spark, extractImmediateChildPaths(pathAfterApplyingFilters, spark),
            numericPredicates - colName, stringPredicates)

        case None => stringPredicates.get(colName) match {
          case Some(predicateList: List[StringConstraint]) =>
            val pathAfterApplyingFilters: Array[Path] =
              filterPathsUsingStringPredicate(extractColValueFromPath[String](paths), predicateList)
                .map(_._2)

            extractFilteredPaths(spark, extractImmediateChildPaths(pathAfterApplyingFilters, spark),
              numericPredicates, stringPredicates - colName)

          case None => extractFilteredPaths(spark, extractImmediateChildPaths(paths, spark), numericPredicates,
            stringPredicates)
        }
      }
    }
  }

  /**
   * Retrieves an array of paths leading to the immediate child directories or leaf files
   * from the given input paths.
   *
   * The method handles three cases:
   *
   * 1. **Immediate Child Directory Present**:
   * If the input path contains child directories, it returns paths up to the immediate child directory.
   *    - **Example**:
   *      - Input: `basePath/version=1.0.0/partn_key_yr_hr=20230806/source_system=bua`
   *      - Output: `basePath/version=1.0.0/partn_key_yr_hr=20230806/source_system=bua/processing_stage=standard`
   *
   * 2. **Only Leaf Files Present**:
   * If the input path contains only leaf files, the method returns the paths of those files.
   *    - **Example**:
   *      - Input: `basePath/version=1.0.0/partn_key_yr_hr=20230806/source_system=bua/processing_stage=standard`
   *      - Output: `file.orc`
   *
   * 3. **No Child Directories or Leaf Files**:
   * If the input path does not contain any child directories or leaf files, an empty array is returned.
   *    - **Example**:
   *      - Input: `basePath/version=1.0.0/partn_key_yr_hr=20230806/source_system=bua/processing_stage=standard/file.orc`
   *      - Output: `[]`
   *
   * @param paths An array of paths from which immediate child directories or leaf files will be extracted.
   * @param spark The active Spark session for executing operations.
   * @return An array of paths up to the immediate child directories or leaf files based on the input paths.
   */
  def extractImmediateChildPaths(paths: Array[Path], spark: SparkSession): Array[Path] = {
    val hdConfig = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hdConfig)
    val pathsWithImmediateChildDirectory = paths
      .flatMap(path => fs.listStatus(path).filterNot(pathStatus => {
        val pathName = pathStatus.getPath.getName
        shouldExcludePath(pathName)
      }))

    val aa = pathsWithImmediateChildDirectory.filter(_.isDirectory).map(_.getPath)

    aa.length match {
      case 0 => paths.flatMap(path => fs.listStatus(path).filter(!_.getPath.getName.equals(path.getName))
        .filter(_.isFile).map(_.getPath))
      case _ => aa
    }
  }

  /**
   * Filters an array of paths based on a list of numeric predicates.
   *
   * This method applies a series of numeric predicates to filter the provided paths. Each predicate specifies a condition
   * that the path's numeric component must satisfy. The method operates recursively, applying each predicate in the
   * list to the remaining paths after filtering based on the current predicate.
   *
   * @param paths         An array of tuples, where each tuple contains a path represented as a string
   *                      and its corresponding `Path` object.
   * @param predicateList A list of numeric predicates used to filter the paths. Each predicate defines
   *                      a comparison operation (e.g., less than, greater than).
   * @return An array of tuples containing the filtered paths that satisfy all specified numeric predicates.
   *         If either the input paths or predicate list is empty, the original array of paths is returned.
   */
  @tailrec
  private def filterPathsUsingNumericPredicates(paths: Array[(String, Path)], predicateList: List[NumericConstraint]):
  Array[(String, Path)] = {
    if (paths.isEmpty || predicateList.isEmpty) {
      paths
    } else {
      val predicate: NumericConstraint = predicateList.head
      predicate.operator match {
        case NumericDataOperator.lt => val filteredPaths = paths.filter(path => predicate > path._1)
          filterPathsUsingNumericPredicates(filteredPaths, predicateList.tail)
        case NumericDataOperator.gt => val filteredPaths = paths.filter(path => predicate < path._1)
          filterPathsUsingNumericPredicates(filteredPaths, predicateList.tail)
        case NumericDataOperator.lteq => val filteredPaths = paths.filter(path => predicate >= path._1)
          filterPathsUsingNumericPredicates(filteredPaths, predicateList.tail)
        case NumericDataOperator.gteq => val filteredPaths = paths.filter(path => predicate <= path._1)
          filterPathsUsingNumericPredicates(filteredPaths, predicateList.tail)
        case NumericDataOperator.in => val filteredPaths = paths.filter(path => predicate in path._1)
          filterPathsUsingNumericPredicates(filteredPaths, predicateList.tail)
        case NumericDataOperator.equalTo => val filteredPaths = paths.filter(path => predicate === path._1)
          filterPathsUsingNumericPredicates(filteredPaths, predicateList.tail)
      }
    }
  }

  /**
   * Filters an array of paths based on a list of string predicates.
   *
   * This method applies a series of string predicates to filter the provided paths. It evaluates each path against the
   * predicates in the order they are supplied and returns only those paths that satisfy all specified conditions.
   *
   * @param paths         An array of tuples where each tuple contains a string path and a Path object.
   * @param predicateList A list of string predicates that define the filtering criteria.
   *                      Supported operations include:
   *                       - `in`: Checks if the string is included in the predicate.
   *                       - `equalTo`: Checks for equality with the predicate.
   * @return An array of paths that match all the provided string predicates. If the input paths array is empty or
   *         if the predicate list is empty, the original array of paths is returned without modification.
   */
  @tailrec
  private def filterPathsUsingStringPredicate(paths: Array[(String, Path)], predicateList: List[StringConstraint]): Array[(String, Path)] = {
    if (paths.isEmpty || predicateList.isEmpty) return paths

    val predicate: StringConstraint = predicateList.head
    predicate.operator match {
      case StringDataOperator.in => val filteredPaths = paths.filter(path => predicate in path._1)
        filterPathsUsingStringPredicate(filteredPaths, predicateList.tail)
      case StringDataOperator.equalTo => val filteredPaths = paths.filter(path => predicate === path._1)
        filterPathsUsingStringPredicate(filteredPaths, predicateList.tail)
    }
  }

  /**
   * Transforms an array of paths into an array of tuples containing the column value and the complete path.
   *
   * This method processes each path in the input array to extract a specific column value from the path and returns
   * a tuple for each path that contains the column value along with the original path.
   *
   * For example, given the input:
   * paths = [basePath/version=1.0.0/partn_key_yr_hr=20230304, basePath/version=1.0.0/partn_key_yr_hr=20230305]
   *
   * The output will be:
   * [
   * (20230304, basePath/version=1.0.0/partn_key_yr_hr=20230304),
   * (20230305, basePath/version=1.0.0/partn_key_yr_hr=20230305)
   * ]
   *
   * @param paths An array of strings representing the paths to be transformed.
   * @tparam T The type of the column value being extracted from each path.
   * @return An array of tuples, where each tuple consists of the extracted column value and the corresponding complete path.
   */
  private def extractColValueFromPath[T](paths: Array[Path]): Array[(String, Path)] =
    paths.map { path: Path => (getLastColumnValueFromPath(path), path) }

  /**
   * Extracts the last column value from the specified path.
   *
   * This method analyzes the given path and returns the last segment after the final slash ('/'),
   * which is considered the column value. For example, given the path:
   *
   * basePath/version=1.0.0/partn_key_yr_hr=20230806/source_system=xyz
   *
   * The method will return:
   *
   * xyz
   *
   * @param path A string representing the full path from which the last column value is to be extracted.
   * @return A string representing the name of the last column in the path.
   */
  private def getLastColumnValueFromPath(path: Path): String = path.getName.split('=').last

  /**
   * This method retrieves the name of the last column from the provided path.
   *
   * Example:
   * Given the path: basePath/version=1.0.0/partn_key_yr_hr=20230806/source_system=bua
   * The method will return: source_system
   *
   * @param path The path from which to extract the last column name.
   * @return The name of the last column in the path.
   */
  private def getLastColumnNameFromPath(path: Path): String = path.getName.split('=').head

  /**
   * Determines if a given path name should be excluded from processing based on specific criteria.
   *
   * The method evaluates the path name against exclusion and inclusion rules:
   * - A path name is excluded if:
   *   - It starts with an underscore (`_`) and does not contain an equals sign (`=`).
   *   - It starts with a dot (`.`).
   *   - It ends with the string "._COPYING_".
   *
   * - A path name is included if:
   *   - It starts with `_common_metadata` or `_metadata`.
   *
   * The method returns true if the path name meets the exclusion criteria and does not meet the inclusion criteria.
   *
   * @param pathName The path name to evaluate for filtering.
   * @return True if the path name should be filtered out; otherwise, false.
   */
  private def shouldExcludePath(pathName: String): Boolean = {
    val exclude = (pathName.startsWith("_") && !pathName.contains("=")) || pathName.startsWith(".") || pathName.endsWith("._COPYING_")
    val include = pathName.startsWith("_common_metadata") || pathName.startsWith("_metadata")

    exclude && !include
  }
}
