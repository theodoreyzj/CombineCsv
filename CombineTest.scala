package CombineCsv

import com.cibc.fctp.ita.utils.SparkTest

class CombinedTest extends SparkTest {
  import spark.implicits._

  test("File Name Filter") {
    val df  = Seq(
      (111, "Asia Prod"),
      (222, "Combined"),
      (333, "/test/Combined.csv")
    ).toDF("Source IP", "fileName")

    val result = CombinedCsv.transform(df)

    val expected = Seq(
      (111, "Asia Prod")
    ).toDF("Source IP", "Environment")

    result.collect() should contain theSameElementsAs expected.collect()
  }

  test("Distinct Values") {
    val df  = Seq(
      (111, "Asia Prod"),
      (111, "Asia Prod")
    ).toDF("Source IP", "fileName")

    val result = CombinedCsv.transform(df)

    val expected = Seq(
      (111, "Asia Prod")
    ).toDF("Source IP", "Environment")

    result.collect() should contain theSameElementsAs expected.collect()
  }

  test("Environment BaseName Remove Extension") {
    val df  = Seq(
      (111, "/test/test/Asia Prod.csv"),
      (222, "/Asia Prod.txt")
    ).toDF("Source IP", "fileName")

    val result = CombinedCsv.transform(df)

    val expected = Seq(
      (111, "Asia Prod"),
      (222, "Asia Prod")
    ).toDF("Source IP", "Environment")

    result.collect() should contain theSameElementsAs expected.collect()
  }

  test("Environment BaseName Duplicates") {
    val df  = Seq(
      (111, "/Asia Prod 1.csv"),
      (222, "/Asia Prod 2.txt")
    ).toDF("Source IP", "fileName")

    val result = CombinedCsv.transform(df)

    val expected = Seq(
      (111, "Asia Prod"),
      (222, "Asia Prod")
    ).toDF("Source IP", "Environment")

    result.collect() should contain theSameElementsAs expected.collect()
  }
}