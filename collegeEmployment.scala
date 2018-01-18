package com.sparkTutorial.myCodes

package com.sparkTutorial.sparkSql

import javassist.runtime.Desc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object collegeEmployment {

  val UE_MAJOR = "unemployment_ByMajor"


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("CollegeGrads").master("local[1]").getOrCreate()
    val dataFrameReader = session.read

    val collegeInfo = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/all-ages.csv")

    // Unemployment of different major graduates in USA

    val collegeInfoWithSelectedColumns = collegeInfo.select("Major", "Major_category", "Total", "Employed","Unemployed")

    // Engineering disciplines with more than 5000 unemployed graduates in the market
    val minCount=5000

    collegeInfoWithSelectedColumns.filter(collegeInfoWithSelectedColumns.col("Major_category").===("Engineering"))
        .select("Major","Major_category","Unemployed").filter(collegeInfoWithSelectedColumns.col("Unemployed") > minCount).orderBy(desc("Unemployed")).show()




    //Majors with unemployments higher than 5 percent
    val minRate=0.05

    val collegeUnemployedRates = collegeInfo.withColumn(UE_MAJOR,
      collegeInfo.col("Unemployed").divide(collegeInfo.col("Total")))

    import org.apache.spark.sql.functions._
    collegeUnemployedRates.filter(collegeUnemployedRates.col(UE_MAJOR) > minRate).select("Major", "Major_category", UE_MAJOR)
        .orderBy(desc(UE_MAJOR)).show()


  }

}
