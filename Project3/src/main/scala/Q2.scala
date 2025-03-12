import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Q2 {
  def main2(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("EventDataQuery")
      .master("local[*]")
      .getOrCreate()
    val startTime = System.nanoTime()
    import spark.implicits._

    // attendees without health status
    val megaEventNoDisclosureDF = spark.read.option("header", "true").csv("Mega-Event-No-Disclosure/Meta_Event_no_disclosure.csv")

    // only sick people
    val reportedIllnessesDF = spark.read.option("header", "true").csv("Reported-Illnesses/Reported_illnesses.csv")

    // Perform an inner join between Mega-Event-No-Disclosure and ReportedIllnesses on the "id" field
    val sickPeopleDF = megaEventNoDisclosureDF
      .join(reportedIllnessesDF, "id")
      .filter($"test" === "sick")

    // Select the relevant columns: id, table
    val resultDF = sickPeopleDF.select("id", "table")

    //  save the result to a new CSV file
    resultDF.coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv("Q2_Sick_People_Attendees")

    val endTime = System.nanoTime()
    val durationInSeconds = (endTime - startTime) / 1e9
    println(s"Total time taken: $durationInSeconds seconds")

    spark.stop()
  }
}
