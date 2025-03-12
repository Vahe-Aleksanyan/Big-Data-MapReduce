import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Q1 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("EventDataQuery")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val startTime = System.nanoTime()
    val megaEventDF = spark.read.option("header", "true").csv("Mega-Event/Mega_Event.csv")

    // Query 1: Filter for people who were sick (test = sick)
    val sickPeopleDF = megaEventDF.filter($"test" === "sick")

    //save the result to a new CSV file
    sickPeopleDF.coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv("Q1_Sick_People")

    val endTime = System.nanoTime()
    val durationInSeconds = (endTime - startTime) / 1e9 // Convert from nanoseconds to seconds
    println(s"Total time taken: $durationInSeconds seconds")

    // Stop Spark Session
    spark.stop()
  }
}
