import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import scala.util.Random

object EventDataGenerator {
  def main0(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("EventDataGenerator")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Parameters
    val numRecords = 1500000
    val illnessRatio = 0.001   // 1% of attendees are sick
    val tables = 500
    def randomString(length: Int): String =
      Random.alphanumeric.filter(_.isLetter).take(length).mkString

    def randomName(): String =
      s"${randomString(5 + Random.nextInt(4))} ${randomString(6 + Random.nextInt(5))}"

    // Generate dataset
    val metaEventDF = (1 to numRecords).map { id =>
      val name = randomName()
      val table = Random.nextInt(tables) + 1
      val test = if (Random.nextDouble() < illnessRatio) "sick" else "not-sick"
      (id, name, table, test)
    }.toDF("id", "name", "table", "test")

    // Save Meta-Event.csv
    metaEventDF.coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv("Mega-Event")

    // Save Meta-Event-No-Disclosure.csv
    metaEventDF.drop("test").coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv("Mega-Event-No-Disclosure")

    // Save Reported-Illnesses.csv
    metaEventDF.filter($"test" === "sick").select("id", "test")
      .coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv("Reported-Illnesses")
    spark.stop()
  }
}
