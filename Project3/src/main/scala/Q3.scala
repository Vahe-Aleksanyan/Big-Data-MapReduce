import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object Q3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("EventDataMapReduce")
      .master("local[*]")
      .getOrCreate()



    val sc = spark.sparkContext
    val startTime = System.nanoTime()
    // Load Mega-Event.csv as an RDD
    val megaEventRDD: RDD[String] = sc.textFile("Mega-Event/Mega_Event.csv")

    // Define header and remove it
    val header = megaEventRDD.first()
    val dataRDD = megaEventRDD.filter(row => row != header)  // Filter out the header

    // Map Phase: Extracting (table, id, name, test) for each person
    val peopleRDD = dataRDD.map { line =>
      val columns = line.split(",")
      val id = columns(0)
      val name = columns(1)
      val table = columns(2).toInt
      val test = columns(3)
      (table, (id, name, test))
    }

    // Group by table
    val groupedByTableRDD = peopleRDD.groupByKey()

    // Reduce Phase
    val resultRDD = groupedByTableRDD.flatMap { case (table, people) =>
      val sickPeople = people.filter(_._3 == "sick").map(_._1).toSet
      val healthyPeople = people.filter(_._3 == "not-sick").map(p => (p._1, p._2))

      // If there is at least one sick person at the table, return all healthy people
      if (sickPeople.nonEmpty) healthyPeople else Seq()
    }


    // Save result to a new file
    resultRDD.coalesce(1)
      .map { case (id, name) => s"$id,$name" }  // Convert to CSV format
      .saveAsTextFile("Q3_Healthy_People_Sitting_With_Sick_MapReduce")


    val endTime = System.nanoTime()
    val durationInSeconds = (endTime - startTime) / 1e9
    println(s"Total time taken: $durationInSeconds seconds")
    spark.stop()
  }
}
