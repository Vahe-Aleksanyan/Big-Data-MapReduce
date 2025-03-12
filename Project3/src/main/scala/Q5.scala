import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object Q5 {
  def main5(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NotifyHealthyPeople")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    val startTime = System.nanoTime()

    // Step 1: Load Reported Illnesses file and build a broadcast set of sick IDs
    val reportedIllnessesRDD: RDD[String] = sc.textFile("Reported-Illnesses/Reported_illnesses.csv")
      .filter(line => !line.startsWith("id"))  // Remove header
      .map(line => line.split(",")(0).trim)    // Extract sick person ID

    val sickIDs: Set[String] = reportedIllnessesRDD.collect().toSet
    val sickIDsBroadcast = sc.broadcast(sickIDs)

    // Step 2: Load Mega Event file (id, name, table)
    val megaEventRDD: RDD[(String, String, String)] = sc.textFile("Mega-Event-No-Disclosure/Meta_Event_no_disclosure.csv")
      .filter(line => !line.startsWith("id"))  // Remove header
      .map { line =>
        val cols = line.split(",")
        (cols(0).trim, cols(1).trim, cols(2).trim)
      }

    // Step 3: Map each record to (table, (id, name, isSick))
    // Here, we flag whether the person is sick based on the broadcast set.
    val mappedByTable: RDD[(String, (String, String, Boolean))] = megaEventRDD.map { case (id, name, table) =>
      val isSick = sickIDsBroadcast.value.contains(id)
      (table, (id, name, isSick))
    }

    // Step 4: Reduce by table to compute a flag for each table indicating if any person is sick.
    // For each table key, we reduce the Boolean flags by OR-ing them.
    val tableSickFlags: RDD[(String, Boolean)] = mappedByTable
      .mapValues { case (_, _, isSick) => isSick }
      .reduceByKey((flag1, flag2) => flag1 || flag2)

    // Collect the tables that have at least one sick person.
    val sickTables: Set[String] = tableSickFlags.filter(_._2).map(_._1).collect().toSet
    val sickTablesBroadcast = sc.broadcast(sickTables)

    // Step 5: Identify healthy people at risk.
    // A person is healthy if their ID is not in sickIDs,
    // and at risk if they are at a table that has at least one sick person.
    val healthyAtRiskRDD: RDD[(String, String, String)] = megaEventRDD.filter { case (id, name, table) =>
      sickTablesBroadcast.value.contains(table) && !sickIDsBroadcast.value.contains(id)
    }

    // Step 6: Save the result as a CSV file.
    healthyAtRiskRDD
      .map { case (id, name, table) => s"$id,$name,$table" }
      .coalesce(1)  // Optionally combine into a single output file
      .saveAsTextFile("Q5_Healthy_People_At_Risk")

    val endTime = System.nanoTime()
    val durationInSeconds = (endTime - startTime) / 1e9
    println(s"Total time taken: $durationInSeconds seconds")
    spark.stop()
  }
}
