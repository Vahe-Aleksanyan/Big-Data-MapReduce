import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD


object Q4 {
  def main4(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("EventDataReduce")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val startTime = System.nanoTime()
    val megaEventRDD: RDD[String] = sc.textFile("Mega-Event/Mega_Event.csv")

    val header = megaEventRDD.first()
    val dataRDD = megaEventRDD.filter(_ != header)

    val tableHealthPairs = dataRDD.flatMap { line =>
      val cols = line.split(",")
      if (cols.length >= 4) {
        val table = cols(2).trim  //  column index for "table"
        val health = cols(3).trim.toLowerCase  //column index for "test"
        Some((table, health))
      } else {
        None
      }
    }

    val mapped = tableHealthPairs.map { case (table, health) =>
      val isHealthy = health == "not-sick"
      (table, (1, isHealthy))
    }

    val reduced = mapped.reduceByKey { (a, b) =>
      (a._1 + b._1, a._2 && b._2)
    }

    val result = reduced.map { case (table, (count, allHealthy)) =>
      val flag = if (allHealthy) "healthy" else "concern"
      (table, count, flag)
    }

    result
      .map { case (table, count, flag) => s"$table,$count,$flag" }
      .coalesce(1)
      .saveAsTextFile("Q4_Table_Health_Concerns.csv")

    val endTime = System.nanoTime()
    val durationInSeconds = (endTime - startTime) / 1e9
    println(s"Total time taken: $durationInSeconds seconds")
    spark.stop()
  }
}