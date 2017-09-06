
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkConfig extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val sparkConf: SparkConf = new SparkConf().setMaster("local[2]")
    .setAppName("SparkAssignment")

  val streamingContext = new StreamingContext(sparkConf,Seconds.apply(2))

  val lines: ReceiverInputDStream[String] = streamingContext.receiverStream(new CustomReceiver)

  val words: DStream[String] = lines.flatMap(_.split(" "))

  val wordCounts: DStream[(String, Int)] = words.map(word=> (word,1)).reduceByKey(_ + _)

  wordCounts.foreachRDD(a=> Logger.getLogger("Stream").info(a.collect().toList))

  streamingContext.start()

  streamingContext.awaitTermination()

}
