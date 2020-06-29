// Twitter libraries used to run spark streaming
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, _}

object SparkSentimentalAnalysis2020 {
  def main(args: Array[String]) {

    //To Log only the error messages
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Parse input argument for accessing twitter streaming api
    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " + "< access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    //Set the keys to authenticate data streaming
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val conf = new SparkConf().setAppName("SparkSentimentalAnalysis2020").setMaster("local[*]")
    conf.set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC")
    val ssc = new StreamingContext(conf, Seconds(5))
    val tweets = TwitterUtils.createStream(ssc, None)


    // Apply the sentiment analysis using detectSentiment function
    tweets.foreachRDD { (rdd, time) =>

      rdd.map(t => {
        Map(
          "text" -> t.getText,
          "hashtags" -> t.getHashtagEntities.map(_.getText),
          "language" -> t.getText,
          "sentiment" -> SentimentAnalysisUtils.detectSentiment(t.getText).toString)
      }).filter(x => {
        val hashArray = x("hashtags").asInstanceOf[Array[String]]
        x("language").equals("en") && hashArray.length > 0
      })

      }

def updateTopicCounts(key: String,
                      value: Option[Int],
                      state: State[Int]): (String, Int) =
{
  val existingCount: Int =
    state
      .getOption()
      .getOrElse(0)

  val newCount = value.getOrElse(0)

  state.update(newCount)
  (key, newCount - existingCount)
}
    val stateSpec = StateSpec.function(updateTopicCounts _)
    val window = tweets.transform(rdd => {
      rdd.map(t => {
        Map(

          "text" -> t.getText,
          "hashtags" -> t.getHashtagEntities.map(_.getText),
          "language" -> t.getLang,
          "sentiment" -> SentimentAnalysisUtils.detectSentiment(t.getText).toString)
      })
    }).filter(v => {
      val hashArray = v("hashtags").asInstanceOf[Array[String]]
      v("language").equals("en") && hashArray.length > 0
    }).window(Seconds(60), Seconds(60))

    // calculate the emerging topics using hashtags
    val topics = window.flatMap(t => t("hashtags").asInstanceOf[Array[String]])
      .map(h => (h, 1))
      .reduceByKey(_+_)
      .mapWithState(stateSpec)
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    // extract the emerging topic from the topics
    var emergingTopic = ""
    topics.foreachRDD(rdd => {
      val top = rdd.take(1)
      top.foreach{case (count, topic) => emergingTopic = topic}
    })

    window.foreachRDD(rdd => {
      println("Emerging topic: %s".format(emergingTopic))
      rdd.filter(t => {
        val hashtagList = t("hashtags").asInstanceOf[Array[String]]
        hashtagList.contains(emergingTopic)
      }).coalesce(1).saveAsTextFile("./output_"+emergingTopic)
    })

    ssc.checkpoint("./checkpoint")
    // Start streaming
    ssc.start()
    //Wait for Termination
    ssc.awaitTermination()
  }
}