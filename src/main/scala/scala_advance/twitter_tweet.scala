package scala_advance

import org.apache.spark.SparkConf
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.streaming.twitter.TwitterUtils

//import org.apache.log4j.Logger
//import org.apache.log4j.Level



object twitter_tweet {

  def main(args: Array[String]): Unit = {

    //Logger.getLogger("org").setLevel(Level.OFF)
    //Logger.getLogger("akka").setLevel(Level.OFF)

    //val session = SparkSession.builder().appName("twitter").master("local").getOrCreate()
    val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.sparkContext.setLogLevel("ERROR")


    val consumerKey= "s4Jlz5yGTBMTETPMzrWlivUlc"
    val consumerSecret= "Nnsch5M2fekJlHI7tmU6zSKudywUEyBAnrptsl1rYTxMJDSfS9"
    val accessToken = "863219944300756995-HtGYFbO7fO3mrCepTmmrZQ9d3cAER0t"
    val accessTokenSecret= "rKrO88snh5uBVti509ytWpCAgZSOqTUGywsZxsGbN3sir"

    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)

    val auth = new OAuthAuthorization(cb.build)


    val tweets = TwitterUtils.createStream(ssc, Some(auth))


    val statuses = tweets.filter(_.getLang() == "en")
      .map(status =>
      {val string_tweet = status.getText()
      if (string_tweet.contains("India"))
    {
      string_tweet
    }
    }
    )
    statuses.print()

    ssc.start()
    ssc.awaitTermination()

  }


}
