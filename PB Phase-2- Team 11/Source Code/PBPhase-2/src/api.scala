/*** Created by Team 11 on 11/11/2016.*/

import org.apache.http.client.HttpClient
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods.HttpGet
import oauth.signpost.commonshttp.CommonsHttpOAuthConsumer
import org.apache.commons.io.IOUtils
import org.apache.spark.{SparkConf, SparkContext}


object api {

  val accessToken = "1974127951-N1QRXNCsVCywXl67BU1Wx7VlJ1fw2TlScZDY07s"
  val accessSecret = "zLLfZ4ZF9BxvgzjXAkjJFAVFOL4P1i0fLSaZzc6LrnqZJ"
  val consumerKey = "RfqrQLUtEAvwqSmNpGjZydmOn"
  val consumerSecret = "1XdUvxMDfcL5eE89oAo7ZO8UESv6H7HacNa9B0Y7cGFa5MiPjo"


  def main(args: Array[String]) {

    val consumer = new CommonsHttpOAuthConsumer(consumerKey, consumerSecret)
    consumer.setTokenWithSecret(accessToken, accessSecret)


    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    // Contains SQLContext which is necessary to execute SQL queries
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Reads json file and stores in a variable
    val tweet = sqlContext.read.json("C:\\Users\\nikky\\Desktop\\pbproject\\Disease_Tweets.json")

    //To register tweets data as a table
    tweet.createOrReplaceTempView("tweets")
    println("Below are the 10 Screen Names from Tweets file")
    val users = sqlContext.sql("SELECT distinct user.screen_name as User_Screen_Name from tweets LIMIT 10")
    users.show()
    println("Enter your Screen Name to get Follower Id's: ")
    val name = scala.io.StdIn.readLine()

    val request = new HttpGet("https://api.twitter.com/1.1/followers/ids.json?cursor=-1&screen_name=" + name)
    consumer.sign(request)
    val client = new DefaultHttpClient()
    val response = client.execute(request)

    println(response.getStatusLine().getStatusCode());
    println(IOUtils.toString(response.getEntity().getContent()))
  }
}


