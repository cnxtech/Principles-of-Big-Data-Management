/** Created by Team11 on 11/1/2016. */

import javolution.io
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.types.{DateType, FloatType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{StructType, StructField, StringType};
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._

// For implicit conversions from RDDs to DataFrames

object Disease {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    // Contains SQLContext which is necessary to execute SQL queries
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Reads json file and stores in a variable
    val tweet = sqlContext.read.json("C:\\Users\\nikky\\Desktop\\pbproject\\Disease_Tweets.json")

    //To register tweets data as a table
    tweet.createOrReplaceTempView("tweets")

    val disCat = sqlContext.sql("SELECT user.name as UserName,user.location as loc,text,created_at," +
      "CASE WHEN text like '%heartattack%' THEN 'HEART STROKE'" +
      "WHEN text like '%cancer%' THEN 'CANCER'" +
      "WHEN text like '%hiv%' THEN 'HIV'" +
      "WHEN text like '%aids%' THEN 'AIDS'" +
      "WHEN text LIKE '%diabetes%' THEN 'DIABETES'" +
      "WHEN text like '%tuberculosis%' THEN 'TUBERCULOSIS'" +
      "WHEN text like '%braintumour%' THEN 'BRAIN TUMOUR'" +
      "WHEN text like '%malaria%' THEN 'MALARIA'" +
      "WHEN text like '%dengue%' THEN 'DENGUE'" +
      "WHEN text like '%asthma%' THEN 'ASTHMA'" +
      "WHEN text like '%chickenpox%' THEN 'CHICKENPOX'" +
      "END AS diseaseType from tweets where text is not null")

    disCat.createOrReplaceTempView("disCat2")

    println("Enter any one of the following query to get data")
    println("1.Query-1:Which disease has more tweets")
    println("2.Query-2:Which user tweeted more on which disease")
    println("3.Query-3:Which state tweeted more on which disease")
    println("4.Query-4:On which day more tweets are done")
    println("5.Query-5:Compare disease hashtags with blackboard tags")
    println("Enter any one of the following query to get data:")
    val count = scala.io.StdIn.readLine()
    count match {
      case "1" =>
        /*--------------------Query 1: This query fetches the sports and its popularity based on tweets-----------------------*/
        val textFile = sc.textFile("C:\\Users\\nikky\\Desktop\\pbproject\\Disease_Tweets.json")
        val heartattack = (textFile.filter(line => line.contains("#heartattack")).count())
        val cancer = (textFile.filter(line => line.contains("#cancer")).count())
        val hiv = (textFile.filter(line => line.contains("#hiv")).count())
        val aids = (textFile.filter(line => line.contains("#aids")).count())
        val diabetes = (textFile.filter(line => line.contains("#diabetes")).count())
        val tuberculosis = (textFile.filter(line => line.contains("#tuberculosis")).count())
        val braintumour = (textFile.filter(line => line.contains("#braintumour")).count())
        val malaria = (textFile.filter(line => line.contains("#malaria")).count())
        val dengue = (textFile.filter(line => line.contains("#dengue")).count())
        val asthma = (textFile.filter(line => line.contains("#asthma")).count())
        val chickenpox = (textFile.filter(line => line.contains("#chickenpox")).count())

        println("********************************************")
        println("Number of users tweeted on different disease")
        println("********************************************")
        println("Heart Attack : %s".format(heartattack))
        println("Cancer : %s".format(cancer))
        println("HIV : %s".format(hiv))
        println("AIDS : %s".format(aids))
        println("DIABETES : %s".format(diabetes))
        println("TUBERCULOSIS : %s".format(tuberculosis))
        println("BRAIN TUMOUR : %s".format(braintumour))
        println("MALARIA : %s".format(malaria))
        println("DENGUE : %s".format(dengue))
        println("ASTHMA : %s".format(asthma))
        println("CHICKENPOX : %s".format(chickenpox))

      /*-----------------------------Query 2:  User who tweeted most on which sport--------------------------------------------*/
      case "2" =>

        val r1 = sqlContext.sql("SELECT UserName,'HEART STROKE' as diseaseType,count(*) as count FROM disCat2 WHERE diseaseType='HEART STROKE' " +
          "group by UserName order by count desc limit 1")
        val r2 = sqlContext.sql("SELECT UserName,'CANCER' as diseaseType,count(*) as count FROM disCat2 WHERE diseaseType='CANCER' " +
          "group by UserName order by count desc limit 1 ")
        val r3 = sqlContext.sql("SELECT UserName,'HIV' as diseaseType,count(*) as count FROM disCat2 WHERE diseaseType='HIV' " +
          "group by UserName order by count desc limit 1 ")
        val r4 = sqlContext.sql("SELECT UserName,'AIDS' as diseaseType,count(*) as count FROM disCat2 WHERE diseaseType='AIDS' " +
          "group by UserName order by count desc limit 1 ")
        val r5 = sqlContext.sql("SELECT UserName,'DIABETES' as diseaseType,count(*) as count FROM disCat2 WHERE diseaseType='DIABETES' " +
          "group by UserName order by count desc limit 1 ")
        val r6 = sqlContext.sql("SELECT UserName,'TUBERCULOSIS' as diseaseType,count(*) as count FROM disCat2 WHERE diseaseType='TUBERCULOSIS' " +
          "group by UserName order by count desc limit 1 ")
        val r7 = sqlContext.sql("SELECT UserName,'BRAIN TUMOUR' as diseaseType,count(*) as count FROM disCat2 WHERE diseaseType='BRAIN TUMOUR' " +
          "group by UserName order by count desc limit 1 ")
        val r8 = sqlContext.sql("SELECT UserName,'MALARIA' as diseaseType,count(*) as count FROM disCat2 WHERE diseaseType='MALARIA' " +
          "group by UserName order by count desc limit 1 ")
        val r9 = sqlContext.sql("SELECT UserName,'DENGUE' as diseaseType,count(*) as count FROM disCat2 WHERE diseaseType='DENGUE' " +
          "group by UserName order by count desc limit 1")
        val r10 = sqlContext.sql("SELECT UserName,'ASTHMA' as diseaseType,count(*) as count FROM disCat2 WHERE diseaseType='ASTHMA' " +
          "group by UserName order by count desc limit 1")
        val r11 = sqlContext.sql("SELECT UserName,'CHICKENPOX' as diseaseType,count(*) as count FROM disCat2 WHERE diseaseType='CHICKENPOX' " +
          "group by UserName order by count desc limit 1 ")

        val rdd1 = r1.union(r2).union(r3).union(r4).union(r5).union(r6).union(r7).union(r8).union(r9) union (r10).union(r11)

        println("****************************************")
        println("Which user tweeted more on which Disease")
        println("****************************************")
        rdd1.show()

      //TopTweets.collect().foreach(println)
      //TopTweets.write.format("com.databricks.spark.csv").option("header", "true").save("C:\\Users\\nikky\\Desktop\\pbproject\\TopTweetsBySports.csv")

      /*-----------------------------------Query 3: US states with more popular sport-------------------------------------*/
      case "3" =>
        val stateWiseCnt = sqlContext.sql(
          """ SELECT Case
            |when user.location LIKE '%Alaska%' then 'Alaska'
            |when user.location LIKE '%Arizona%' then 'Arizona'
            |when user.location LIKE '%Arkansas%' then 'Arkansas'
            |when user.location LIKE '%California%' then 'California'
            |when user.location LIKE '%Colorado%' then 'Colorado'
            |when user.location LIKE '%Connecticut%' then 'Connecticut'
            |when user.location LIKE '%Delaware%' then 'Delaware'
            |when user.location LIKE '%Florida%' then 'Florida'
            |when user.location LIKE '%Georgia%' then 'Georgia'
            |when user.location LIKE '%Hawaii%' then 'Hawaii'
            |when user.location LIKE '%Idaho%' then 'Idaho'
            |when user.location LIKE '%Illinois%' then 'Illinois'
            |when user.location LIKE '%Indiana%' then 'Indiana'
            |when user.location LIKE '%Iowa%' then 'Iowa'
            |when user.location LIKE '%Kansas%' then 'Kansas'
            |when user.location LIKE '%Kentucky%' then 'Kentucky'
            |when user.location LIKE '%Louisiana%' then 'Louisiana'
            |when user.location LIKE '%Maine%' then 'Maine'
            |when user.location LIKE '%Maryland%' then 'Maryland'
            |when user.location LIKE '%Massachusetts%' then 'Massachusetts'
            |when user.location LIKE '%Michigan%' then 'Michigan'
            |when user.location LIKE '%Minnesota%' then 'Minnesota'
            |when user.location LIKE '%Mississippi%' then 'Mississippi'
            |when user.location LIKE '%Missouri%' then 'Missouri'
            |when user.location LIKE '%Montana%' then 'Montana'
            |when user.location LIKE '%Nebraska%' then 'Nebraska'
            |when user.location LIKE '%Nevada%' then 'Nevada'
            |when user.location LIKE '%NewHampshire%' then 'New Hampshire'
            |when user.location LIKE '%NewJersey%' then 'New Jersey'
            |when user.location LIKE '%NewMexico%' then 'NewMexico'
            |when user.location LIKE '%NewYork%' then 'New York'
            |when user.location LIKE '%NorthCarolina%' then 'North Carolina'
            |when user.location LIKE '%NorthDakota%' then 'North Dakota'
            |when user.location LIKE '%Ohio%' then 'Ohio'
            |when user.location LIKE '%Oklahoma%' then 'Oklahoma'
            |when user.location LIKE '%Oregon%' then 'Oregon'
            |when user.location LIKE '%Pennsylvania%' then 'Pennsylvania'
            |when user.location LIKE '%RhodeIsland%' then 'Rhode Island'
            |when user.location LIKE '%SouthCarolina%' then 'South Carolina'
            |when user.location LIKE '%SouthDakota%' then 'South Dakota'
            |when user.location LIKE '%Tennessee%' then 'Tennessee'
            |when user.location LIKE '%Texas%' then 'Texas'
            |when user.location LIKE '%Utah%' then 'Utah'
            |when user.location LIKE '%Vermont%' then 'Vermont'
            |when user.location LIKE '%Virginia%' then 'Virginia'
            |when user.location LIKE '%Washington%' then 'Washington'
            |when user.location LIKE '%WestVirginia%' then 'West Virginia'
            |when user.location LIKE '%Wisconsin%' then 'Wisconsin'
            |when user.location LIKE '%Wyoming%' then 'Wyoming'
            | end as US_State,text from tweets where text is not null""".stripMargin)
        stateWiseCnt.createOrReplaceTempView("stateWiseDataCnt")

        val stateWiseDataCnt = sqlContext.sql("select US_State, count(text) as State_Tweet_Count " +
            "from stateWiseDataCnt where US_State is not null " +
            "and text is not null group by US_State,text order by count(text) desc")

        println("*****************************************")
        println("Which US State Tweeted More On Which Disease")
        println("*****************************************")
        stateWiseDataCnt.show();
      //stateSportType2.collect().foreach(println)
      //stateSportType2.write.format("com.databricks.spark.csv").option("header", "true").save("C:\\Users\\nikky\\Desktop\\pbproject\\SportsByState.csv")
      /*-------------------------------Query 4 : On which Day More Tweets are done-----------------------------------*/
      case "4" =>
        val day_data = sqlContext.sql("SELECT substring(user.created_at,1,3) as day from tweets where text is not null")

        day_data.createOrReplaceTempView("day_data")

        val days_final = sqlContext.sql(
          """ SELECT Case
            |when day LIKE '%Mon%' then 'MONDAY'
            |when day LIKE '%Tue%' then 'TUESDAY'
            |when day LIKE '%Wed%' then 'WEDNESDAY'
            |when day LIKE '%Thu%' then 'THURSDAY'
            |when day LIKE '%Fri%' then 'FRIDAY'
            |when day LIKE '%Sat%' then 'SATURDAY'
            |when day LIKE '%Sun%' then 'SUNDAY'
            | else
            | null
            | end as day1 from day_data where day is not null""".stripMargin)

        days_final.createOrReplaceTempView("days_final")

        val res = sqlContext.sql("SELECT day1 as Day,Count(*) as Day_Count from days_final where day1 is not null group by day1 order by count(*) desc")

        println ("**********************************")
        println("On Which Day More Tweets Were Done")
        println("**********************************")
        res.show()

      /*-------------------------------Query 5 : Blackboard Hash Tags Join -----------------------------------*/
      case "5" =>
        val hashtag = sqlContext.read.json(
          "C:\\Users\\nikky\\Downloads\\HashtagsTopics.txt")
        //To register tweets data as a table

        //hashtag.createOrReplaceTempView("hashtable")
        val hasdf = hashtag.toDF().withColumnRenamed("_Corrupt_Record", "name")
        hasdf.createOrReplaceTempView("hashtag")
        val query = sqlContext.sql(
          "SELECT t.text as Text,d.name as HashTag from tweets t JOIN hashtag d ON t.text like CONCAT('%', d.name, '%')")
        println("************************************************")
        println("Same Hash Tags from Tweets and Blackboard Tweets")
        println("************************************************")
        query.show()
    }
  }
}
