# Principles-of-Big-Data-Management : Disease Analysis

This repository contains all the documents related to PB-Fall 2016.

<h3>About the project</h3>
We choose ‘Diseases’ as our topic to do big data analysis. Based on twitter tweets, we predicted some interesting analysis on Diseases using thousands of tweets tweeted by different people. First we collected the tweets from twitter API based on some key words related to Disease. After that, we analyzed the data that we have collected. By using the analysis, we written some interesting SQL queries useful to give a proper result for the analysis.

<h4><i>Query 1: Popular Tweets on Different Diseases </i></h4>
In this query, we are fetching the diseases and its tweets count in the file. This query is written using RDD, where we are fetching the count of diseases using hashtags using filter and the count is printed further.

<img src="https://github.com/cmoulika009/Principles-of-Big-Data-Management/blob/master/PB%20Phase-3-%20Team-11/Images/1.png">

<img src="https://github.com/cmoulika009/Principles-of-Big-Data-Management/blob/master/PB%20Phase-3-%20Team-11/Images/2.png">

<h4><i>Query 3: Countries that tweeted more on Diseases (Google Maps)</i></h4>
In this query, the top countries that tweeted more on diseases is fetched. First the location in tweets are fetched from tweets file and count is displayed as shown below. The data is stored in .csv format and the file is read and Visualization is done on Google Maps.
<img src="https://github.com/cmoulika009/Principles-of-Big-Data-Management/blob/master/PB%20Phase-3-%20Team-11/Images/3.png">

<h4><i>Query 4: Popular Hashtags</i></h4>
In this query, we took popular hash tags text file from blackboard and performed JOIN operation with hash tags from diseases tweets file. The fetched data is stored in .csv format to do visualization.
<img src="https://github.com/cmoulika009/Principles-of-Big-Data-Management/blob/master/PB%20Phase-3-%20Team-11/Images/5.png">

<h4><i>Query 5: On which day of week, more tweets are done on diseases</i></h4>
In this query, data is fetched based on which day of week more tweets are done on Diseases. Initially created_at is fetched from tweets file and count of tweets is done on each day of week.
<img src="https://github.com/cmoulika009/Principles-of-Big-Data-Management/blob/master/PB%20Phase-3-%20Team-11/Images/6.png">

<h4><i>Query 6: Top 10 Users Tweeted on Diseases</i></h4>
In this query the we are fetching top 10 users who tweeted more on diseases. This query is written using RDD. Initially for each disease, the top tweeted user is fetched and UNION RDD is used to club all the diseases. The results are stored in .csv file to do visualization
<img src="https://github.com/cmoulika009/Principles-of-Big-Data-Management/blob/master/PB%20Phase-3-%20Team-11/Images/7.png">

<h4><i>Query 7: Follower Id’s count using Twitter API</i></h4>
Twitter Get Followers ids API is used. A query to display five screen names from the tweets file is written. When the query is executed a table with ten screen names is displayed in the table.

<b>Val request = new HttpGet("https://api.twitter.com/1.1/followers/ids.json?cursor=-1&screen_name=" + name)</b>

First the user is given a Choice to enter a screen name of his choice. Once the screen name has been inputted the follower’s id

<img src="https://github.com/cmoulika009/Principles-of-Big-Data-Management/blob/master/PB%20Phase-3-%20Team-11/Images/9.png" width=200px height=200px>

Once screen name RevistaCOFEPRIS is entered the follower id’s count are displayed as shown below

<img src="https://github.com/cmoulika009/Principles-of-Big-Data-Management/blob/master/PB%20Phase-3-%20Team-11/Images/10.png">

<img src="https://github.com/cmoulika009/Principles-of-Big-Data-Management/blob/master/PB%20Phase-3-%20Team-11/Images/8.png">

<h3>Related Links</h3>
<b>Phase-1 Document:</b> https://github.com/cmoulika009/Principles-of-Big-Data-Management/blob/master/PB%20Phase-1-%20Team%2011/PRINCIPLES%20OF%20BIG%20DATA%20MANAGEMENT%20PHASE%201.pdf

<b>Phase-2 Document:</b> https://github.com/cmoulika009/Principles-of-Big-Data-Management/blob/master/PB%20Phase-2-%20Team%2011/PB%20Phase-2%20Team-11.pdf

<b>Final Project Document:</b> https://github.com/cmoulika009/Principles-of-Big-Data-Management/blob/master/PB%20Phase-3-%20Team-11/PB%20Phase-3%20Team-11.pdf

<b>Youtube Video:</b> https://youtu.be/dRO-2chnycM
