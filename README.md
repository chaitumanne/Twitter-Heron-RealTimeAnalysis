# Twitter-Heron-RealTimeAnalysis

YouTube Link : https://www.youtube.com/watch?v=-lTSz09xOFw&feature=youtu.be

Presentation Link: https://www.dropbox.com/s/x8zhtdxjr978l04/Twitter-Heron-RealTimeAnalysis.pptx?dl=0

#Heron

Heron is a platform developed by Twitter for real-time analysis. It is developed on top of Storm and is fully compatible with Storm API. 

#Hackathon_CalmCoders

UseCase:

Real Time Trends - Twitter is a continuous stream of tweets and in this stream, you need to identify the emerging top 10 trends. In order to simplify the problem break the tweet into words and identify the hash tags. Continously keep track of the top 10 hash tags and if there is tweet that bears this hash tag, add them to result and emit - so that you get the trends and the tweets supporting the trends.

Real Time BI - Tweets contain a lot of metadata - in fact each Tweet from the fire hose might be as long as 1K. Based on the metadata, you can find out several trends - break down by mobile/web, what mobile operating system, a crude sense of location, etc

Heatmap of Tweets - Some % of tweets contains latitude and longitude. Get those tweets continuously and update the google maps to see where the tweets are coming from and view them globally in a map, If there are dense set of tweets in an area it will be red, while cooler areas will be blue and some gradation in coloring between the two.

Description:

Use case is completely developed in java using Twitter Heron. Topology consists of a "Spout" and seven "Bolts".

Spout: It is a firhose where the streaming of tweets are generated. The data is then emitted to different bolts where actual analytics is done.

SourceCountBolt: In this bolt we retrieve the information from "Source" attribute. That is, from which source the tweet has been made. For example, whether the tweet is made from Android, Iphone, Ipad, Deesktop, etc. The count for each individual source is calcualted and the overall count of the tweets is calculated. Then the respective counts are stored in MongoDB from where the data is collected and displayed on UI dynamically.

CoordinatesBolt: In this bolt we retrieve the Latitude and Longitude of the person who made the tweet. All the latitude and logitude details are collected for all the tweets that come. These details are then emitted to do furthur analysis in the next bolt.

LocationBolt: In this bolt the latitude and logitude details that were emiited from the precious bolt are collected. The data is then stored into MongoDB from where the data is collected and displayed in the UI dynamically.

CountryFilterBolt: In this bolt the country details are gathered from each and every tweet in order to know the place from where more number of tweets are coming in a given time. These countrry details are then emitted for furthur analysis in the next bolt.

CountryCountBolt: In this bolt all the countries details and their respective counts are stored. Then the top three countries from where more number of tweets are made is then calculated. These are details are then stored to MongoDb to displaye dynamically on UI.

HashTagsBolt: In this bolt all the different hashtags are present in each and every tweet are collected and then emitted in order to do furthur analysis on the hashtags.

TopTenHashTagsBolt: In this bolt the hash tags that are emitted from the previous bolt are gathered and then the count of the hashtags are calculated. This is done in order to findout the most trending HashTag, Top ten hashtags. These details are then stored into the MongoDB which are later retrieved to display on the front end.

SentimentAnalysisBolt: In this bolt the sentiment is calculated for each and every tweet that comes to firehose in order to know whether the tweet was made in positive, negative or neutral mood. The overall count is then stored in MongoDB, retrieved it on the UI and then a PIE chart is used to display the comparitve results.
