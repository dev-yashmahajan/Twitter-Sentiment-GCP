# Twitter Sentiment Analysis In  Google Cloud Platform



The project is about creating a cloud project which constantly pulls tweets from twitter and passes it through a ML model in GCP to get the sentiment of that tweet.


## Architecture

![Architecture](/Images/Architecture.png)  


## Compute Engine

A Virtual Machine is created in Compute Engine to run a script to pull the tweets.  
A python library called [Tweepy](https://www.tweepy.org/) is used for this purpose.  
Using Tweepy we listen and pull tweets which match a certain list of relevant Hastags such as "#Spiderman" for the new movie released.  
The relevant tweets are then pulled and send to a Google PubSub.  
The script is stored in a Cloud storage bucket and copied to the VM for it to run. 

## Cloud PubSub

A cloud PubSub is used as a middleware which helps store the tweets and deliver them to the entire pipeline.  
This is helpful as tweets might potentially flow at a higher rate than the pipeline can pickup.  


## ML Model (AI Platform)

To determine the sentiment of each tweet we use a ML model.  
I trained a ML model and used the trained model in the AI platform to serve it.  
The model is a two part model - preprocessing and CNN based model.  
The preprocessing part preprocesses the incoming tweets for our model to use.  
The Model is a CNN based model written in Keras tensorflow.  
The model was trained on a dataset called [Sentiment140](http://help.sentiment140.com/).

Referred this [blog post by Google](https://cloud.google.com/blog/products/ai-machine-learning/ai-in-depth-creating-preprocessing-model-serving-affinity-with-custom-online-prediction-on-ai-platform-serving) to serve the model. 



## Apache Beam pipeline (Cloud Dataflow)

Since the tweets come in a streaming fashion I used a Apache beam streaming pipleine to mange it.  

Steps of the pipeline:  
##### 1.  Pull tweets from PubSub
##### 2.  Batch the tweets 
##### 3.  Classify the tweets using the ML model
##### 4.  In parallel Write the tweets in BigQuery and
##### 5.  Calculate the mean sentiment every 10s and store it in another BigQuery table.

