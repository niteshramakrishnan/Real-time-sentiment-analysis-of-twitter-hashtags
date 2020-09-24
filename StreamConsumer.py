from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch
from textblob import TextBlob
es = Elasticsearch()

def main():
    '''
    main function initiates a kafka consumer, initialize the tweetdata database.
    Consumer consumes tweets from producer extracts features, cleanses the tweet text,
    calculates sentiments and loads the data into postgres database
    '''
    # set-up a Kafka consumer
    consumer = KafkaConsumer("twitter")
    for msg in consumer:
        #output = []
        dict_data = json.loads(msg.value.decode())
        tweet = TextBlob(dict_data["text"])
        
        #Sentiment analyzer
        
        if tweet.sentiment.polarity > 0: 
            reaction = 'positive'
        elif tweet.sentiment.polarity == 0: 
            reaction = 'neutral'
        else: 
            reaction = 'negative'       
        print(tweet)
        # add text and sentiment info to elasticsearch
        es.index(index="tweet",
                  doc_type="test-type",
                 body={"author": dict_data["user"]["screen_name"],
                       "date": dict_data["created_at"],
                       "message": dict_data["text"],
                       "react": reaction,
		       "hashtag": [l["text"] for l in dict_data["entities"]["hashtags"]]})
        print('\n')
        
if __name__ == "__main__":
    main()