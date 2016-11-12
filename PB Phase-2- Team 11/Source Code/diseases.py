from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Variables that contains the user credentials to access Twitter API 
ACCESS_TOKEN = "1974127951-N1QRXNCsVCywXl67BU1Wx7VlJ1fw2TlScZDY07s"
ACCESS_SECRET = "zLLfZ4ZF9BxvgzjXAkjJFAVFOL4P1i0fLSaZzc6LrnqZJ"
CONSUMER_KEY = "RfqrQLUtEAvwqSmNpGjZydmOn"
CONSUMER_SECRET = "1XdUvxMDfcL5eE89oAo7ZO8UESv6H7HacNa9B0Y7cGFa5MiPjo"

class StdOutListener(StreamListener):

    def on_data(self, data):
        print (data)
        saveFile=open('Disease_Tweets.json','a')
        saveFile.write(data)
        saveFile.write('\n')
        saveFile.close()
        return True

    def on_error(self, status):
        print (status)

if __name__ == '__main__':
    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET) 
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET) 
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=['#heartattack','#cancer','#hiv','#aids','#diabetes','#tuberculosis','#braintumour','#malaria','#dengue','#asthma','#chickenpox','#breastcancer','#lungcancer','#braincancer','#kidneycancer','#livercancer','#leukemia'])
