import sys
import time
from settings import *
from twython import Twython
from pymongo import Connection
from datetime import datetime
import dateutil.parser as parser


print 'Connecting to remote database...',
db = Connection(dbloc, slave_okay=True)
storage = db['electiontweets']
consuccess = storage.authenticate(mongousr, mongopwd)

if consuccess:
    print 'Success.'
else:
    print 'Fail. Quitting.'
    sys.exit()


print 'Setting up twitter oauth...',
try:
    twitter = Twython(twitter_token, twitter_secret, oauth_token, oauth_token_secret)
except:
    print 'Fail. Quitting.'
    sys.exit()
print 'Success.'


print 'TS: Starting'
done = False
query_subjects = ["Romney", "Obama"]
while not done:
    print 'TS: Polling...',
    numnew = 0
    try:
        for query in query_subjects:
            search_results = twitter.search(q=query, rpp="100", result_type="current", page=str(1))
            for tweet in search_results["results"]:
                if not storage['tweets'].find_one({'tweet_id': tweet['id_str']}):
                    d = {}
                    d['tweet_id'] = tweet['id_str']
                    d['from_user'] = tweet['from_user'].encode('utf-8')
                    d['from_user_id'] = tweet['from_user_id_str']
                    d['created_at'] = parser.parse(tweet['created_at'])
                    d['text'] = tweet['text'].encode('utf-8')
                    geo = tweet['geo']
                    d['geopos'] = geo['coordinates'] if geo != None else "null"
                    d['scraped_at'] = datetime.now()
                    storage['tweets'].insert(d)
                    numnew += 1
        print str(numnew) + " new entries."
    except KeyboardInterrupt:
        print 'TS: Quitting.',
        done = True
    except:
        print 'TS: Crashed! :(',
    time.sleep(30)
