# BirdSpider

## Getting Started

### Minimum system requirements

We recommend at least 4GB of RAM (at least 8GB, or better yet 16GB or more is recommended)
Neo4j on its own requires a minimum of 2GB of RAM (16GB or better recommended) according to official documentation

During test runs, simply bringing the full BirdSpider stack up with no tasks running used slightly over 1GB of RAM.
This usage rose further when tasks were running.

### Clone the repo and build the containers:

For quick deployment (for testing, or for small scale production use) BirdSpider can be deployed in Docker
as outlined below using the Dockerfiles and docker-compose included in the project.
For larger installations, it may be desirable to split out and run the Neo4j and Solr containers separately to
the BirdSpider tasks (which run on Celery and Redis). This can be done by changing the appropriate settings
for the locations of the Neo4j and Solr hosts .

Make sure you have [Docker](https://docs.docker.com/install/) and [Docker Compose](https://docs.docker.com/compose/)
installed. Clone the repository, make a copy of the example "run.sh" script, then build/pull the containers.

```
git clone https://github.com/augeas/BirdSpider.git
cd BirdSpider
cp eg_run.sh run.sh
docker-compose build

```

### Request Twitter API credentials:

You will need to apply for a [Twitter development account](https://developer.twitter.com/en/apply/user)
and [create a new app](https://apps.twitter.com/). Then you can fill in the credentials in "run.sh".
You should also change a Neo4j password and probably increase its
[container's](https://neo4j.com/docs/operations-manual/current/installation/docker/) RAM.


```
NEO_USER=neo4j \
NEO_PW=birdspider \
NEO_RAM=2G \
CONSUMER_KEY=CONSUMER_KEY \
CONSUMER_SECRET=CONSUMER_SECRET \
OAUTH_TOKEN=OAUTH_TOKEN \
OAUTH_TOKEN_SECRET=OAUTH_TOKEN_SECRET \
ACCESS_TOKEN=ACCESS_TOKEN \
docker-compose run --rm birdspider celery worker -l info -A app

```

Now you can start the database and Celery worker:

```
./run.sh

```

## Usage


The crawler is controlled by a set of Celery tasks. Before starting any tasks, you should verify that
Neo4j is running by visiting the "NEO_HOST" host in a web Browser and logging in with the credentials
specified in "run.sh".
By default this uses application level OAUTH2 authorization. See later is this guide for OAUTH1 instructions.
Having installed celery, you can call the tasks by name. To get all the Tweets for
the [@emfcamp](https://twitter.com/emfcamp) account:

```python
from celery import Celery
app = Celery('birdspider', broker='redis://localhost:6379', backend='redis://localhost:6379')
app.send_task('twitter_tasks.getTweets', args=['emfcamp'])

```

The tweets for the account will be visible in the Neo4j Browser when you expand the account's node by clicking on it.
Find the node with the Cypher query:

```
MATCH (n:twitter_user {screen_name: 'emfcamp'}) RETURN n

```


![simple user query](https://raw.githubusercontent.com/augeas/BirdSpider/master/docs/img/emfcamp_query.png)

### Starting a user scrape

To start a user scrape, call the celery twitter_task seedUser with scrape='True'

```python
from celery import Celery
app = Celery('birdspider', broker='redis://localhost:6379', backend='redis://localhost:6379')
app.send_task('twitter_tasks.seedUser', args=['emfcamp', 'True'])

```

### Halting a running scrape

A user scrape has a stopping condition within it, but you may sometimes wish to stop a scrape early.
Scraping a user checks that the cache key user_scrape == 'true' to signal 'keep going'
If you wish to halt a running scrape before it finishes, for the moment you should change this key to 'false'.
To do this you will need to keep the task_id returned as the result of calling Celery app.send_task
result = app.send_task(task_name,args=[arg0,arg1..., argn])
task_id = result.task_id

this can now be called as a celery task

```python
from celery import Celery
app = Celery('birdspider', broker='redis://localhost:6379', backend='redis://localhost:6379')
task_id = 'id of the task you wish to stop here'
app.send_task('twitter_tasks.stop_scrape', args=[task_id])

```

this can also be done by directly editing th cache keys in redis

```python
import redis
# assuming redis is accessible on localhost, substitute hostname as appropriate
task_id = 'root task id of seedUser task you want to stop goes here'
cache = redis.StrictRedis(host='localhost')
cache.set('user_scrape_' + task_id, 'false')

```

### Passing User level OAUTH1 credentials to twitter tasks ###

As noted above by default BirdSpider now uses application level OAUTH2 authorisation for Twitter API calls.
This means all calls are using one single rate limit on calls. User level OAUTH1 calls can be used to spread calls
across multiple rate limits, as each user will have their own rate limit.
Note: the user should have authorised the application, also note that at the moment this is not
 securing or encrypting the keys and that fact needs fixing!

 example is for scraping a user

 ```python
 import json
 from celery import Celery
 credentials = {
    'oauth1_token': 'your_user_oauth_token_here',
    'oauth1_secret': 'your_user_oauth_secret_here',
 }
 app = Celery('birdspider', broker='redis://localhost:6379', backend='redis://localhost:6379')
 app.send_task('twitter_tasks.seedUser', args=['emfcamp', 'True'], kwargs={ 'credentials': json.dumps(credentials)})

 ```


### Clustering around a twitter user


```python
from celery import Celery
app = Celery('birdspider', broker='redis://localhost:6379', backend='redis://localhost:6379')
app.send_task('clustering_tasks.cluster', args=['emfcamp', 'twitter_user', 'TransFoF'])

```

Cypher queries to view the clustering results:

all clustering nodes: (clusters are members of the clustering run that created them)

```
MATCH (n:clustering) RETURN n

```
clustering for a given seed user:

```
MATCH p=(n:twitter_user {screen_name: 'Dino_Pony'})-[r:SEED_FOR]->() RETURN p LIMIT 25

```


