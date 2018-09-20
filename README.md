# BirdSpider

## Getting Started

### Minimum system requirements

We recommend at least 4GB of RAM (at least 8GB, or better yet 16GB or more is recommended)
Neo4j on its own requires a minimum of 2GB of RAM (16GB or better recommended) according to official documentation

During test runs, simply bringing the full BirdSpider stack up with no tasks running used slightly over 1GB of RAM.
This usage rose further when tasks were running.

### Clone the repo and build the containers:

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
You should also change a Neo4j password and probably increase it's
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
A better interface for this is a TODO

```python
import redis
# assuming redis is accessible on localhost, substitute hostname as appropriate
cache = redis.StrictRedis(host='localhost')
cache.set('user_scrape', 'false')

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


