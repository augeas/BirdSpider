# BirdSpider

## Getting Started

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
docker-compose run --rm birdspider /bin/sh

```

Now you can start the database and Celery worker:

```
./run.sh

```

## Usage

The crawler is controlled by a set of Celery tasks. Having installed celery,
you can call the tasks by name. To get all the Tweets for the [@emfcamp](https://twitter.com/emfcamp) account:

```python
import celery
app = Celery('birdspider',broker='redis://localhost:6379',backend='redis://localhost:6379')
app.send_task('twitter_tasks.getTweets',args=['emfcamp'])   

```

The tweets for the account will be visible in the Neo4j Browser:


