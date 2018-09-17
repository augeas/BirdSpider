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
[container's](https://neo4j.com/docs/operations-manual/current/installation/docker/) RAM


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

