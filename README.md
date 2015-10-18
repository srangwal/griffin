## Griffin
A service to sync a blob of data to a cluster of machines across multiple data center using Apache Kafka. Interaction
with Griffin service is via a REST API.

### Requirements
- Gradle 2.5+
- protoc 2.5+
- Java 1.7


### Build
Prepare a single jar with
```sh
$> gradle build
```

### Running Griffin
#### Required configuration
##### Griffin
Griffin requires a configuration file `griffin.conf` in the local directory. A sample `griffin.conf` is included in the
`examples` directory. Please update the file with the required information.

- Griffin.ZkServers: `LIST_OF_ZKSEVERS`
- Griffin.KafkaBrokersList: `LIST_OF_KAFKA_BROKERS_WITH_PORTS`
- Griffin.Error.Email.Recipients: `EMAIL_ID`
- serverid: `UNIQUE_SERVER_ID`
- dcname: `DATACENTER_NAME`

##### Apache Kafka
Griffin requires that Apache Kafka has automatic topic creation and topic deletion enabled, i.e.,
```sh
auto.create.topics.enable=true
delete.topic.enable=true
```
#### Griffin Example
We will run two instances of griffin. Each instance of griffin is required to have a unique `serverid`. Zookeeper
cluster specified using `ZkServers` and Kafka cluster specified using `KafkaBrokersList` should be same for all griffin
services that share the same `dcname`

Open two terminals. In first terminal start the first instance of griffin as
```sh
$> cd examples
$> java -jar ../build/libs/griffin-0.1.0.jar
```

In second terminal change `serverid` in `examples/griffin.conf` to `griffin-2` and `server.port` in
`examples/application.properties` to `8081` and start another instance of griffin as
```sh
$> cd examples
$> java -jar ../build/libs/griffin-0.1.0.jar
```

In yet another terminal we can interact with these griffin services. We will push a file to one instance of griffin and
we will observe it getting replicated to another instance of griffin.
```sh
$> curl -i http://localhost:8080/griffin/localrepo/
$> curl -i http://localhost:8081/griffin/localrepo/
```

We can push a file to one of the griffin instances as
```sh
$> curl -i -F blobname=gradle -F dest='.*' -F file=@build.gradle http://localhost:8080/griffin/localrepo
```

After a short while one can observe the file available in local repository of both griffin services
```sh
$> curl -i http://localhost:8080/griffin/localrepo/
$> curl -i http://localhost:8081/griffin/localrepo/
```

Check out REST API section for full description of rest calls above.

### Docker
**Requires**
- Docker 1.8+
- Docker compose (https://docs.docker.com/compose/install/)

#### Creating a docker image
A docker container containing griffin can be built using
```sh
$> gradle buildDocker
```
This will create a docker image `srangwal/griffin` containing griffin service.


#### Starting a full cluster using docker-compose
A `docker-compose.yml` to create a full cluster of zookeeper, kafka, and griffin is included (one is _not_ required to
build griffin image for the step above). To create a cluster, update `KAFKA_ADVERTISED_HOST_NAME` in
`docker-compose.yml` to the IP address of the host machine and start the cluster as
```sh
$> docker-compose up -d
$> docker-compose scale kafkalax1=3
$> docker-compose scale griffinlax1=3
```
This might take some time as it requires docker to download few images.

The cluster is now up and running. To get local repository of first griffin service
```sh
$> curl -i http://$(docker port griffin_griffinlax1_1 8080)/griffin/localrepo
```

To push a file (to one of the griffin container)
```sh
$> curl -i# -F blobname=gradle -F dest='.*' -F file=@build.gradle http://$(docker port griffin_griffinlax1_1 8080)/griffin/localrepo
```

We can now observe (after a short delay) that the file is available in all griffin containers
```sh
$> curl -i http://$(docker port griffin_griffinlax1_1 8080)/griffin/localrepo
$> curl -i http://$(docker port griffin_griffinlax1_2 8080)/griffin/localrepo
$> curl -i http://$(docker port griffin_griffinlax1_3 8080)/griffin/localrepo
```


### REST API
#### GET /localrepo
List all the files in the local repository of that instance of griffin

#### PUT,POST /localrepo/
Push a given file to the specified set of machines

|Parameter|Type|Required|Description|
|---------|----|--------|-----------|
|blobname|string|true|A name by which this blob of bytes will be referred by the repository |
|dest|string|true|A regular expression specifying the destination. This blob will be downloaded by any server whose ```serverid``` matches the regex|
|file|form|true|Multi-part content specifying the data bytes|


#### GET /globalrepo
List all the files available in all the instances of griffin  (henceforth called global repository)

#### PUT,POST /globalrepo/
Push a given file to the specified set of machines

|Parameter|Type|Required|Description|
|---------|----|--------|-----------|
|blobname|string|true|A name by which this blob of bytes will be referred by the repository |
|dest|string|true|A regular expression specifying the destination. This blob will be downloaded by any server whose ```serverid``` matches the regex|
|file|form|true|Multi-part content specifying the data bytes|

#### GET /missing
List all the files in global repository that is supposed to be in the local repository but has not
yet been download



