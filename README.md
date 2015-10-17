## Griffin
A service to sync a blob of data to a cluster of machines across multiple data center using Apache Kafka

Griffin provides a REST API to interact with the service
### Requirements
- Gradle 2.5+
- protoc 2.5+
- Java 1.7


### Build
Prepare a single jar with

```sh
$> gradle build
```

### Run
Griffin requires a configuration file `griffin.cong` in the local directory. Please update the file with the required information.

**Required configuration**
- Griffin.ZkServers:  ```<list of zkservers>```
- Griffin.KafkaBrokersList:  ```<list of kafka brokers with port>```
- Griffin.Error.Email.Recipients:  ```<your email id>```
- serverid:  ```<unique id for this griffin instance>```
- dcname:  ```<data center name>```

We will run two instances of griffin. Each instance of griffin is required to have a unique serverid and zkservers and broker list for all the instances of griffin running with the same dcname should be same.

Open two terminals. In first terminal
```sh
$> cd examples
$> java -jar ../build/libs/griffin-0.1.0.jar
```

In second terminal change ```serverid``` in ```examples/griffin.conf``` to ```griffin-2``` and ```server.port``` in ```examples/application.properties``` to ```8081``` and start another instance of griffin
```sh
$> cd examples
$> java -jar ../build/libs/griffin-0.1.0.jar
```

In yet another terminal we can interact with these griffin services. We will push a file to one instance of griffin and we will observe it getting replicated to another instance of griffin.
```sh
$> curl -i http://localhost:8080/griffin/localrepo/
$> curl -i http://localhost:8081/griffin/localrepo/
```
We can push a file to one of the griffin instances as
```sh
$> curl -i -F blobname=gradle -F dest='.*' -F file=@build.gradle http://localhost:8080/griffin/localrepo
```
After a short while one can observe both files available in local repository of both the services
```sh
$> curl -i http://localhost:8080/griffin/localrepo/
$> curl -i http://localhost:8081/griffin/localrepo/
```
Check out REST API section for full description of rest calls above.

### Docker
**Requires**
- Docker 1.8+
- Docker compose (https://docs.docker.com/compose/install/)

A docker container containing the this service can also be built using
```sh
$> gradle buildDocker
```

This will create a docker image ```srangwal/griffin``` containing griffin service.

A ```docker-compose.yml``` to create a full cluster of zookeeper, kafka, and griffin is included (one is not required to
build griffin image for this step). To create a cluster

```sh
$> docker-compose up -d
$> docker-compose scale kafkalax1=3
$> docker-compose scale griffinlax1=3
```
This might take some time as it might require docker to download few images.

The cluster is now up and running. To get local repository of first griffin service
```sh
$> curl -i http://$(docker port griffin_griffinlax1_1 8080)/griffin/localrepo
```

To push a file (to one instance of griffin)
```sh
   $> curl -i# -F blobname=gradle -F dest='.*' -F file=@build.gradle http://$(docker port griffin_griffinlax1_1 8080)/griffin/localrepo
```

We can now observe (with a short delay) that the file is available in all the services
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
  **Parameters**
  blobname(string)   required   A name by which this blob of bytes will be referred by the repository
  dest(string)       required   A regular expression specifying the destination. This blob will be
                                downloaded by any server whose ```serverid``` matches the regex
  file(form)         required   Multi-part content specifying the data bytes


#### GET /globalrepo
  List all the files available in all the instances of griffin  (henceforth called global repository)

#### PUT,POST /globalrepo/
  Push a given file to the specified set of machines
  **Parameters**
  blobname(string)   required   A name by which this blob of bytes will be referred by the repository
  dest(string)       required   A regular expression specifying the destination. This blob will be
                                downloaded by any server whose ```serverid``` matches the regex
  file(form)         required   Multi-part content specifying the data bytes


#### GET /missing
  List all the files in global repository that is supposed to be in the local repository but has not
  yet been download



