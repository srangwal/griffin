## Griffin
A service to sync a blob of data to a cluster of machines across multiple data center using Apache Kafka

### Requirements
- Gradle 2.5+
- protoc 2.5+
- Java 1.7


### Build
Prepare a single jar with

```sh
$> gradle fatJar
```


### Run
Griffin requires a configuration file `config.properties` in the local directory. Please update the file with the
required information.

**Required configuration**
- Griffin.ZkServers:  ```<list of zkservers>```
- Griffin.KafkaBrokersList:  ```<list of kafka brokers with port>```
- Griffin.Error.Email.Recipients:  ```<your email id>```
- serverid:  ```<unique id for this griffin instance>```
- dcname:  ```<data center name>```

We will run two instances of griffin. Each instance of griffin is required to have a unique serverid and zkservers and
broker list for all the instances of griffin running with the same dcname should be same.

Open two terminals. In first terminal
```sh
$> mkdir griffin-1
$> cp config.properties griffin-1
$> cd griffin-1
$> java -jar ../build/libs/griffin-all-0.1.jar
```

In second terminal change ```serverid``` in ```config.properties``` to ```griffin-2```
```sh
$> mkdir griffin-2
$> cp config.properties griffin-2
$> cd griffin-2
$> java -jar ../build/libs/griffin-all-0.1.jar
```


**Commands**
- localrepo: List of all the files in local repository
- globalrepo: List of all the files in global repository
- missingfiles: List of files missing in the local repository
- syncblob <blobname> <dest> <filepath>: Push given file to specified instances in the cluster"
     e.g., syncblob config .* config.properties
