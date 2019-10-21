# Kafka Axon Springboot Example

This is an example SpringBoot application using the Kafka Axon extension. 
It configures a simple Kafka message publishing using Kafka infrastructure run locally. 

## How to run

### Preparation

You will need `docker` and `docker-compose` to run this example.

Please run:

```bash 
docker-compose -f ./kafka-axon-example/docker-compose.yaml up -d
```

This will start Kafka, Zookeeper,
 Kafka Rest and Kafka Rest UI (available then on [http://localhost:8000/](http://localhost:8000/))

Now build the application by running:

```bash
mvn clean package -f ./kafka-axon-example 
``` 

### Running example application
 
You can start the application by running `java -jar ./kafka-axon-example/target/axon-kafka-example.jar`.

The application runs in two different modes demonstrating publishing
 of events to Kafka in subscribing or tracking modes. 
To activate these modes,
 please use Spring profiles by providing a command line parameter `--spring.profiles.active=subscribing` 
 and `--spring.profiles.active=tracking` respectively.   
