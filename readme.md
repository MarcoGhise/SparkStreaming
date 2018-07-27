### SparkStreaming ###

Run the Cluster
```text
./sbin/start-master.sh
```

Run the Worker process
```text
./bin/spark-class org.apache.spark.deploy.worker.Worker spark://linuxmint-virtual-machine:7077 --cores 4 --memory 2G
```

Run the TCP Server
```text
java -cp SparkStreaming-1.0.0.jar:lib/* it.blog.spark.streaming.socket.SocketServer
```

Deploy and run the application on cluster
```text
linuxmint-virtual-machine bin # ./spark-submit --class it.blog.spark.streaming.JavaNetworkTruckPosition --master spark://linuxmint-virtual-machine:7077 --deploy-mode cluster --jars /home/linuxmint/apache-spark-2.3.0-bin-hadoop2.6/SparkStreaming/target/lib/ --driver-class-path /home/linuxmint/apache-spark-2.3.0-bin-hadoop2.6/SparkStreaming/target/lib/ /home/linuxmint/apache-spark-2.3.0-bin-hadoop2.6/SparkStreaming/target/SparkStreaming-1.0.0.jar
```