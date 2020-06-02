Kafka Raft
=================
Kafka Raft is a sub module of Apache Kafka which features a tailored version of
[Raft Consensus Protocol](https://www.usenix.org/system/files/conference/atc14/atc14-paper-ongaro.pdf).
<p>

You could test the basic demo by running `bin/raft-server-start.sh`, which features a distributed counter
service that does self-increment every 0.5 seconds.

### Run Single Quorum ###
    bin/raft-server-start.sh config/raft-single.properties

### Run Multi Node Quorum ###
Open up 3 separate terminals, and run individual commands:

    bin/raft-server-start.sh config/raft-quorum-1.properties
    bin/raft-server-start.sh config/raft-quorum-2.properties
    bin/raft-server-start.sh config/raft-quorum-3.properties
    
This would setup a three node Raft quorum.

### Produce Data to the Quorum ###
You should turn off the self increment feature by setting `counter.self.increment=false` to 
false in the property files `config/raft-*.properties`.

Next run the `ProducerPerformance` module using this example command:

    ./bin/kafka-producer-perf-test.sh --topic __cluster_metadata --num-records 2000 --throughput -1 --record-size 10 --producer.config config/raft-producer.properties 

Then collect the print out metrics to compare throughput with Kafka performance.
