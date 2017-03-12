### kafka-streams-example collects small projects that I worked on using Kafka streams.  This is the first time I wrote java 8 lambda and built with gradle.  Those projects were inspired by kafka-src streams-examples.  I plan to include scala examples too.
#### The topics include:
   1. WordCountLambdaDemo (working example)
      The instruction to run integration tests is in javadoc of that class
       
      To run WordCountDemo in streams-examples of Kafka-src 
      
        - copy run-streams-examples.sh that I put together under script folder to the kafka-src directory
        - change to that directory 
        - gradle
        - ./gradlew streams:examples:tasks (if you want to find out what gradle task to use)
        - ./gradlew clean -PscalaVersion=2.12 streams:examples:jar
        - ./run-streams-examples.sh org.apache.kafka.streams.examples.wordcount.WordCountDemo
      
   2. PageViewRegionLambda (Working progress)   
      
      