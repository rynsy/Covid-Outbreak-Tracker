# TODO: Fix the names

FROM openjdk:8
RUN mkdir /myapp
COPY target/cs505-final-1.0-SNAPSHOT.jar /myapp
COPY data/hospitals.csv /myapp
COPY data/kyzipdistance.csv /myapp
WORKDIR /myapp
CMD ["java", "-jar","cs505-final-1.0-SNAPSHOT.jar"]
