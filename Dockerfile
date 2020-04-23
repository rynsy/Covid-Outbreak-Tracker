# TODO: Fix the names

FROM openjdk:8
RUN mkdir /myapp
RUN mkdir -p /myapp/data
COPY target/cs505-final-1.0-SNAPSHOT.jar /myapp
COPY data/hospitals.csv /myapp/data
COPY data/kyzipdistance.csv /myapp/data
WORKDIR /myapp
CMD ["java", "-jar","cs505-final-1.0-SNAPSHOT.jar"]
