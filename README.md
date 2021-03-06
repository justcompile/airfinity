# airfinity

This is Richard Hayes' submission for the Data Engineer exercise. This readme contains information of how to setup & run the application components.

> Disclaimer
> Due to time constraints, for the occurrences where details of events within a supplied format do not match with those in the Events Database they are added to another table with the intension that they can be processed at a later date.
### Description
The application consists of two main features; Ingesters and Consumers.
##### Ingesters
Responsible for reading the data from a source and passing it onto a Kafka topic in order for consumers to process it asynchronously. In this application there is only a `CSVStreamIngester` which reads a csv file from `stdin` and adds each row to the appropirate Kafka topic.

##### Consumers
Responsible for parsing the data for a given format and mapping attendee data to events. Consumer classes have been build with extensibilty in mind, but only have implementations for the alpha (`AlphaConsumer`), beta (`BetaConsumer`) and gamma (`GammaConsumer`) formats.

##### Data stores
The application uses `Kafka` to act as the message broker which allows the both the Ingester and Consumer components to be distributed and individually scalable if and when the need arose. 
It uses `Neo4j` as it's database to store the data in a graph format as the data in question lends itself very much to being stored in a graph format. 

Both of these technologies were new to me and it's been fun & enjoyable to have the challenge of using them.

### Setup Instructions

#### Docker
The data stores: Kafka & Neo$j, require Docker to run. If you haven't installed it, get it from [The Docker Website](https://www.docker.com/community-edition).

#### Python 2.7
Presumes that python 2.7 is installed, if not install it for your particular OS. It should be installed by default on MacOS.

Create an virtual environment within the application folder so that the libraries do not conflict with system libraries:
```!bash
virtualenv venv
```
If the above command fails, install it with pip
```!bash
pip install virtualenv # may have to run as sudo
virtualenv venv
```
if you don't have pip, install pip with easy_install
```!bash
easy_install pip # may have to run as sudo
pip install virtualenv # may have to run as sudo
virtualenv venv
```

Once created, activate the virtual environment
```!bash
. venv/bin/activate
```

#### Installing dependencies
To install only the libraries required to run the application
```!bash
pip install -r requirements.txt
```
To run those required to run the test suite
```!bash
pip install -r dev-requirements
```

### Running the Application
First, you need to start the data stores:
```!bash
docker-compose up -d
```

Next, you will need to seed the Events database

```!bash
python eventdb_importer.py < data/event_db.csv
```
Next, we need to ingest the event format data by running the following:
```!bash
python ingester.py alpha < data/alpha.csv
python ingester.py beta < data/beta.csv
python ingester.py gamma < data/gamma.csv
```


Finally run the consumer daemon, this will create a number of processes which will consume the different data formats from Kafka:
```!bash
python consumer.py
```

Once the console has stopped having data printed out, it is complete, hit ctrl+c to exit.

### Report Generation
There are a number of reports generated
- A Summary of the all the events with a count of attendees
- A list of all Attendees and which Events they went to
- A report per Event listing all known attendee information

This can be generated with:
```!bash
python reports.py
```
This will print out the results to the console. It will also generate a CSV file for each event detailing attendees within the `reports` directory of the application.


### Running the Test Suite
Whilst I began with a TDD approach, time constraints prevented me from running this through to completion, so whilst there is a good level of test coverage, it could be better.

Assuming you installed the dev-requirements.txt earlier, run the test suite with:
```!bash
bash run_tests.sh
```


### Clean up
```!bash
docker-compose down # stops the Docker containers
rm -rf airfinity_dbs
deactivate
```
