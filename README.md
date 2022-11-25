Jadoop
Joint All-Domain Operations & Planning System (JADOPS) presents a “Common Operating Picture” (COP) of all electromagnetic assets. Its an advanced battlespace Planning & Operations System which provides National Security agencies / Military Commanders to effectively execute command and control in joint-forces operations in a congested, contested, and hostile Electro Magnetic Environment (EME), while maintaining sufficient electromagnetic superiority in order to achieve military objectives. Commanders are able to shape the EME to ensure friendly forces can operate, while denying the same advantage to the adversary.

Arcitctural Diagram

## How to work
Here we are using kafka for messages queue.
Data comming from IOT Device/Simulation Script stored in MQTT broker topic.
Now data move farward from MQTT broker to Kafka Broker.
Consume data from kafka topic using spark.performing transformation and preprocessing to enrich the data.
Stored processed data in kafka new topic(hot pipeline) and historic data stored in Elastic Search (cold pipline).
Environment Variables
To run this project, you will need to add the following environment variables to your .bashrc file

#export SPARK_HOME="/etc/spark",
#export PATH="$PATH:$SPARK_HOME/bin",
#export PYSPARK_PYTHON="/usr/bin/python3",
#export PYSPARK_DRIVER_PYTHON="/usr/bin/python3",
#export PATH="/etc/kafka/bin:$PATH",
#export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64",
#export PATH="$JAVA_HOME/bin:$PATH",
#export PYTHONPATH="/root/jadoops-de",
#export JADOOP_HOME="/root/jadoops-de",



## Run Locally

Clone the project

```bash
  git clone git clone git@bitbucket.org:jadops-rapidev-admin/jadoops-de.git
Go to the project directory

  cd /main/python/config
Install dependencies

  pip3 intall -r requirments.txt
Current Fix for setup tools problem pip3 install --upgrade pip setuptools==45.2.0

Start the producers scripts(Run Ubuntu Vms)

for producers scripts(Data pushed to kafka topic)

#nohup python3 publish_weather_script.py >> ../logs/publish_weather.log &"
#nohup python3 publish_opensky_script.py >> ../logs/publish_opensky_data.log &"
#nohup python3 publish_ships_script.py >> ../logs/publish_ships_script.log &"
#nohup python3 publish_radar_script.py >> ../logs/publish_radar_script.log &
#nohup python3 publish_sensor_data.py >> ../logs/publish_sensor_data.log &
#nohup python3 publish_moving_crfs.py >> ../logs/publish_moving_crfs.log &
#nohup python3 publish_static_crfs.py >> ../logs/publish_static_crfs.log &
#nohup python3 publish_signal_shark_sensor_script.py >> ../logs/publish_signal_shark_sensor.log &
Start the spark consumer scripts(Run Ubuntu Vms)

#nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 run_jadoop_weather_pipeline.py  >> ../logs/spark-submit-weather.log &"
#nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 run_jadoop_opensky_pipeline.py  >> ../logs/spark-submit-opensky.log &"
#nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 run_jadoop_ships_pipline.py >> ../logs/spark-submit-ship.log &"
#nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 run_jadoop_sensor_pipeline.py  >> ../logs/spark-submit-sensor.log &"
#nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 run_jadoop_radar_pipeline.py  >> ../logs/spark-submit-radar.log &"
#nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 run_jadoop_static_crfs_pipeline.py >> ../logs/spark-submit-crfs-static.log &
#nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 run_jadoop_moving_crfs_pipeline.py >> ../logs/spark-submit-crfs-moving.log &

To run your pytest scripts go to the tests directory. run with python command (pytest -q 'python_file.py') run all the scripts from bash(sh pytest.sh)

Authors
Hamza Nadeem(Data Engineer)
