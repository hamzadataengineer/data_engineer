spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 ./Subscribers/run_jadoop_signal_shark_sensor_pipeline.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 ./Subscribers/run_jadoop_elient_sensor_pipeline.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 ./Subscribers/run_jadoop_static_crfs_pipeline.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 ./Subscribers/run_jadoop_moving_crfs_pipeline.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 ./Subscribers/run_jadoop_blighter_pipeline.py
