# star-local-jupyter-dbt

This project is intended to create a local development environment for Jupyter, Spark, & DBT (and eventually, Airflow) that is similar to what is available in AWS.

The "Set-up Steps" below demonstrate how to:
* Stand-up a fully-working Jupyter and spark environment.
* Use PySpark code to transform a CSV file to Parquet in a notebook and define the table structure in the Hive metastore.
* Use dbt-spark to transform that Parquet-based table into another table.
* Query that data via the Jupyter notebook.


# Pre-requisites
 * Docker: Have the latest version of the Docker engine installed.
 * Python: Have Python 3.6 or later installed.

# Set-up Steps
Build the image:
```
cd <src>/docker/all-spark-notebook-thrift
docker build -t star/all-spark-notebook-thrift:latest .
cd ../..
```

Deploy the stack:
```
docker stack deploy -c stack.yml star-jupyter
```

Check the logs:
```
docker stack ps star-jupyter --no-trunc
```

You should see three containers running:
* star-jupyter_adminer.1
* star-jupyter_postgres.1 
* star-jupyter_spark.1

Get the login URL for jupyter:
```
docker logs $(docker ps | grep star-jupyter_spark | awk '{print $NF}')
```
Look for something like this:
```
    To access the notebook, open this file in a browser:
        file:///home/<username>/.local/share/jupyter/runtime/nbserver-26-open.html
    Or copy and paste one of these URLs:
        http://4ab3233899d5:8888/?token=<token>
     or http://127.0.0.1:8888/?token=<token>
```
Copy the last link into your browser.

Open a Jupyter terminal window. Then, type this:
```
sudo /usr/local/spark/sbin/start-thriftserver.sh
beeline
!connect jdbc:hive2://localhost:10000
```

Enter your credentials: postgres/postgres
Then, type this:

```
create database spark_demo;
```

In Jupyter, open the `spark_demo.ipynb` notebook. In the sixth cell, change the absolute path of the `output_path` variable.

Run the first six cells. This will create the `bakery_transactions` table in the `spark_demo` schema.

Now, in your OS terminal, install dbt-spark in a virtual environment.

```
cd <src>/dbt-app/dbt/spark_demo
python3 -m venv .venv
source .venv/bin/activate
pip install dbt-spark[PyHive]
```
Make sure you see something like this:
```
Configuration:
  profiles.yml file [OK found and valid]
  dbt_project.yml file [OK found and valid]

Required dependencies:
 - git [OK found]

Connection:
  host: localhost
  port: 10000
  cluster: None
  endpoint: None
  schema: spark_demo
  organization: 0
  Connection test: [OK connection ok]

All checks passed!
```

Run DBT:
```
dbt run --profiles-dir ../.. --profile spark_demo --target dev
```

You should see something like this:
```
15:32:22  Running with dbt=1.0.1
15:32:22  Partial parse save file not found. Starting full parse.
15:32:22  Found 1 model, 0 tests, 0 snapshots, 0 analyses, 190 macros, 0 operations, 0 seed files, 0 sources, 0 exposures, 0 metrics
15:32:22  
15:32:23  Concurrency: 1 threads (target='dev')
15:32:23  
15:32:23  1 of 1 START view model spark_demo.bread........................................ [RUN]
15:32:24  1 of 1 OK created view model spark_demo.bread................................... [OK in 1.03s]
15:32:25  
15:32:25  Finished running 1 view model in 2.19s.
15:32:25  
15:32:25  Completed successfully
15:32:25  
15:32:25  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 TOTAL=1
```

Go back to your Jupyter notebook, and run the last cell.
