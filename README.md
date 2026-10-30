# Cloud Hardware Efficiency

## Downloading libraries and dependancies
- It is recommended to use Linux/WSL

- In linux/WSL terminal, first download python and jdk (if you dont have them): 

    `sudo apt install -y python3 python3-pip python3-venv openjdk-17-jdk`

- Then create a virtual env and activate:

    `python3 -m venv spark-env`

    `source spark-env/bin/activate`

- Install the python packages. Be sure to have the same name since it breaks if versions are different.

 `pip -q install pyspark==3.5.6 delta-spark==3.2.0 cassandra-driver streamlit`

## Dataset Download
To download dataset execute the file `fetchData.sh`. Alternatively execute command in terminal: 
>wget -c --retry-connrefused --tries=0 --timeout=50  http://aliopentrace.oss-cn-beijing.aliyuncs.com/v2017Traces/alibaba-trace-2017.tar.gz

Then extract the folder, and rename it to "data"

## Cassandra installation instructions

Note: newer versions than java 17 may not work with cassandra.

- To get the Apache signing key, run:

    `sudo mkdir -p /etc/apt/keyrings`

    `sudo curl -o /etc/apt/keyrings/apache-cassandra.asc https://downloads.apache.org/cassandra/KEYS`

- Then add the cassandra 5.0 repository:

    `echo "deb [signed-by=/etc/apt/keyrings/apache-cassandra.asc] https://debian.cassandra.apache.org 50x main" | sudo tee /etc/apt/sources.list.d/cassandra.sources.list`

- Then update the package list and install cassandra:

    `sudo apt update`

    `sudo apt install cassandra -y`

## Running the project

- Before running main.py, start cassandra:

    `sudo service cassandra start`

- Then run `nodetool status` to check if the server runs fine. Note that cassandra may take up to a minute to start, it may fail if you use this command too soon. If it shows a node with status UN (meaning Up/Normal) it is running properly.

- Execute `main.py`:

    `python3 main.py`

This will process data, initialise the cassandra database and write `waste_by_machine`, `waste_by_job`, `waste_by_hour` to cassandra.

- Execute:

    `streamlit run app.py`

The application should run in your browser automatically. If it does not (which happens if you use WSL), use the given link (which should be http://localhost:8501)
