# Cloud Hardware Efficiency

## Downloading libraries and dependancies
- It is recommended to use Linux/WSL

- In linux/WSL terminal, first download python and jdk (if you dont have them): 

    `sudo apt install -y python3 python3-pip python3-venv openjdk-11-jdk`

- Then create a virtual env and activate:

    `python3 -m venv spark-env`

    `source spark-env/bin/activate`

- Install the python packages. Be sure to have the same name since it breaks if versions are different.

 `pip -q install pyspark==3.5.6 delta-spark==3.2.0`

## Dataset Download
To download dataset execute the file `fetchData.sh`. Alternatively execute command in terminal: 
>wget -c --retry-connrefused --tries=0 --timeout=50  http://aliopentrace.oss-cn-beijing.aliyuncs.com/v2017Traces/alibaba-trace-2017.tar.gz

Then extract the folder, and rename it to "data"





### Note about cpu usage (Remove later):
to get the efficiency you subtract real_cpu_avg (batch_instance) from plan_cpu (batch_task)
real_cpu_avg is normalized between [0,1], but plan_cpu is in order 100 (so its multiplied by 100
i.e. if the value is 50 then its 0.5)
When you want to do effeciency metric, divide plan_cpu by a 100 so both are in the same units. Memory is normalized so that's alright

Check the function check_units() to see the output


|summary|          plan_cpu|            plan_mem|

|  count|             71536|               71536|

|   mean| 62.50020968463431|0.007248826701042301|

| stddev|22.585005580365387| 0.00434242760109868|

|    min|              40.0|0.002754122123573...|

|    max|             800.0|  0.1273315559236922|


+-------+------------------+--------------------+

|summary|      real_cpu_avg|        real_mem_avg|


|  count|           1292820|             1292820|

|   mean|0.5407433362726333| 0.01773499288846844|

| stddev| 0.453113098628032|0.014726896667051913|

|    min|              0.01|3.468537260890948E-4|

|    max|              4.44| 0.16389615095902466|
+-------+------------------+--------------------+
