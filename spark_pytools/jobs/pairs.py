import os
from pyspark import storagelevel
from spark_pytools.jobs.init import sc

# Reading the README from the spark distribution being used.
lines = sc.textFile(os.environ['SPARK_HOME'] + '/README.md')
pairs = lines.map(lambda x: (x.split(" ")[0], x))
results = pairs.filter(lambda t: len(t[1]) > 20)
print(results.collect())
