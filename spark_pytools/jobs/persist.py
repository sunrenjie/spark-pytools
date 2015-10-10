from pyspark import storagelevel
from spark_pytools.jobs.init import sc


nums = sc.parallelize([1, 5, 3, 9])
result = nums.map(lambda x: x * x)

# persist to re-use data
result.persist(storagelevel.StorageLevel.DISK_ONLY)
print(result.count())
print(",".join([str(i) for i in result.collect()]))

