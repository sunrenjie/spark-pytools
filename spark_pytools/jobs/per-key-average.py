# compute Per-key average as in Example 4-12.
from spark_pytools.jobs.init import sc

nums = sc.parallelize(((1, 2), (3, 4), (3,6)))
sum_count = nums.combineByKey(
    (lambda x: (x, 1)),  # initial value creator
    (lambda x, y: (x[0] + y, x[1] + 1)),  # accumulator
    (lambda x, y: (x[0] + y[0], x[1] + y[1]))  # combiner
)
print(sum_count.mapValues(lambda p: float(p[0]) / p[1]).collectAsMap())
