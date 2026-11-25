
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import time
import sys

import os

experiment_start = time.time()

# Dataset size in MB
dataset_size_mb = int(sys.argv[2]) if len(sys.argv) > 2 else 500

# =====================================================
# 1. CREATE SPARK SESSION
# =====================================================

spark = (
    SparkSession.builder
    .appName("StudentPerformanceDataproc")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

SEED = 42

# Input file in Google Cloud Storage
INPUT_PATH = sys.argv[1] if len(sys.argv) > 1 else \
    "gs://YOUR_BUCKET/data/StudentPerformanceFactors.csv"

# =====================================================
# 2. LOAD DATA
# =====================================================

start_time = time.time()

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(INPUT_PATH)
)

print("Original row count:", df.count())
print("Original columns:", df.columns)

# =====================================================
# 3. STANDARDIZE COLUMN NAMES
# =====================================================

for old_name in df.columns:
    new_name = (
        old_name.strip()
        .replace(" ", "_")
        .replace("-", "_")
        .lower()
    )

    df = df.withColumnRenamed(old_name, new_name)

# =====================================================
# 4. REMOVE DUPLICATES
# =====================================================

df = df.dropDuplicates(
    ["_replica_id"] + [
        c for c in df.columns if c != "_replica_id"
    ]
)

# =====================================================
# 5. CLEAN MISSING VALUES
# =====================================================

df = df.dropna(how="any")

# =====================================================
# 6. CLEAN CATEGORICAL COLUMNS
# =====================================================

categorical_cols = [
    "parental_involvement",
    "access_to_resources",
    "extracurricular_activities",
    "motivation_level",
    "internet_access",
    "family_income",
    "teacher_quality",
    "school_type",
    "peer_influence",
    "learning_disabilities",
    "parental_education_level",
    "distance_from_home",
    "gender"
]

for c in categorical_cols:
    if c in df.columns:
        df = df.withColumn(
            c,
            F.initcap(F.trim(F.col(c)))
        )

# =====================================================
# 7. CONVERT NUMERIC COLUMNS
# =====================================================

numeric_cols = [
    "hours_studied",
    "attendance",
    "sleep_hours",
    "previous_scores",
    "tutoring_sessions",
    "physical_activity",
    "exam_score"
]

for c in numeric_cols:
    df = df.withColumn(
        c,
        F.col(c).cast(DoubleType())
    )

# Remove rows where numeric conversion failed
df = df.dropna(subset=numeric_cols)

# =====================================================
# 8. HANDLE EXAM SCORE OUTLIERS
# =====================================================

df = df.withColumn(
    "exam_score_capped_flag",
    F.when(F.col("exam_score") > 100, 1).otherwise(0)
)

df = df.withColumn(
    "exam_score",
    F.least(F.col("exam_score"), F.lit(100.0))
)

# =====================================================
# 9. DATA SUMMARY
# =====================================================

print("Cleaned row count:", df.count())

df.printSchema()

df.describe(
    "hours_studied",
    "attendance",
    "exam_score"
).show()

# =====================================================
# 10. SAVE CLEANED DATA
# =====================================================

df.write.mode("overwrite").parquet(
    "gs://YOUR_BUCKET/output/cleaned_student_data"
)

print("Data cleaning completed.")

print(
    "Data processing time:",
    round(time.time() - start_time, 2),
    "seconds"
)

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler
)
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# =====================================================
# 11. DEFINE FEATURES AND TARGET
# =====================================================

target = "exam_score"

# Exclude target and target-derived leakage column
feature_cols = [
    c for c in df.columns
    if c not in [
        "exam_score",
        "exam_score_capped_flag",
        "_replica_id"
    ]
]

# Identify numeric and categorical features
numeric_features = [
    c for c in feature_cols
    if c not in categorical_cols
]

categorical_features = [
    c for c in categorical_cols
    if c in feature_cols
]

# =====================================================
# 12. TRAIN / VALIDATION / TEST SPLIT
# =====================================================

train_val, test = df.randomSplit(
    [0.85, 0.15],
    seed=SEED
)

train, validation = train_val.randomSplit(
    [0.8235, 0.1765],
    seed=SEED
)

print("Training rows:", train.count())
print("Validation rows:", validation.count())
print("Test rows:", test.count())

# =====================================================
# 13. CATEGORICAL ENCODING
# =====================================================

indexers = [
    StringIndexer(
        inputCol=c,
        outputCol=c + "_index",
        handleInvalid="keep"
    )
    for c in categorical_features
]

encoder = OneHotEncoder(
    inputCols=[c + "_index" for c in categorical_features],
    outputCols=[c + "_encoded" for c in categorical_features]
)

# =====================================================
# 14. FEATURE ASSEMBLY
# =====================================================

assembler_inputs = (
    numeric_features +
    [c + "_encoded" for c in categorical_features]
)

assembler = VectorAssembler(
    inputCols=assembler_inputs,
    outputCol="features",
    handleInvalid="keep"
)

# =====================================================
# 15. GRADIENT BOOSTED TREE MODEL
# =====================================================

gbt = GBTRegressor(
    featuresCol="features",
    labelCol=target,
    maxIter=30,
    maxDepth=5,
    stepSize=0.05,
    seed=SEED
)

pipeline = Pipeline(
    stages=indexers + [
        encoder,
        assembler,
        gbt
    ]
)

# =====================================================
# 16. TRAIN MODEL
# =====================================================

training_start = time.time()

model = pipeline.fit(train)

training_time = time.time() - training_start

print("Training time:", round(training_time, 2), "seconds")

# =====================================================
# 17. VALIDATION
# =====================================================

validation_predictions = model.transform(validation)

evaluator_rmse = RegressionEvaluator(
    labelCol=target,
    predictionCol="prediction",
    metricName="rmse"
)

evaluator_mae = RegressionEvaluator(
    labelCol=target,
    predictionCol="prediction",
    metricName="mae"
)

evaluator_r2 = RegressionEvaluator(
    labelCol=target,
    predictionCol="prediction",
    metricName="r2"
)

val_rmse = evaluator_rmse.evaluate(validation_predictions)
val_mae = evaluator_mae.evaluate(validation_predictions)
val_r2 = evaluator_r2.evaluate(validation_predictions)

print("\nVALIDATION RESULTS")
print("MAE:", val_mae)
print("RMSE:", val_rmse)
print("R2:", val_r2)

# =====================================================
# 18. FINAL TEST EVALUATION
# =====================================================

test_predictions = model.transform(test)

test_rmse = evaluator_rmse.evaluate(test_predictions)
test_mae = evaluator_mae.evaluate(test_predictions)
test_r2 = evaluator_r2.evaluate(test_predictions)

print("\nFINAL TEST RESULTS")
print("MAE:", test_mae)
print("RMSE:", test_rmse)
print("R2:", test_r2)

# =====================================================
# 19. SAVE PREDICTIONS
# =====================================================

test_predictions.select(
    target,
    "prediction"
).write.mode("overwrite").parquet(
    "gs://YOUR_BUCKET/output/predictions"
)

# =====================================================
# 20. SAVE MODEL
# =====================================================

model.write().overwrite().save(
    "gs://YOUR_BUCKET/output/student_gbt_model"
)

print("Model saved successfully.")

# =====================================================
# PERFORMANCE MEASUREMENTS
# =====================================================

total_time = time.time() - experiment_start

throughput = dataset_size_mb / total_time

print("\n========== PERFORMANCE RESULTS ==========")

print("Dataset size (MB):", dataset_size_mb)
print("Training time:", round(training_time, 2))
print("Total execution time:", round(total_time, 2))
print("Throughput (MB/sec):", round(throughput, 2))

# Save experiment results
results = spark.createDataFrame(
    [(
        dataset_size_mb,
        round(training_time, 2),
        round(total_time, 2),
        round(throughput, 2)
    )],
    [
        "dataset_size_mb",
        "training_time",
        "total_execution_time",
        "throughput_mb_sec"
    ]
)

results.write.mode("append").parquet(
    "gs://YOUR_BUCKET/experiment_results"
)

print("Experiment results saved.")