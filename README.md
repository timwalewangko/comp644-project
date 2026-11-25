# comp644-project

bird team

PySpark data processing and scalability experiments on Google Cloud Dataproc. The pipeline cleans, encodes, scales, joins, and aggregates student performance data, then writes Parquet and grouped summaries.

## Files

- Tim: [Spark_Scalability.ipynb](Spark_Scalability.ipynb) — processing-only Dataproc experiments, timing tables, graphs, and analysis.
- Hasmik: [Dataset_size_increase](Dataset_size_increase) — dataset replication and upload notes.
- Hasmik: [student_performance_spark.py](student_performance_spark.py) — original preprocessing and model experiment.

The experiment instructions below use the notebook. Hasmik's original files are preserved for reference.

## Dataproc UI setup

Create and configure the cluster in the Google Cloud UI. Enable Jupyter and Component Gateway, then open the notebook in the cluster's **PySpark kernel**. The rubric specifies image 2.2, a `n2-standard-4` master, `n1-standard-2` workers, and `us-central1`.

Set worker count, executor resources, autoscaling, and Spark properties in the UI. Keep machine types, disks, per-executor resources, and input/partition settings consistent between comparisons. Use a fresh application after each configuration change and verify executor activity in Spark UI.

Use these fixed Spark properties, entered in the UI:

| Property | Value |
| --- | --- |
| `spark.sql.shuffle.partitions` | `64` |
| `spark.sql.files.maxPartitionBytes` | `33554432` |
| `spark.sql.files.minPartitionNum` | `2` |
| `spark.sql.adaptive.enabled` | `false` |
| `spark.sql.constraintPropagation.enabled` | `false` |
| `spark.dynamicAllocation.enabled` | `false` |

Choose executor counts/cores/memory in the UI so the application can use the intended workers. The notebook reads effective settings and active YARN workers; it does not configure or resize the cluster. Export the UI cluster configuration for the report.

The rubric requests 1, 2, and 5 workers. Google's [standard cluster type](https://docs.cloud.google.com/dataproc/docs/reference/rest/v1/ClusterConfig#clustertype) requires at least two primary workers. Use an instructor-approved configuration for the one-worker baseline or an approved alternative baseline. A master-only cluster is different. The analysis labels and normalizes to the smallest measured worker count.

Dataproc supplies PySpark, Java, and Jupyter. Ensure the helper packages in [requirements.txt](requirements.txt) are available using the UI package settings; keep the image's PySpark installation.

## Bucket and notebook

1. Upload `StudentPerformanceFactors.csv` to `gs://YOUR_BUCKET_NAME/StudentPerformanceFactors.csv` and give the cluster service account read/write access to the bucket.
2. Open [Spark_Scalability.ipynb](Spark_Scalability.ipynb), replace `BUCKET`, and choose a shared `RUN` label. Authentication uses the cluster service account.
3. Run Sections 1–3 once on the 2-worker cluster to create the size variants. Generation repeats the source rows and records actual object sizes.
4. With `STUDY = "data"`, run Sections 4–6 on 2 workers: 100 MB, 500 MB, 1 GB, and 2 GB, three trials each.
5. Configure each approved worker count in the UI. In a fresh kernel/application, set `STUDY = "workers"` and run Sections 1–2 and 4–6. All worker runs use the same **2 GB** files and three trials. Skip Section 3 after the first visit.
6. After both studies, run Sections 7–9 to combine the saved results, produce five graphs, and answer the performance questions.

Keep `RUN` unchanged across these visits. The notebook writes processed data under `gs://YOUR_BUCKET_NAME/comp644-project/processed/RUN/` and results under `gs://YOUR_BUCKET_NAME/comp644-project/results/RUN/`. It saves the current configuration's trial table after each completed trial. Rerunning that configuration replaces its previous table.

`HOURLY_USD` is an optional total-cluster hourly estimate from the UI/pricing calculator. It defaults to `NaN`, so missing prices stay blank. Estimated processing cost is elapsed seconds × hourly estimate / 3600; include startup, idle time, storage, and network charges separately using Billing UI.

The Dataproc notebook has cleared outputs until cloud runs are performed. Previous local measurements are archived in [results/local](results/local/).

References: [Dataproc Jupyter](https://docs.cloud.google.com/dataproc/docs/concepts/components/jupyter), [Cloud Storage connector](https://docs.cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage), [YARN node API](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Cluster_Nodes_API).
