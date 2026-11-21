# comp644-project

bird team

PySpark data processing and scalability experiments using student performance data. The pipeline cleans, encodes, scales, joins, and aggregates the data, then writes Parquet output.

- Data scalability: 100 MB, 500 MB, 1 GB, and 2 GB with 2 local workers.
- Worker scalability: 500 MB with 1, 2, and 5 local workers.
- Three trials per configuration, with median results and five performance graphs.

Create the environment from this directory:

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Open [Spark_Scalability.ipynb](Spark_Scalability.ipynb), select the environment as the kernel, and run from top to bottom. The notebook targets macOS with Java 17; its setup cell contains the Java path.

The notebook includes dataset generation and analysis. [results](results/) contains the measured timings, summaries, configuration, and graphs. Large generated datasets and Spark runtime files are ignored by Git.
