AI OPs Assignment 3 Submission
- By Madhav AK
- DA24B012

This README file goes through all major submission points.

- Main report can be found as `report.pdf` in the main directory.
- Video Submission Link:

- `spark_clean.py` contains the Spark preprocessing pipeline.
- `ray_clean.py` contains the equivalent Ray Data pipeline.
- `validate_outputs.py` checks that both outputs match.
- `results.csv` contains the measured benchmark values.
- `screenshots` contains the Spark, Ray, resource usage and validation evidence.
- The approximately 2 GB of Yellow Taxi data from January 2024 through August 2026 and
  the taxi zone lookup are inside `data` and are not committed.

Both pipelines remove nulls and duplicates, format timestamps, filter invalid trips, join the
taxi zone lookup for pickup and drop-off locations, calculate average speed and pickup hour,
and write the result to parquet. Both frameworks are run locally with two workers.

### Setup

```bash
cd /home/madhav/AIOps_A3
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Spark

Open three terminals and activate the virtual environment in each one.

```bash
# Terminal 1: master
spark-class org.apache.spark.deploy.master.Master \
  --host localhost --port 7077 --webui-port 8080

# Terminal 2: worker 1
spark-class org.apache.spark.deploy.worker.Worker \
  --webui-port 8081 --cores 1 --memory 8g spark://localhost:7077

# Terminal 3: worker 2
spark-class org.apache.spark.deploy.worker.Worker \
  --webui-port 8082 --cores 1 --memory 8g spark://localhost:7077
```

The master UI is available at `http://localhost:8080`. Run the pipeline from a fourth terminal:

```bash
cd /home/madhav/AIOps_A3
source .venv/bin/activate
spark-submit --master spark://localhost:7077 spark_clean.py
```

The application UI is available at `http://localhost:4040` while the pipeline is running.

### Ray

Run Ray after stopping Spark so that both frameworks get the same machine resources.

```bash
cd /home/madhav/AIOps_A3
source .venv/bin/activate

ray start --head --num-cpus=2 --object-store-memory=12884901888 \
  --port=6379 --dashboard-host=127.0.0.1

python ray_clean.py
```

The Ray dashboard is available at `http://localhost:8265`. Stop Ray after the run:

```bash
ray stop
```

### Output Validation

```bash
python validate_outputs.py
```

The final validation should show:

```text
COLUMNS_MATCH=True
ROW_COUNTS_MATCH=True
ROW_HASHES_MATCH=True
PARITY=PASS
```

The validator reads the parquet files in batches and uses temporary hash partitions, so it does
not load both 94 million row outputs into RAM at once.
