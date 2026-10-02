"""SparkSession Factory configured for Windows with winutils, JDK 21, and 8.3 short paths."""
import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession

# 1. Resolve 8.3 short path for Python executable to prevent space-in-path issues on Windows
active_python = sys.executable
if " " in active_python:
    short_python = active_python.replace("NAGESH REDDY", "NAGESH~1")
else:
    short_python = active_python

os.environ["PYSPARK_PYTHON"] = short_python
os.environ["PYSPARK_DRIVER_PYTHON"] = short_python

# 2. Ensure JDK 21 is set in environment for Windows stability
if "JAVA_HOME" not in os.environ or "jdk-24" in os.environ.get("JAVA_HOME", "").lower():
    jdk21_path = Path("C:/Program Files/Java/jdk-21")
    if jdk21_path.exists():
        os.environ["JAVA_HOME"] = str(jdk21_path)
        os.environ["PATH"] = f"{jdk21_path / 'bin'};{os.environ.get('PATH', '')}"

# 3. Configure HADOOP_HOME for Windows FileSystem compatibility
hadoop_home = Path("C:/Users/NAGESH~1/.gemini/antigravity-ide/scratch/hadoop")
if (hadoop_home / "bin" / "winutils.exe").exists():
    os.environ["HADOOP_HOME"] = str(hadoop_home)
    os.environ["PATH"] = f"{hadoop_home / 'bin'};{os.environ.get('PATH', '')}"


def get_spark_session(app_name: str = "SitePulse-Engine") -> SparkSession:
    """Build or retrieve a configured SparkSession."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.pyspark.python", short_python)
        .config("spark.pyspark.driver.python", short_python)
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
