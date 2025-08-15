from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
import os

os.makedirs("output", exist_ok=True)
os.makedirs("insights",exist_ok=True)

class DataQualityTests:

    @staticmethod
    def run_all_tests(data):
        DataQualityTests.test_null_values(data)
        DataQualityTests.test_date_consistency(data)
        DataQualityTests.test_value_ranges(data)

    @staticmethod
    def test_null_values(data):
        # Check for null values in key columns
        key_columns = {
            "fact_play_sessions": ["session_id", "user_id", "channel_id", "start_time"],
            "fact_payments": ["payment_id", "user_registration_id", "plan_id", "payment_date"],
            "dim_users": ["user_id", "email"]
        }

        for table, columns in key_columns.items():
            for col_name in columns:
                null_count = data[table].filter(col(col_name).isNull()).count()
                assert null_count == 0, \
                    f"Found {null_count} null values in {table}.{col_name}"

    @staticmethod
    def test_date_consistency(data):
        # Check that session end times are after start times
        invalid_sessions = data["fact_play_sessions"].filter(
            col("end_time") < col("start_time")
        ).count()
        assert invalid_sessions == 0, \
            f"Found {invalid_sessions} sessions with end time before start time"

        # Check that plan end dates are after start dates
        invalid_plans = data["fact_payments"].filter(
            col("end_date") < col("start_date")
        ).count()
        assert invalid_plans == 0, \
            f"Found {invalid_plans} plans with end date before start date"

    @staticmethod
    def test_value_ranges(data):
        # Check that duration is positive and reasonable
        invalid_durations = data["fact_play_sessions"].filter(
            (col("duration_minutes") < 0) | (col("duration_minutes") > 1440)
        ).count()
        assert invalid_durations == 0, \
            f"Found {invalid_durations} sessions with invalid duration"

        # Check that scores are non-negative
        negative_scores = data["fact_play_sessions"].filter(
            col("total_score") < 0
        ).count()
        assert negative_scores == 0, \
            f"Found {negative_scores} sessions with negative scores"


def create_spark_session():
    spark = SparkSession.builder \
    .appName("DiceGameDataProcessing") \
    .config("spark.sql.warehouse.dir", "spark-warehouse") \
    .getOrCreate()

    return spark





# Load all source data (assuming CSV files with headers)
def load_source_data(spark):
    """Load all source data files into DataFrames"""

    # Reference tables
    channel_codes = spark.read.csv("data/channel_code.csv", header=True)
    status_codes = spark.read.csv("data/status_code.csv", header=True)
    payment_frequency = spark.read.csv("data/plan_payment_frequency.csv", header=True)

    # Core tables
    play_sessions = spark.read.csv("data/user_play_session.csv", header=True)

    # User-related tables

    users = spark.read.csv("data/user_registration.csv", header=True)
    user_plans = spark.read.csv("data/user_plan.csv", header=True)
    payment_details = spark.read.csv("data/user_payment_detail.csv", header=True)
    plans = spark.read.csv("data/plan.csv", header=True)

    return {
        "channel_codes": channel_codes,
        "status_codes": status_codes,
        "payment_frequency": payment_frequency,
        "play_sessions": play_sessions,
        "users": users,
        "user_plans": user_plans,
        "payment_details": payment_details,
        "plans": plans
    }


def transform_data(data):
    # 1. Create dimension tables
    dim_channels = data["channel_codes"].select(
        col("play_session_channel_code").alias("channel_id"),
        col("english_description").alias("channel_description")
    ).distinct()

    dim_statuses = data["status_codes"].select(
        col("play_session_status_code").alias("status_id"),
        col("english_description").alias("status_description")
    ).distinct()

    dim_payment_frequency = data["payment_frequency"].select(
        col("payment_frequency_code").alias("frequency_id"),
        col("english_description").alias("frequency_description")
    ).distinct()

    dim_users = data["users"].select(
        "user_id",
        "username",
        "email",
        "first_name",
        "last_name",
        lit(datetime.now()).alias("etl_load_time")
    )

    dim_plans = data["plans"].join(
        data["payment_frequency"],
        data["plans"]["payment_frequency_code"] == data["payment_frequency"]["payment_frequency_code"],
        "left"
    ).select(
        data["plans"]["plan_id"],
        data["plans"]["payment_frequency_code"],
        data["payment_frequency"]["english_description"].alias("payment_frequency_desc"),
        data["plans"]["cost_amount"].alias("plan_cost"),
        lit(datetime.now()).alias("etl_load_time")
    )

    dim_payment_methods = data["payment_details"].select(
        "payment_detail_id",
        "payment_method_code",
        "payment_method_value",
        "payment_method_expiry",
        lit(datetime.now()).alias("etl_load_time")
    ).distinct()

    # 2. Create fact tables
    fact_play_sessions = data["play_sessions"].withColumn(
        "duration_minutes",
        (unix_timestamp(col("end_datetime")) - unix_timestamp(col("start_datetime"))) / 60
    ).select(
        col("play_session_id").alias("session_id"),
        "user_id",
        col("channel_code").alias("channel_id"),
        col("status_code").alias("status_id"),
        col("start_datetime").alias("start_time"),
        col("end_datetime").alias("end_time"),
        "duration_minutes",
        "total_score",
        lit(datetime.now()).alias("etl_load_time")
    )

    fact_payments = data["user_plans"].join(
        data["payment_details"],
        "payment_detail_id"
    ).join(
        data["plans"],
        "plan_id"
    ).select(
        concat(data["user_plans"]["user_registration_id"].cast("string"),lit("-"),data["user_plans"]["payment_detail_id"].cast("string")).alias("payment_id"),
        data["user_plans"]["user_registration_id"],
        data["user_plans"]["plan_id"],
        data["user_plans"]["payment_detail_id"],
        data["user_plans"]["start_date"].alias("payment_date"),
        data["plans"]["cost_amount"].alias("amount"),
        when(data["plans"]["payment_frequency_code"] == "ONETIME", False)
        .otherwise(True).alias("is_subscription"),
        lit(datetime.now()).alias("etl_load_time")
    )

    return {
        "dim_channels": dim_channels,
        "dim_statuses": dim_statuses,
        "dim_payment_frequency": dim_payment_frequency,
        "dim_users": dim_users,
        "dim_plans": dim_plans,
        "dim_payment_methods": dim_payment_methods,
        "fact_play_sessions": fact_play_sessions,
        "fact_payments": fact_payments
    }


def save_results(data):
    """Save dimensional tables and insights to disk"""

    # Save dimensional tables as Parquet
    for table_name, df in data.items():
        df.write.mode("overwrite").parquet(f"output/{table_name}")
        #df.write.mode("overwrite").option("header","true").csv(f"output/{table_name}")

def write_insights(insights):
    for table_name, df in insights.items():
        df.write.mode("overwrite").option("header", "true").csv(f"insights/{table_name}")

def generate_insights(data):
    insights = {}

    # User engagement by plan type
    insights["engagement_by_plan"] = data["fact_play_sessions"].join(
        data["fact_payments"], data["fact_payments"]["user_registration_id"] == data["fact_play_sessions"]["user_id"]
    ).join(
        data["dim_plans"], "plan_id"
    ).groupBy("plan_id", "payment_frequency_code").agg(
        countDistinct("user_id").alias("unique_users"),
        count("*").alias("total_sessions"),
        avg("duration_minutes").alias("avg_session_length"),
        avg("total_score").alias("avg_score"),
        sum("amount").alias("total_revenue")
    ).orderBy("total_revenue", ascending=False)

    # Revenue analysis by payment type
    insights["revenue_by_payment_type"] = data["fact_payments"].groupBy("is_subscription").agg(
        count("*").alias("payment_count"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_payment_amount")
    ).orderBy("total_revenue", ascending=False)

    # Monthly active users trend
    insights["mau_trend"] = data["fact_play_sessions"].withColumn(
        "month", date_format("start_time", "yyyy-MM")
    ).groupBy("month").agg(
        countDistinct("user_id").alias("active_users")
    ).orderBy("month")

    # Play sessions distribution by channel
    insights["sessions_by_channel"] = data["fact_play_sessions"].join(
        data["dim_channels"], "channel_id"
    ).groupBy("channel_description").agg(
        count("*").alias("session_count"),
        avg("duration_minutes").alias("avg_duration_minutes"),
        avg("total_score").alias("avg_score")
    ).orderBy("session_count", ascending=False)

    return insights

spark=create_spark_session()
data = load_source_data(spark)
dimensional_data = transform_data(data)
DataQualityTests.run_all_tests(dimensional_data)
insights = generate_insights(dimensional_data)
save_results(dimensional_data)
write_insights(insights)