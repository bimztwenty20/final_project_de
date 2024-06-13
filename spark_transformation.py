from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import os
import shutil
import logging



# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        logger.info("Starting Spark session")
        spark = SparkSession.builder \
            .appName("spark_etl_job") \
            .master("local[*]") \
            .config("spark.executor.memory", "8g") \
            .config("spark.driver.memory", "8g") \
            .config("spark.executor.cores", "4") \
            .config("spark.executor.instances", "2") \
            .getOrCreate()

        input_path = "/home/bimz/dummy_data/final_project"
        output_path = "/home/bimz/dummy_data/final_project/final_output"
        final_output_path = "/home/bimz/dummy_data/final_project"

        logger.info(f"Loading data from {input_path}")

        billing_df = load_csv(spark, os.path.join(input_path, "billing.csv"))
        contracts_df = load_csv(spark, os.path.join(input_path, "contracts.csv"))
        customers_df = load_csv(spark, os.path.join(input_path, "customers.csv"))
        internet_services_df = load_csv(spark, os.path.join(input_path, "internet_services.csv"))
        orders_df = load_csv(spark, os.path.join(input_path, "orders.csv"))
        payment_history_df = load_csv(spark, os.path.join(input_path, "payment_history.csv"))
        services_df = load_csv(spark, os.path.join(input_path, "services.csv"))
        churn_details_df = load_csv(spark, os.path.join(input_path, "churn_details.csv"))

        logger.info("Cleansing services, internet services data, dan customers")
        internet_services_df = cleanse_data(internet_services_df, ["OnlineSecurity", "OnlineBackup", "DeviceProtection", "TechSupport", "StreamingTV", "StreamingMovies"])
        services_df = cleanse_data(services_df, ["MultipleLines", "PhoneService"])

        logger.info("Transforming transactions data")
        transaksi_df = transform_transaksi_to_olap(billing_df, orders_df, payment_history_df)
        logger.info("Transforming reference data")
        referensi_df = transform_referensi_to_olap(customers_df, internet_services_df, contracts_df, services_df, orders_df)
        logger.info("Transforming churn details")
        churn_olap_df = transform_churn_details_to_olap(churn_details_df, customers_df, services_df, contracts_df)

        # Buat direktori output jika belum ada
        if not os.path.exists(output_path):
            os.makedirs(output_path)
            logger.info(f"Created output directory: {output_path}")

        # Write the transformed data directly to CSV
        transaksi_output_dir = os.path.join(output_path, "transaksi.csv")
        referensi_output_dir = os.path.join(output_path, "referensi.csv")
        churn_output_dir = os.path.join(output_path, "churn.csv")
        write_csv(transaksi_df, transaksi_output_dir)
        write_csv(referensi_df, referensi_output_dir)
        write_csv(churn_olap_df, churn_output_dir)

        # Buat direktori tujuan jika belum ada
        if not os.path.exists(final_output_path):
            os.makedirs(final_output_path)
            logger.info(f"Created final output directory: {final_output_path}")

        # Memindahkan file part- menjadi transaksi.csv, referensi.csv, dan churn.csv
        move_and_rename(transaksi_output_dir, os.path.join(final_output_path, "transaksi.csv"))
        move_and_rename(referensi_output_dir, os.path.join(final_output_path, "referensi.csv"))
        move_and_rename(churn_output_dir, os.path.join(final_output_path, "churn.csv"))

        # Menghapus folder transaksi.csv, referensi.csv, dan churn.csv di output_path
        if os.path.exists(transaksi_output_dir):
            shutil.rmtree(transaksi_output_dir)
            logger.info(f"Deleted directory: {transaksi_output_dir}")
        if os.path.exists(referensi_output_dir):
            shutil.rmtree(referensi_output_dir)
            logger.info(f"Deleted directory: {referensi_output_dir}")
        if os.path.exists(churn_output_dir):
            shutil.rmtree(churn_output_dir)
            logger.info(f"Deleted directory: {churn_output_dir}")

        logger.info("All files have been processed successfully.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        spark.stop()

def load_csv(spark, file_path):
    logger.info(f"Reading CSV file from {file_path}")
    if os.path.exists(file_path):
        return spark.read.csv(f"file://{file_path}", header=True, inferSchema=True)
    else:
        raise FileNotFoundError(f"File not found: {file_path}")
    
def cleanse_data(df, columns):
    logger.info("Cleansing data")
    for col in columns:
        df = df.withColumn(col, when(df[col] == "No internet service", "No")
                          .when(df[col] == "No phone service", "No")
                          .otherwise(df[col]))
    return df

def transform_transaksi_to_olap(billing_df, orders_df, payment_history_df):
    logger.info("Transforming transactions into OLAP format")
    transaksi_df = billing_df.join(orders_df, "customerID", "inner") \
        .join(payment_history_df, "customerID", "inner") \
        .groupBy("customerID", "OrderID", "OrderDate") \
        .agg(
            {"MonthlyCharges": "avg", "TotalCharges": "sum"}
        ) \
        .withColumnRenamed("avg(MonthlyCharges)", "AverageMonthlyCharges") \
        .withColumnRenamed("sum(TotalCharges)", "TotalCharges") \
        .select(
            "customerID",
            "OrderID",
            "OrderDate",
            "AverageMonthlyCharges",
            "TotalCharges"
        )
    return transaksi_df

def transform_referensi_to_olap(customers_df, internet_services_df, contracts_df, services_df, orders_df):
    logger.info("Transforming reference data into OLAP format")
    referensi_df = customers_df.join(internet_services_df, "customerID", "inner") \
        .join(contracts_df, "customerID", "inner") \
        .join(services_df, "customerID", "inner") \
        .join(orders_df.select("customerID", "OrderDate"), "customerID", "inner") \
        .select(
            "customerID",
            "gender",
            "SeniorCitizen",
            "Partner",
            "Dependents",
            "InternetService",
            "OnlineSecurity",
            "OnlineBackup",
            "DeviceProtection",
            "TechSupport",
            "StreamingTV",
            "StreamingMovies",
            "Contract",
            "PaperlessBilling",
            "PaymentMethod",
            "tenure",
            "PhoneService",
            "MultipleLines",
            "OrderDate"
        )
    return referensi_df

def transform_churn_details_to_olap(churn_details_df, customers_df, services_df, contracts_df):
    logger.info("Transforming churn details into OLAP format")
    churn_olap_df = churn_details_df.join(customers_df, "customerID", "inner") \
        .join(services_df, "customerID", "inner") \
        .join(contracts_df, "customerID", "inner") \
        .select(
            "customerID",
            "ChurnReason",
            "ChurnDate",
            "LastInteractionDate",
            "SupportContactCount",
            "RetentionOffers",
            "FeedbackScore",
            "tenure",
            "PhoneService",
            "MultipleLines"
        )
    return churn_olap_df

def write_csv(df, output_dir):
    try:
        logger.info(f"Writing data to {output_dir}")
        df.write.option("header", "true").mode("overwrite").csv(f"file://{output_dir}")
        logger.info(f"Data written to {output_dir}")
    except Exception as e:
        logger.error(f"Error occurred while writing CSV: {e}")
        raise

def move_and_rename(output_dir, final_file_path):
    logger.info(f"Moving and renaming files from {output_dir} to {final_file_path}")
    final_output_dir = os.path.dirname(final_file_path)
    if not os.path.exists(final_output_dir):
        os.makedirs(final_output_dir)
        logger.info(f"Created final output directory: {final_output_dir}")

    for file_name in os.listdir(output_dir):
        if file_name.startswith("part-") and file_name.endswith(".csv"):
            source_file = os.path.join(output_dir, file_name)
            shutil.move(source_file, final_file_path)
            logger.info(f"Moved file {source_file} to {final_file_path}")
            break
    else:
        logger.error(f"No part files found in {output_dir}")


if __name__ == "__main__":
    main()