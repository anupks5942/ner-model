import os
import csv
import logging
import boto3
import mysql.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup Logging
logger = logging.getLogger(__name__)

# Config
BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "resume-input-pdfs")
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DB", "resume_db"),
}

# Initialize S3 Client
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION")
)

def save_and_fetch_mysql(entity):
    """Inserts extracted data into MySQL and returns the full row (including ID)."""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True, buffered=True)

        # Insert or Update
        cursor.execute(
            """
            INSERT INTO resume_entities (name, email, mobile, dob, gender)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                mobile = VALUES(mobile),
                dob = VALUES(dob),
                gender = VALUES(gender)
            """,
            (
                entity.get("name"),
                entity.get("email"),
                entity.get("mobile"),
                entity.get("dob"),
                entity.get("gender"),
            ),
        )
        conn.commit()

        # Fetch the ID of the record we just touched
        cursor.execute(
            "SELECT * FROM resume_entities WHERE email = %s",
            (entity["email"],)
        )
        row = cursor.fetchone()
        
        cursor.close()
        conn.close()
        logger.info(f"✅ Saved to MySQL: {entity.get('email')}")
        return row
        
    except Exception as e:
        logger.error(f"❌ MySQL Error: {e}")
        raise e

def save_csv_to_s3(row):
    """Converts the DB row to CSV and uploads to S3 for Snowflake."""
    try:
        if not row:
            return

        filename = f"{row['id']}.csv"
        
        # Write CSV locally
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow([
                row["id"],
                row["name"],
                row["email"],
                row["mobile"],
                row["dob"],
                row["gender"]
            ])

        # Upload to S3
        s3_key = f"processed/{filename}"
        s3.upload_file(filename, BUCKET_NAME, s3_key)
        logger.info(f"✅ Uploaded to S3: {s3_key}")

        # Clean up local file
        os.remove(filename)

    except Exception as e:
        logger.error(f"❌ S3 Upload Error: {e}")
        raise e