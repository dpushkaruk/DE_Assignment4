from minio import Minio
from pathlib import Path

MINIO_ENDPOINT = "localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET = "firepoint"
CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "minio_data" / "employee_attendance.csv"

client = Minio(MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)

if not client.bucket_exists(BUCKET):
    client.make_bucket(BUCKET)
    print(f"created bucket: {BUCKET}")

client.fput_object(BUCKET, "employee_attendance.csv", str(CSV_PATH))
print(f"uploaded {CSV_PATH.name} to {BUCKET}/employee_attendance.csv")
print("done")
