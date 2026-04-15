import os
import time

time_ns = time.time_ns()
os.environ["AWS_ACCESS_KEY_ID"] = f"fakeMyKeyId{time_ns}"
os.environ["AWS_SECRET_ACCESS_KEY"] = f"fakeSecretAccessKey{time_ns}"
os.environ["REGION"] = "eu-central-1"
