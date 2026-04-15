import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal
from logging import getLogger
from threading import Event, Thread
from typing import Mapping

import boto3
from botocore.exceptions import ClientError

logger = getLogger(__name__)

DYNAMODB = "dynamodb"
DEFAULT_PARTITION_KEY_NAME = "lock_key"
DEFAULT_SORT_KEY_NAME = "sort_key"
DEFAULT_RECORD_VERSION_NUMBER_KEY_NAME = "rvn"
DEFAULT_HEARTBEAT_PERIOD = timedelta(seconds=5)
DEFAULT_TTL_ATTRIBUTE_NAME = "expire_at"
DEFAULT_TTL_HEARTBEAT_MULTIPLIER = 3

DEFAULT_READ_CAPACITY = 3
DEFAULT_WRITE_CAPACITY = 3


@dataclass(repr=True)
class DynamoDBLock:
    lock_key: str
    sort_key: str
    heartbeat_period: timedelta
    ttl_heartbeat_multiplier: int = DEFAULT_TTL_HEARTBEAT_MULTIPLIER
    record_version_number: str = field(default_factory=lambda: str(uuid.uuid7()))
    now: Decimal | None = None
    ttl: Decimal | None = None

    def __post_init__(self):
        if self.ttl is None:
            self.now = Decimal(int(time.time()))
            self.ttl = self.now + Decimal(
                int(
                    self.heartbeat_period.total_seconds()
                    * self.ttl_heartbeat_multiplier
                )
            )

    def to_token(self):
        return f"<{self.lock_key}|{self.sort_key}>"


@dataclass
class DynamoDBPersistentLockFactory:
    table_name: str
    region_name: str
    endpoint_url: str | None = None
    partition_key_name: str = DEFAULT_PARTITION_KEY_NAME
    sort_key_name: str = DEFAULT_SORT_KEY_NAME
    ttl_attribute_name: str = DEFAULT_TTL_ATTRIBUTE_NAME
    record_version_number_name: str = DEFAULT_RECORD_VERSION_NUMBER_KEY_NAME
    heartbeat_period: timedelta = DEFAULT_HEARTBEAT_PERIOD
    ttl_heartbeat_multiplier: int = DEFAULT_TTL_HEARTBEAT_MULTIPLIER

    read_capacity: int = DEFAULT_READ_CAPACITY
    write_capacity: int = DEFAULT_WRITE_CAPACITY
    dynamodb_resource: "boto3.resources.factory.dynamodb.ServiceResource" = None

    def __post_init__(self):
        if self.dynamodb_resource is None:
            self.dynamodb_resource = boto3.resource(
                service_name=DYNAMODB,
                region_name=self.region_name,
                endpoint_url=self.endpoint_url,
            )

    def open_lock_client(self):
        return DynamoDBPersistentLockClient(
            heartbeat_period=self.heartbeat_period,
            table=self.dynamodb_resource.Table(self.table_name),
            partition_key_name=self.partition_key_name,
            sort_key_name=self.sort_key_name,
            record_version_number_name=self.record_version_number_name,
            ttl_heartbeat_multiplier=self.ttl_heartbeat_multiplier,
        )

    def ensure_table(self):
        try:
            logger.info(f"❓ Check if DynamoDB table <{self.table_name}> exists")
            self.dynamodb_resource.meta.client.describe_table(TableName=self.table_name)
            logger.info(f"✅ DynamoDB table <{self.table_name}> exists")
        except ClientError as e:
            logger.info(f"❌ DynamoDB table <{self.table_name}> does not exist: {e}")
            logger.info(f"Create DynamoDB table <{self.table_name}>")
            table = self.dynamodb_resource.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {"AttributeName": self.partition_key_name, "KeyType": "HASH"},
                    {"AttributeName": self.sort_key_name, "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": self.partition_key_name, "AttributeType": "S"},
                    {"AttributeName": self.sort_key_name, "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )

            logger.info(f"⏳ Creating DynamoDB table <{self.table_name}>")
            table.wait_until_exists()
            logger.info(f"✅ Created DynamoDB table <{self.table_name}>")

            logger.info(f"Enabling TTL on <{self.ttl_attribute_name}>")
            response = self.dynamodb_resource.meta.client.update_time_to_live(
                TableName=self.table_name,
                TimeToLiveSpecification={
                    "Enabled": True,
                    "AttributeName": self.ttl_attribute_name,
                },
            )
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                logger.info("✅ TTL has been successfully enabled.")
            else:
                logger.error(
                    f"❌ Failed to enable TTL, status code {response['ResponseMetadata']['HTTPStatusCode']}"
                )


@dataclass
class DynamoDBPersistentLockClient:
    heartbeat_period: timedelta
    table: "boto3.resources.factory.dynamodb.Table"
    owner_name: str | None = None
    partition_key_name: str = DEFAULT_PARTITION_KEY_NAME
    sort_key_name: str = DEFAULT_SORT_KEY_NAME
    ttl_attribute_name: str = DEFAULT_TTL_ATTRIBUTE_NAME
    ttl_heartbeat_multiplier: int = DEFAULT_TTL_HEARTBEAT_MULTIPLIER
    record_version_number_name: str = DEFAULT_RECORD_VERSION_NUMBER_KEY_NAME

    locks: Mapping[str, Event] = field(default_factory=dict)

    def __post_init__(self):
        logger.info("table %s", type(self.table))
        self.owner_name = self.owner_name or f"{os.uname().nodename}-{uuid.uuid7()}"

    def try_acquire_lock(self, lock_key: str, sort_key: str = "-") -> str:
        lock = self._try_acquire_lock(lock_key=lock_key, sort_key=sort_key)
        if lock is None:
            return None

        self._start_heartbeat(lock)
        return lock.to_token()

    def lock_acquired(self, lock_token: str) -> bool:
        return lock_token in self.locks

    def close(self) -> None:
        for lock_key, (event, _) in self.locks.items():
            logger.info(f"Stopping lock heartbeat for {lock_key}")
            event.set()

        for lock_key, (_, thread) in self.locks.items():
            logger.info(f"Waiting for lock {lock_key} heartbeat to exit")
            thread.join()

    def _try_acquire_lock(
        self, lock_key: str, sort_key: str = "-"
    ) -> DynamoDBLock | None:
        lock = DynamoDBLock(
            lock_key=lock_key,
            sort_key=sort_key,
            heartbeat_period=self.heartbeat_period,
            ttl_heartbeat_multiplier=self.ttl_heartbeat_multiplier,
        )

        try:
            return self._try_create_lock(lock)
        except self.table.meta.client.exceptions.ConditionalCheckFailedException as e:
            logger.error(e)
            return self._try_reacquire_existing_lock(lock)
        except Exception as e:
            logger.error(e)
            return None

        return lock

    def _try_create_lock(self, lock: DynamoDBLock) -> DynamoDBLock:
        logger.info(f"❓ Trying to acquire the lock {lock.to_token()}.")
        item = {
            "Item": {
                self.partition_key_name: lock.lock_key,
                self.sort_key_name: lock.sort_key,
                "owner_name": self.owner_name,
                self.record_version_number_name: lock.record_version_number,
                self.ttl_attribute_name: lock.ttl,
            },
            "ConditionExpression": "NOT(attribute_exists(#pk) AND attribute_exists(#sk))",
            "ExpressionAttributeNames": {
                "#pk": self.partition_key_name,
                "#sk": self.sort_key_name,
            },
        }
        self.table.put_item(**item)
        logger.info(f"✅ The lock {lock.to_token()} has been acquired: {lock}")
        return lock

    def _read_existing_lock(self, lock: DynamoDBLock) -> DynamoDBLock:
        logger.info(f"❓ Reading existing lock {lock.to_token()}: {lock}")
        query = {
            "Key": {
                self.partition_key_name: lock.lock_key,
                self.sort_key_name: lock.sort_key,
            },
            "ConsistentRead": True,
        }
        existing_item = self.table.get_item(**query)["Item"]
        return DynamoDBLock(
            lock_key=lock.lock_key,
            sort_key=lock.sort_key,
            heartbeat_period=lock.heartbeat_period,
            ttl=Decimal(existing_item.get(self.ttl_attribute_name)),
            record_version_number=existing_item.get(self.record_version_number_name),
        )

    def _try_reacquire_existing_lock(self, new_lock: DynamoDBLock) -> DynamoDBLock:
        logger.warning(f"❌ The lock {new_lock.to_token()} already exists.")

        existing_lock = self._read_existing_lock(new_lock)
        logger.warning(f"❌ The lock {new_lock.to_token()} already exists.")
        existing_lock = self._read_existing_lock(new_lock)

        logger.info(
            f"❓ Checking the expiration of the existing lock {new_lock.to_token()}."
        )
        if existing_lock.ttl > new_lock.now:
            logger.error(f"❌ The existing lock {new_lock.to_token()} has not expired.")
            return None

        logger.info(
            f"❓ Trying to re-acquired the existing expired lock {new_lock.to_token()}."
        )
        self._update_lock(existing_lock=existing_lock, new_lock=new_lock)
        logger.info(f"✅ Re-acquired the existing expired lock {new_lock.to_token()}.")
        return new_lock

    def _update_lock(
        self,
        existing_lock: DynamoDBLock,
        new_lock: DynamoDBLock,
    ) -> DynamoDBLock:
        update_query = {
            "Key": {
                self.partition_key_name: new_lock.lock_key,
                self.sort_key_name: new_lock.sort_key,
            },
            "UpdateExpression": "SET #rvn = :new_rvn, #owner = :owner, #ttl = :ttl",
            "ConditionExpression": "#rvn = :old_rvn",
            "ExpressionAttributeNames": {
                "#ttl": self.ttl_attribute_name,
                "#rvn": self.record_version_number_name,
                "#owner": "owner_name",
            },
            "ExpressionAttributeValues": {
                ":ttl": new_lock.ttl,
                ":owner": self.owner_name,
                ":old_rvn": existing_lock.record_version_number,
                ":new_rvn": new_lock.record_version_number,
            },
            "ReturnValues": "NONE",
        }

        try:
            self.table.update_item(**update_query)
            return new_lock
        except self.table.meta.client.exceptions.ConditionalCheckFailedException as e:
            logger.error(e)
            raise e

    def _delete_lock(self, existing_lock: DynamoDBLock) -> None:
        delete_query = {
            "Key": {
                self.partition_key_name: existing_lock.lock_key,
                self.sort_key_name: existing_lock.sort_key,
            },
            "ConditionExpression": "#rvn = :rvn",
            "ExpressionAttributeNames": {
                "#rvn": self.record_version_number_name,
            },
            "ExpressionAttributeValues": {
                ":rvn": existing_lock.record_version_number,
            },
        }
        try:
            logger.info(f"Deleting the lock of {existing_lock.to_token()}")
            self.table.delete_item(**delete_query)
        except self.table.meta.client.exceptions.ConditionalCheckFailedException as e:
            logger.error(e)

    def _send_heartbeat(self, existing_lock: DynamoDBLock, event: Event) -> None:
        def get_retries() -> int:
            return min(1, self.ttl_heartbeat_multiplier - 1)

        retries = get_retries()
        while retries > 0 and not event.wait(
            existing_lock.heartbeat_period.total_seconds()
        ):
            logger.info(f"⏳ Extending an existing lock: {existing_lock.to_token()}")
            new_lock = DynamoDBLock(
                lock_key=existing_lock.lock_key,
                sort_key=existing_lock.sort_key,
                heartbeat_period=existing_lock.heartbeat_period,
                ttl_heartbeat_multiplier=existing_lock.ttl_heartbeat_multiplier,
            )
            try:
                self._update_lock(existing_lock=existing_lock, new_lock=new_lock)
                retries = get_retries()
            except (
                self.table.meta.client.exceptions.ConditionalCheckFailedException
            ) as e:
                logger.warning(
                    f"❗ ❌ Lock {existing_lock.to_token()} was stolen! Giving up!: {e}"
                )
                self.locks.pop(existing_lock.to_token(), None)
                retries = 0
                return
            except Exception as e:
                logger.warning(
                    f"❗ Experienced an error, will retry? {retries > 0}: {e}"
                )
                retries -= 1
            existing_lock = new_lock

        self._delete_lock(existing_lock=existing_lock)

    def _start_heartbeat(self, existing_lock: DynamoDBLock) -> None:
        event = Event()
        thread = Thread(
            name=f"DynamoDb-Persistent-Lock-on-<{existing_lock.to_token()}>",
            target=self._send_heartbeat,
            args=(existing_lock, event),
        )
        self.locks[existing_lock.to_token()] = (event, thread)
        thread.start()
