import pytest
from datetime import timedelta

from dynamodb_persistent_lock.dynamodb_persistent_lock import (
    DynamoDBPersistentLockFactory,
    DynamoDBPersistentLockClient,
)

from dynamodb_local import download_dynamodb, start_dynamodb_local, DynamoDBLocalServer


@pytest.fixture
def factory(endpoint_url: str) -> DynamoDBPersistentLockFactory:
    lock_factory = DynamoDBPersistentLockFactory(
        table_name="locks",
        region_name="eu-central-1",
        endpoint_url=endpoint_url,
        partition_key_name="lock_key",
        sort_key_name="sort_key",
        record_version_number_name="rvn",
        heartbeat_period=timedelta(seconds=10),
        ttl_attribute_name="expire_at",
        ttl_heartbeat_multiplier=3,
    )
    lock_factory.ensure_table()
    return lock_factory


@pytest.fixture
def endpoint_url() -> str:
    dynamodb_local_server = start_dynamodb_local(parent_dir="tmp/DynamoDBLocal")

    yield dynamodb_local_server.endpoint

    dynamodb_local_server.shutdown()


@pytest.fixture
def lock_client(factory: DynamoDBPersistentLockFactory) -> DynamoDBPersistentLockClient:
    client = factory.open_lock_client()
    yield client
    client.close()


def test_create_lock_client(factory: DynamoDBPersistentLockFactory):
    client = factory.open_lock_client()

    assert client is not None

    client.close()


@pytest.mark.slow
def test_acquire_lock_once(lock_client: DynamoDBPersistentLockClient):
    lock_client.heartbeat_period = timedelta(seconds=1)
    lock_client.owner_name = "test_acquire_lock_once"

    lock = lock_client.try_acquire_lock("my_lock_key")

    assert lock is not None


def test_acquire_lock_twice(lock_client: DynamoDBPersistentLockClient):
    lock_client.heartbeat_period = timedelta(seconds=5)
    lock_client.owner_name = "test_acquire_lock_twice"

    lock1 = lock_client.try_acquire_lock("my_lock_key")

    assert lock1 is not None

    lock2 = lock_client.try_acquire_lock("my_lock_key")

    assert lock2 is None
