========================
DynamoDB Persistent Lock
========================

Getting Started
---------------

1. Install the library

.. code-block:: bash

    % python3 -m pip install dynamodb-persistent-lock

2. Usage

Non-async DynamoDB local download:

.. code-block:: python

    from dynamodb_persistent_lock.dynamodb_persistent_lock import (
        DynamoDBPersistentLockFactory,
        DynamoDBPersistentLockClient,
    )
    from datetime import timedelta
    ...
    # Create a lock factory
    lock_factory = DynamoDBPersistentLockFactory(
        table_name="locks",
        region_name="eu-central-1",
        partition_key_name="lock_key",
        sort_key_name="sort_key",
        record_version_number_name="rvn",
        heartbeat_period=timedelta(seconds=10),
        ttl_attribute_name="expire_at",
        ttl_heartbeat_multiplier=3,
    )
    lock_factory.ensure_table()
    ...
    # Open a lock client
    lock_client = lock_factory.open_lock_client()
    try:
        lock = lock_client.try_acquire_lock("my_lock_key")
        ...
        # Lock-guarded code here.
    finally:
        lock_client.close()


Features
--------
* Creates a new lock, if there is none, in DynamoDB table.
* Reacquires stale existing lock based on the ttl column value.
* Keeps extending the ttl of an acquired lock with a given heartbeat rate.

Credits
-------
* `Building Distributed Locks with the DynamoDB Lock Client <https://aws.amazon.com/blogs/database/building-distributed-locks-with-the-dynamodb-lock-client/>`_.
