__version__ = "0.0.6"

from dynamodb_persistent_lock.dynamodb_persistent_lock import (
    DynamoDBPersistentLockClient,
    DynamoDBPersistentLockFactory,
)

__all__ = ["DynamoDBPersistentLockClient", "DynamoDBPersistentLockFactory"]
