__version__ = "0.0.5"

from dynamodb_persistent_lock.dynamodb_persistent_lock import (
    DynamoDBPersistentLockClient,
    DynamoDBPersistentLockFactory,
)

__all__ = ["DynamoDBPersistentLockClient", "DynamoDBPersistentLockFactory"]
