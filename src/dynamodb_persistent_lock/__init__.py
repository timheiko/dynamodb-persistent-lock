__version__ = "1.0.0"

from dynamodb_persistent_lock.dynamodb_persistent_lock import (
    DynamoDBPersistentLockClient,
    DynamoDBPersistentLockFactory,
)

__all__ = ["DynamoDBPersistentLockClient", "DynamoDBPersistentLockFactory"]
