"""
live_sync_incremental_one_to_many.py

Incremental live sync from source -> multiple destinations for ONLY the specified database (sales).
Only new or updated documents are copied.
Deletions are reconciled as before (DB / collection / document).

Requirements:
- pymongo installed (`pip install pymongo`)
"""
#one to many
import logging
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# -------------------------
# Configuration
# -------------------------
SOURCE_URI = "mongodb://localhost:27011"
DEST_URIS = [
    "mongodb://localhost:27019",
    "mongodb://localhost:27020",
    "mongodb://localhost:27021"
]  # List of destination URIs

SOURCE_DB = "AWS-Staging2"   # ✅ Database to sync FROM
DEST_DB   = "AWS-Staging2"   # ✅ Database to sync TO (same name for all destinations)

IN_MEMORY_ID_THRESHOLD = 200_000  # Optimization threshold

# Logging
logger = logging.getLogger("incremental_sync")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

# -------------------------
# Connect to MongoDB
# -------------------------
src_client = MongoClient(SOURCE_URI)
dst_clients = [MongoClient(uri) for uri in DEST_URIS]  # Create clients for all destinations

# -------------------------
# Step A: Incremental Upsert
# -------------------------
def incremental_upsert(dst_client):
    logger.info(f"Starting incremental upsert for ONLY database: {SOURCE_DB} to destination: {dst_client.HOST}")

    src_db = src_client[SOURCE_DB]
    dst_db = dst_client[DEST_DB]

    for coll_name in src_db.list_collection_names():
        if coll_name.startswith("system."):
            continue
        src_coll = src_db[coll_name]
        dst_coll = dst_db[coll_name]

        try:
            src_count = src_coll.count_documents({})
            if src_count == 0:
                continue

            logger.info("Processing %s.%s (source_count=%d)", SOURCE_DB, coll_name, src_count)

            if src_count <= IN_MEMORY_ID_THRESHOLD:
                source_docs = list(src_coll.find({}))
                for doc in source_docs:
                    dst_coll.replace_one({"_id": doc["_id"]}, doc, upsert=True)
                logger.info("Upserted %d documents into %s.%s", len(source_docs), DEST_DB, coll_name)
            else:
                for doc in src_coll.find({}):
                    dst_coll.replace_one({"_id": doc["_id"]}, doc, upsert=True)

        except PyMongoError as e:
            logger.exception("Error processing %s.%s: %s", SOURCE_DB, coll_name, e)

# -------------------------
# Step B: Deletion Reconciliation
# -------------------------
def reconcile_deletions(dst_client):
    logger.info(f"Starting deletion reconciliation for ONLY database: {SOURCE_DB} in destination: {dst_client.HOST}")

    try:
        src_db = src_client[SOURCE_DB]
        dst_db = dst_client[DEST_DB]

        src_cols = set([c for c in src_db.list_collection_names() if not c.startswith("system.")])
        dst_cols = set([c for c in dst_db.list_collection_names() if not c.startswith("system.")])

        for coll in dst_cols - src_cols:
            try:
                logger.info("Dropping collection in destination: %s.%s", DEST_DB, coll)
                dst_db.drop_collection(coll)
            except Exception as e:
                logger.exception("Failed to drop collection %s.%s: %s", DEST_DB, coll, e)

        for coll in dst_cols.intersection(src_cols):
            src_coll = src_db[coll]
            dst_coll = dst_db[coll]

            try:
                src_count = src_coll.count_documents({})
                dst_count = dst_coll.count_documents({})
            except Exception as e:
                logger.exception("Count error on %s.%s: %s", DEST_DB, coll, e)
                continue

            logger.info("Reconciling documents for %s.%s (source_count=%d dest_count=%d)",
                        DEST_DB, coll, src_count, dst_count)

            if src_count <= IN_MEMORY_ID_THRESHOLD:
                source_ids = set(doc["_id"] for doc in src_coll.find({}, {"_id": 1}))
                deleted = 0
                for doc in dst_coll.find({}, {"_id": 1}):
                    if doc["_id"] not in source_ids:
                        dst_coll.delete_one({"_id": doc["_id"]})
                        deleted += 1
                logger.info("Deleted %d documents from %s.%s", deleted, DEST_DB, coll)
                continue

            deleted = 0
            for doc in dst_coll.find({}, {"_id": 1}):
                if src_coll.count_documents({"_id": doc["_id"]}, limit=1) == 0:
                    dst_coll.delete_one({"_id": doc["_id"]})
                    deleted += 1
            logger.info("Deleted %d documents from %s.%s", deleted, DEST_DB, coll)

    except PyMongoError as e:
        logger.exception("Mongo error during reconciliation: %s", e)

# -------------------------
# Main
# -------------------------
def main():
    for dst_client in dst_clients:
        incremental_upsert(dst_client)
        reconcile_deletions(dst_client)
    logger.info("Incremental live sync completed successfully for ONLY database: %s", SOURCE_DB)

if __name__ == "__main__":
    main()
