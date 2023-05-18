#include <rocksdb/db.h>

int main(int argc, const char *argv[]) {
    rocksdb::DB* db;
    rocksdb::Options options;

    // Open the database
    rocksdb::Status status = rocksdb::DB::Open(options, argv[1], &db);
    if (!status.ok()) {
        // Handle the error
        return 1;
    }

    // Trigger a manual compaction for the entire database
    rocksdb::CompactRangeOptions compactOptions;
    db->CompactRange(compactOptions, nullptr, nullptr);

    // Close the database
    delete db;

    return 0;
}

