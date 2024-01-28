#!/bin/bash

# Define the size threshold for 'small' files in bytes
# For example, 1000000 for files smaller than 1MB
SMALL_SIZE_THRESHOLD=1000000

# Function to count small files in a given HDFS path
count_small_files() {
    local path=$1
    hadoop fs -ls $path | awk -v threshold=$SMALL_SIZE_THRESHOLD '
    BEGIN { count = 0; }
    {
        if ($5 < threshold) { 
            count++; 
        }
    }
    END { print count; }'
}

# Get the list of tables from Hive
TABLES=$(hive -S -e 'SHOW TABLES;')

# Iterate over each table
for table in $TABLES; do
    # Get the HDFS location of the table
    LOCATION=$(hive -S -e "DESCRIBE FORMATTED $table;" | grep 'Location:' | awk '{print $NF}')
    
    # Count small files in this location
    SMALL_FILES=$(count_small_files $LOCATION)

    echo "Table: $table, Small Files: $SMALL_FILES"
done
