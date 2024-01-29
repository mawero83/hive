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

IFS="
"
DBS=$(hive -e 'SHOW DATABASES;')
DBS=`echo "$DBS"|sed 's/+//g;s/|//g;s/ //g;s/-//g'|grep -v database_name| strings`
echo "--------------------"
echo "$DBS"
echo "--------------------"

# Iterate over each table
for db in `echo "$DBS"`; do
    # Get the HDFS location of the table
    TABLES=$(hive -e 'SHOW TABLES;')
    TABLES=`echo "$TABLES"|sed 's/+//g;s/|//g;s/ //g;s/-//g'|grep -v tab_name| strings`
    for table in `echo "$TABLES"`;  do

    LOCATION=$(hive -e "DESCRIBE FORMATTED ${db}.${table};" | grep 'Location:' | awk '{print $4}')

    # Count small files in this location
    SMALL_FILES=$(count_small_files $LOCATION)

    echo "Table: $db, $table, Small Files: $SMALL_FILES"

    done
done

