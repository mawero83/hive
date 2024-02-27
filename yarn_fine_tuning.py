import sys
# This program suggest several configuration parameters for hive cluster using Tez as execution engine.
# Note that other important configurations might be necessary like changing the lauch commands adding the following:
# -XX:+PrintGCDetails -verbose:gc -XX:
# +PrintGCTimeStamps -XX:+UseNUMA -
# XX:+UseParallelGC
# This will make a more efficient use of GC.
# Run this program as follows:  script.py <total_memory_mb> <total_cores>

def calculate_configurations(total_memory, total_cores):
    # Set initial values based on provided guidelines
    min_container_size = 1024  # This value should be determined based on your YARN minimum allocation
    max_container_size = total_memory // total_cores
    max_container_size = max(min_container_size, max_container_size)  # max container size must be >= min container size

    # Assumptions based on Hadoop and Tez best practices
    map_memory_mb = min_container_size
    reduce_memory_mb = min_container_size * 2
    am_memory_mb = min_container_size
    tez_container_size = min_container_size
    tez_am_memory_mb = min_container_size

    # Calculations based on the guidelines and best practices
    configurations = {
        'yarn.scheduler.minimum-allocation-mb': min_container_size,
        'yarn.scheduler.maximum-allocation-mb': max_container_size,
        'yarn.nodemanager.resource.memory-mb': total_memory,
        'mapreduce.map.memory.mb': map_memory_mb,
        'mapreduce.map.java.opts': int(map_memory_mb * 0.8),
        'mapreduce.reduce.memory.mb': reduce_memory_mb,
        'mapreduce.reduce.java.opts': int(reduce_memory_mb * 0.8),
        'yarn.app.mapreduce.am.resource.mb': am_memory_mb,
        'yarn.app.mapreduce.am.command-opts': int(am_memory_mb * 0.8),
        'mapreduce.task.io.sort.mb': int(0.4 * map_memory_mb),
        'tez.am.resource.memory.mb': tez_am_memory_mb,
        'hive.tez.container.size': tez_container_size,
        'tez.runtime.io.sort.mb': int(0.4 * tez_container_size),
        'tez.am.launch.cmd-opts': '-Xmx' + str(int(tez_am_memory_mb * 0.8)) + 'm',
        'tez.runtime.unordered.output.buffer.size-mb': int(0.1 * tez_container_size),
        'hive.server2.tez.sessions.per.default.queue': 1,  # This should be fine-tuned based on queue capacity
        'tez.session.am.dag.submit.timeout.secs': 300,  # Default value, adjust as necessary
        'tez.am.session.min.held-containers': 1,  # Default value, adjust based on use case
        'tez.am.resource.memory.mb': tez_am_memory_mb,
        'hive.prewarm.enabled': 'false',  # Default to false, enable as needed
        'hiveserver2_java_heapsize': int(am_memory_mb * 0.8)
    }

    return configurations


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <total_memory_mb> <total_cores>")
        sys.exit(1)

    total_memory_mb = int(sys.argv[1])
    total_cores = int(sys.argv[2])

    configs = calculate_configurations(total_memory_mb, total_cores)

    for key, value in configs.items():
        print(key + "=" + str(value))
