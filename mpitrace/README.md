# STEAM Dumpi Utilities

Utility to parse dumpi traces to generate JSON DAG (Directed Acyclic Graph) file


## Description

1. Dumpi trace to event file (trace2event.py)
   It reads a set of dumpi trace files and generate a text file which has network events.
   It also produces an .csv file which has the statistics of communication in the trace files.

2. Event file to DAG file in JSON format (event2dag.py)
   It reads the output trace2event.py file and generate a text file which has network events.
   The output is for ISI-ffsim-opera simulator.

3. Event file to a input file for Opera simulator (event2htsim.py)
   It reads the output of trace2event.py and generates a text file having independent network flows.
   It should be noted that all the dependency information is not handled by Opera simulator.

4. Jason beautify script (json_beautify.py)
   It reads a Json text file and inserts new line to make it more readable

### Dependencies

* Python 3

### Executing program

The following execution will generate two files - $DUMPI_trace_directory/$output_file and $DUMP_trace_directory/event_$output_file.
The file event_$output_file has the events.
The file $output_file has communication statistics

```
python trace2event.py $DUMPI_trace_directory $output_file $number_of_MPI_Ranks
```

The following execution will read a event file ($event_file) and generates a dag in json to STDIO.
```
python event2dag.py $event_file $number_of_MPI_Ranks
```
