AI Disclosures for this Assignment:

- I used ChatGPT as my LLM assistant

I used the LLM for the following purposes:
- Summarizing the assignment and asking doubts about the Spark and Ray cluster structures
- Finding necessary boilerplates from the slides and enquiring what each aspect did so that I can then modify it later as needed.
- Help in translating equivalent preprocessing logic between PySpark and Ray Data syntax
- Help in exact syntax of some commands (for example exact flags to accomplish something that was needed)
- Debugging Ray memory issues, Spark timestamp handling and the output validation script
- Optimizing the Ray shuffle and batch UDF implementation; this is described in the Performance Tuning Note below
- Beautifying my README to have nice bash script boundaries based on the commands i ran
- Beautifying the report, rephrasing my original sentences to look more clean and appealing, and presenting all the necessary info in neatly in well-formatted sections, tables and visualizations

Performance Tuning Note:

An LLM identified that Ray's global duplicate-removal group-by was producing oversized and memory-intensive shuffle work. Ray 2.58's hash-shuffle v2 was enabled with 64 output partitions and 256 MiB input batches. The object store was increased to 12 GiB to reduce backpressure and reactive spilling. Ray output was changed from append to overwrite for repeatable runs, and the batch UDF was given 50,000-row batches, two actor workers, and a list comprehension instead of DataFrame.apply with axis=1. The cleaning rules, joins, feature formula and output columns were not changed.
