### Medley Example Code

The [Job.ecl](Job.ecl) file controls 99% of the runtime behavior within the BWR_* code files.  Please
see that file for a description of attributes that should be modified for test runs.

[BWR\_01\_Create\_Lookup\_Indexes.ecl](BWR\_01\_Create\_Lookup\_Indexes.ecl) is the only job that is
actually required in the test harness.  It creates the indexes used to implement searches against the data declared in
the Job.ecl file.  All other BWR_* files perform tests or analysis against these indexes.

[BWR\_02\_Analyze\_Deduped\_Lookup.ecl](BWR\_02\_Analyze\_Deduped\_Lookup.ecl) performs selected
analysis on the "fuzzy deduplication" results -- records from the original dataset that match fuzzily.

[BWR\_03\_Find\_Related\_Entity\_IDs.ecl](BWR\_03\_Find\_Related\_Entity\_IDs.ecl) performs a search
for fuzzy-match records given a set of entity IDs.

[BWR\_04\_Search\_For\_Related\_Entity\_IDs.ecl](BWR\_04\_Search\_For\_Related\_Entity\_IDs.ecl)
performs a fuzzy search given actual data (as from a web search form).  Note that portions of this file need to be
modified if the source data in Job.ecl changes.
