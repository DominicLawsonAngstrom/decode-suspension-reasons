Dynamic decoding

- Add a new column - decoded lsr (dlsr)/decoded ssr (dssr)
- Use a UDF to decode
- Lambda to update the json data directly

- Data hits bucket -> run UDF in lambda to decode -> add new columns to JSON

- ORIGINAL TABLE
- LEFT JOIN ON NEW DATA TO DECODE SUSPENSION REASONS
- DSSR/DLSR COLUMNS WITH NULL ARE THEN RETURNED 
	- USE TRIGGER FUNCTIONS? - IF A JOIN ISN'T POSSIBLE, DECODE THE CHAR STRING AND UPDATE THE TABLE? UDF IN DB
- DECODE MISSING COMBINATIONS AND UPDATE REFERENCE TABLE

Reference table containing the conversions, all iterations that have appeared
All available combinations decoded instantly
Identify instances that cannot be decoded - HOW? -> return error if not decoded
Decode them the long way
Add them to the table for future use

Workflow idea

- Create Reference data table -> Static table in Athena?
- Ingest data
- Do SSR and LSR conversion with join to reference table, creating columns DSSR and DLSR, when the views are refreshed (add join to SQL script)
WITHIN A LAMBDA:
	- Trigger on (new data in the asf_athena/kafka_topics/stagingus/baseball/data_published/fixturemarkets/ OR after refresh script 	runs - step function/glue?/where/how is view refresh triggered?) 
	- MUST HAPPEN AFTER REFRESH SCRIPT RUNS 
	- Query data and check for NULL values in DSSR and DLSR columns
		- If none, END
	- Get unique suspension code combinations
	- Decode SSR and LSR combos and (insert new results into reference data table/update suspension code conversion JSON)
	- Trigger refresh views

