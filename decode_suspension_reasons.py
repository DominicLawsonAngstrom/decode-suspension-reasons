import boto3
import json
import time
from datetime import datetime

REGION_NAME = "eu-west-2"

# Determines the location to send query metadata.
STAGING_DIR = "s3://asf-athena-query-results/"

# AWS session and clients.
session = boto3.Session(region_name=REGION_NAME)
athena = session.client("athena")
step = session.client("stepfunctions")

# Suspension reason code used to decode new SSR/LSR format
SUSPENSION_REASONS = {
    "-": "Unknown",
    "A": "StalePricing",
    "B": "Settled",
    "C": "OutsideLineRange",
    "D": "BelowMinimumPrice",
    "E": "NoSelection",
    "F": "MatchVoided",
    "H": "NoMainLine",
    "I": "NoUnsettledLines",
    "J": "NoUnsuspendedLines",
    "K": "SettlementCorrection"
}

# Schemas to be checked
# When a table is added into this list, another entry must be added to ENCODINGS_TABLE_SCHEMAS to associate an encodings table to
# the schema
SCHEMAS = ["baseball_stagingus", "ncaab_stagingus"]

# Schemas where the encodings table resides for each sport_env schema
ENCODINGS_TABLE_SCHEMAS = {
    'baseball_stagingus': 'mlb_sit_views',
    'ncaab_stagingus': 'ncaab_sit_views',
}

def query_execution(query: str) -> dict:
    """Track the status of the Athena query and return results when they are ready.
    
    Args:
        query_id (str): The ID of the Athena query to track.

    Returns:
        dict: The results of the query if it succeeds.
        str: The status of the query execution.
    """
    query_exc = athena.start_query_execution(
            QueryString=query,
            ResultConfiguration={"OutputLocation": STAGING_DIR},
        )

    query_id = query_exc["QueryExecutionId"]
    
    possible_statuses = ['QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED']
    status = None

    while True:
        try:
            response = athena.get_query_execution(QueryExecutionId=query_id)
            status = response["QueryExecution"]["Status"]["State"]

            if status in possible_statuses[2:]:
                break
            print("Working...")
            time.sleep(3)

        except Exception as e:
            status = f"EXCEPTION OCCURED: {e}"
            raise f"An exception occured while collecting all unique suspension reason combinations: {e}"

    if status != "SUCCEEDED":
        print(f"Query failed with status {status}")

    print(status)

    result = athena.get_query_results(QueryExecutionId=query_id)

    return result, status

def check_for_new_suspension_reason_tables() -> dict:
    """Check schemas in SCHEMAS list for tables in them that contain ssr/lsr columns.
    
    Returns:
        tuple(dict, str):
            - The results of the Athena query checking for new suspension reason encodings
            - Status of query executions.
    """
    
    IDENTIFY_TABLES_WITH_SUSPENSION_REASONS_COLUMNS = """
        SELECT
            DISTINCT(table_name) AS table_name,
            table_schema
        FROM
            information_schema.columns
        WHERE
            table_schema IN ({schemas})
            AND (
                column_name = 'selectionsuspensionreasons' 
                OR column_name = 'linesuspensionreasons'
            ) AND table_name LIKE '%fmt%'
    """.format(
        schemas=",".join(f"'{schema}'" for schema in SCHEMAS)
    )
    query = IDENTIFY_TABLES_WITH_SUSPENSION_REASONS_COLUMNS

    result, status = query_execution(query)

    return result, status

def get_fixture_keys_from_last_n_hours(schema: str, n: int) -> dict:
    """Get the fixture keys for fixtures that occured in the last n hours"""

    TIME_PARTITIONED_FIXTURE_KEYS_QUERY = f"""
        SELECT
            DISTINCT FixtureKey
        FROM
            {schema}.kafka_fixturemarkets
        WHERE
            "$file_modified_time" >= current_timestamp - INTERVAL '{n}' HOUR;
    """

    results, status = query_execution(TIME_PARTITIONED_FIXTURE_KEYS_QUERY)
    return results, status

def check_for_new_suspension_reason_encodings(schema: str, table: str, fixture_keys: list) -> dict:
    """Check a table with ssr/lsr columns for entries that were not decoded and return the encodings.

    Args:
        schema str: schema to be searched
        table str: table to be searched
        fixture_keys (list): list of fixture keys to be searched for

    Returns:
        tuple(dict, str):
            - The results of the Athena query checking for new suspension reason encodings
            - Status of query execution
    """
    fixture_keys_with_quotes = [f"'{key}'" for key in fixture_keys]

    CHECK_FOR_NEW_ENCODINGS_QUERY= f'''
        SELECT DISTINCT SelectionSuspensionReasons
        FROM
            {schema}.{table}
        WHERE (SelectionSuspensionReasons = SelectionSuspensionReason_str)
            {"AND FixtureKey IN ("+', '.join(fixture_keys_with_quotes)+")" if len(fixture_keys_with_quotes) > 0 else ""}
        UNION    SELECT DISTINCT LineSuspensionReasons
        FROM
            {schema}.{table}
        WHERE (LineSuspensionReasons = LineSuspensionReason_str)
            {"AND FixtureKey IN ("+', '.join(fixture_keys_with_quotes)+")" if len(fixture_keys_with_quotes) > 0 else ""}
    '''

    result, status = query_execution(CHECK_FOR_NEW_ENCODINGS_QUERY)

    return result, status

def format_query_results(result: dict, single_column: bool = False) -> list:
    """Convert the results from the Athena query from a dict into a list of lists, each inner list being a row of the table.
    
    Args:
        result (dict): The result set from an Athena query.
        single_column (bool): Argument to show whether the expected output has a single column for each row

    Returns:
        list: A list of values extracted from the query result.
    """
    results = result["ResultSet"]["Rows"]
    rows = [result["Data"] for result in results]
    columns = [[r["VarCharValue"] for r in row] for row in rows]
    results = columns[1:] # Remove header row
    results = [result[0] for result in results] if single_column else results
    return results

def decode_suspension_reason(text: str) -> str:
    """Decode the encoded suspension reason string into a readable format.
    
    Args:
        text (str): The encoded suspension reason string.

    Returns:
        str: The decoded suspension reason string.
    """
    if type(text) != str:
        return None

    split_text = list(text)
    decoded_reasons_list = [SUSPENSION_REASONS.get(char) for char in split_text]
    decoded_reasons = ";".join(decoded_reasons_list)

    return decoded_reasons

def insert_into_encodings_table(schema: str, encodings: list) -> str:
    """Insert new encodings into the suspension reasons table in Athena.
    
    Args:
        schema (str): Schema associated with the tables the encodings originated from
        encodings (list[tuple]): List of pairs of encoded and decoded suspension reasons.

    Returns:
        str: The status of the job execution.
    """

    if len(encodings) < 1:
        return "SUCCEEDED"
    statuses = []
    for encoded, decoded in encodings:
        query = f"""
            INSERT INTO {ENCODINGS_TABLE_SCHEMAS[schema]}.vw_suspension_reasons
            VALUES {"('"+encoded+"','"+decoded+"');"}
        """
        print(query + " executing...")

        result, status = query_execution(query)
        statuses.append(status)

    if all([status == "SUCCEEDED" for status in statuses]):
        return "SUCCEEDED"
    
    return statuses

def finish_job(start_time: datetime, status: str) -> None:
    end_time = datetime.now()
    execution_time = str((end_time - start_time).total_seconds()) + "s"

    report_job(
        "ETL", 
        "MLB - Decode Suspension Reasons", 
        start_time.strftime("%d/%m/%Y - %H:%M:%S"), 
        end_time.strftime("%d/%m/%Y - %H:%M:%S"), 
        execution_time, 
        status,
    )
    exit(0)

def report_job(type: str, name: str, start: str, end: str, exec_time: str, status: str):
    """Send a status report to the DE job reporting dashboard.
    
    Args:
        type (str): The type of task being run.
        name (str): Name of the task being run.
        start (str): Start time of the task being run.
        end (str): End time of the task being run.
        exec_time (str): Total execution time of the task being run.
        status (str): Status of the task being run.

    Returns:
        None
    """
    payload = {
        "Type" : type,
        "Name" : name,
        "StartDate" : start,
        "EndDate" : end,
        "ExecutionTime": exec_time,
        "Status" : status,
    }

    load_json = json.dumps(payload)

    step.start_execution(
        stateMachineArn="arn:aws:states:eu-west-2:373797717611:stateMachine:DE-Job-History_TARGET",
        input=load_json
    )

if __name__ == "__main__":
    start_time = datetime.now()

    try:
        TABLES_SCHEMAS, status = check_for_new_suspension_reason_tables()
        TABLES_SCHEMAS = format_query_results(TABLES_SCHEMAS)

        print(f'Table_schemas - {TABLES_SCHEMAS}')

        # Get the recent fixtures for each schema
        schemas = [ts[1] for ts in TABLES_SCHEMAS]
        fixtures_by_schema = {}
        for schema in list(set(schemas)):
            fixtures, status = get_fixture_keys_from_last_n_hours(schema, 25)
            fixtures = format_query_results(fixtures, True)
            fixtures_by_schema[schema] = fixtures
        # If there are no new fixtures in any of the schemas, end the job
        if not any([len(fixtures) > 1 for fixtures in list(fixtures_by_schema.values())]):
            status = status + " - No new fixtures"
            print(status)
            finish_job(start_time, status)

        print(f'New fixtures - {fixtures_by_schema}')

        new_encodings_dict = {}
        for table, schema in TABLES_SCHEMAS:
            fixtures_list = fixtures_by_schema[schema]
            new_encodings, status = check_for_new_suspension_reason_encodings(schema, table, fixtures_list)
            encoded_suspension_reasons = format_query_results(new_encodings, True)
            # Remove null or empty strings as encodings
            encoded_suspension_reasons = list(filter(lambda x: bool(x), encoded_suspension_reasons))
            if len(encoded_suspension_reasons) < 1:
                print(f"No new encodings for {table}.{schema}")
                continue
            print(f'New encodings for {table}.{schema} - {encoded_suspension_reasons}')

            decoded_suspension_reasons = [decode_suspension_reason(reason.upper()) for reason in encoded_suspension_reasons]
            suspension_reason_encodings = list(zip(encoded_suspension_reasons, decoded_suspension_reasons))
            print(f'Decoded - {suspension_reason_encodings}') if suspension_reason_encodings else ''
            new_encodings_dict.setdefault(schema, []).extend(suspension_reason_encodings)

        # If not any new encodings in any of the tables, finish the job
        if not any([len(encodings) > 1 for encodings in list(new_encodings_dict.values())]):
            status = status + " - No new encodings"
            print(status)
            finish_job(start_time, status)

        for key in new_encodings_dict.keys():
            status = insert_into_encodings_table(key, new_encodings_dict[key])
            # raise error if run fails for any of the tables
            if status in ['FAILED','CANCELLED']:
                print(f"Insert into encoding table {ENCODINGS_TABLE_SCHEMAS[schema]} failed for {schema}.{table}")
                continue

        status = status + f" - Encodings added : {new_encodings_dict}"
        print(status)

        finish_job(start_time, status)
    except Exception as e:
        print(e)
        finish_job(start_time, e)

    print("----------------------")
    print("---Process Complete---")