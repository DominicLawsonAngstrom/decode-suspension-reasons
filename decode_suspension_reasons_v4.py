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

SCHEMAS = ["baseball_stagingus"]
TABLES = []

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
            status = "EXCEPTION OCCURED"
            raise f"An exception occured while collecting all unique suspension reason combinations: {e}"

    if status != "SUCCEEDED":
        print(f"Query failed with status {status}")

    print(status)

    result = athena.get_query_results(QueryExecutionId=query_id)

    return result, status

def check_for_new_suspension_reason_tables() -> dict:
    """Check schemas in SCHEMAS list for tables that contain ssr/lsr columns.
    
    Returns:
        tuple(dict, str):
            - The results of the Athena query checking for new suspension reason encodings
            - Status of query executions.
    """
    query = IDENTIFY_TABLES_WITH_SUSPENSION_REASONS_COLUMNS

    result, status = query_execution(query)

    return result, status

def get_last_days_fixture_keys(n: int) -> dict:
    """Get the fixture keys for fixtures that occured in the last n hours"""

    TIME_PARTITIONED_FIXTURE_KEYS_QUERY = f"""
        SELECT
            DISTINCT FixtureKey
        FROM
            baseball_stagingus.kafka_fixturemarkets
        WHERE
            "$file_modified_time" >= current_timestamp - INTERVAL '{n}' HOUR;
    """

    results, status = query_execution(TIME_PARTITIONED_FIXTURE_KEYS_QUERY)
    return results, status

def check_for_new_suspension_reason_encodings(tables_schemas: list[str], fixture_keys: list[str]) -> dict:
    """Check all tables with ssr/lsr columns for entries that were not decoded and return the encodings.

    Args:
        tables_schemas (list[tuple(str, str)]): list of table_name, table_schema tuples to be searched
        fixture_keys (list): list of fixture keys to be searched for

    Returns:
        tuple(dict, str):
            - The results of the Athena query checking for new suspension reason encodings
            - Status of query execution
    """
    CHECK_FOR_NEW_ENCODINGS = []
    fixture_keys_with_quotes = [f"'{key}'" for key in fixture_keys]

    for table, schema in tables_schemas:
        CHECK_FOR_NEW_ENCODINGS.append(f'''
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
        ''')

    CHECK_FOR_NEW_ENCODINGS_QUERY = ' UNION '.join(CHECK_FOR_NEW_ENCODINGS)
    print(CHECK_FOR_NEW_ENCODINGS_QUERY)

    result, status = query_execution(CHECK_FOR_NEW_ENCODINGS_QUERY)

    return result, status

def format_query_results(result: dict) -> list:
    """Convert the results from the Athena query from a dict into a list of lists, each inner list being a row of the table.
    
    Args:
        result (dict): The result set from an Athena query.

    Returns:
        list: A list of values extracted from the query result.
    """
    results = result["ResultSet"]["Rows"]
    rows = [result["Data"] for result in results]
    columns = [[r["VarCharValue"] for r in row] for row in rows]
    results = columns[1:] # Remove header row

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

def insert_into_encodings_table(encodings: list) -> str:
    """Insert new encodings into the suspension reasons table in Athena.
    
    Args:
        encodings (list[tuple]): List of pairs of encoded and decoded suspension reasons.

    Returns:
        str: The status of the job execution.
    """

    if len(encodings) < 1:
        return "SUCCEEDED"
    statuses = []
    for encoded, decoded in encodings:
        query = f"""
            INSERT INTO mlb_sit_views.vw_suspension_reasons
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
    json_str = str(load_json)

    step.start_execution(
        stateMachineArn="arn:aws:states:eu-west-2:373797717611:stateMachine:DE-Job-History_TARGET",
        input=json_str
    )

if __name__ == "__main__":
    start_time = datetime.now()

    TABLES_SCHEMAS, status = check_for_new_suspension_reason_tables()
    TABLES_SCHEMAS = format_query_results(TABLES_SCHEMAS)

    fixtures, status = get_last_days_fixture_keys(48)
    fixtures = format_query_results(fixtures)
    fixtures_list = [fixture[0] for fixture in fixtures]

    if len(fixtures_list) < 1:
        status = status + " - No new fixtures"
        # finish_job(start_time, status)

    new_encodings, status = check_for_new_suspension_reason_encodings(TABLES_SCHEMAS, fixtures_list)
    new_encodings = format_query_results(new_encodings)
    encoded_suspension_reasons = [encoding[0] for encoding in new_encodings]

    if len(new_encodings) < 1:
        status = status + " - No new encodings"
        # finish_job(start_time, status)

    decoded_suspension_reasons = [decode_suspension_reason(reason) for reason in encoded_suspension_reasons]
    suspension_reason_encodings = list(zip(new_encodings, decoded_suspension_reasons))

    status = insert_into_encodings_table(suspension_reason_encodings)
    status = status + f" - encodings added : {suspension_reason_encodings}"

    # finish_job(start_time, status)

    print("----------------------")
    print("---Process Complete---")