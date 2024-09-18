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

CHECK_FOR_NEW_ENCODINGS_QUERY = """
    SELECT
        *
    from
        reference_data_sit.distinct_suspension_reason_decoding

"""
    
def track_query_execution(query_id: str) -> dict:
    """Track the status of the Athena query and return results when they are ready.
    
    Args:
        query_id (str): The ID of the Athena query to track.

    Returns:
        dict: The results of the query if it succeeds.
        str: The status of the query execution.
    """
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

def check_for_new_suspension_reason_encodings() -> dict:
    """Check the fixturemarkets_fmt_s3 view for ssr/lsr columns that were not decoded and return the encodings.
    
    Returns:
        dict: The results of the Athena query checking for new suspension reason encodings.
    """
    query = CHECK_FOR_NEW_ENCODINGS_QUERY

    query_exc = athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={"OutputLocation": STAGING_DIR},
    )

    query_id = query_exc["QueryExecutionId"]

    result, status = track_query_execution(query_id)

    return result

def format_query_results(result: dict) -> list:
    """Convert the results from the Athena query from a dict into a list of values.
    
    Args:
        result (dict): The result set from an Athena query.

    Returns:
        list: A list of values extracted from the query result.
    """
    results = result["ResultSet"]["Rows"]
    rows = [result["Data"] for result in results]
    columns = [row[0]["VarCharValue"] for row in rows]
    results = columns[1:]

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

        query_exc = athena.start_query_execution(
            QueryString=query,
            ResultConfiguration={"OutputLocation": STAGING_DIR},
        )

        query_id = query_exc["QueryExecutionId"]

        result, status = track_query_execution(query_id)
        statuses.append(status)

    if all([status == "SUCCEEDED" for status in statuses]):
        return "SUCCEEDED"
    
    return statuses

def report_job(type: str, name: str, start: str, end: str, exec_time: str, status: str | list, encodings_added: list):
    """Send a status report to the DE job reporting dashboard.
    
    Args:
        type (str): The type of task being run.
        name (str): Name of the task being run.
        start (str): Start time of the task being run.
        end (str): End time of the task being run.
        exec_time (str): Total execution time of the task being run.
        status (str): Status of the task being run.
        encodings_added (dict): Newly added encodings.

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
        "EncodingsAdded" : encodings_added
    }

    load_json = json.dumps(payload)
    json_str = str(load_json)

    step.start_execution(
        stateMachineArn="arn:aws:states:eu-west-2:373797717611:stateMachine:DE-Job-History_TARGET",
        input=json_str
    )

if __name__ == "__main__":
    start_time = datetime.now()
    encodings_with_no_decode = check_for_new_suspension_reason_encodings()
    encoded_suspension_reasons = format_query_results(encodings_with_no_decode)
    decoded_suspension_reasons = [decode_suspension_reason(reason) for reason in encoded_suspension_reasons]
    suspension_reason_encodings = list(zip(encoded_suspension_reasons, decoded_suspension_reasons))
    status = insert_into_encodings_table(suspension_reason_encodings)
    end_time = datetime.now()
    execution_time = str((end_time - start_time).total_seconds()) + "s"

    report_job(
        "ETL", 
        "MLB - Decode Suspension Reasons", 
        start_time.strftime("%d/%m/%Y - %H:%M:%S"), 
        end_time.strftime("%d/%m/%Y - %H:%M:%S"), 
        execution_time, 
        status, 
        suspension_reason_encodings
    )

    print("----------------------")
    print("---Process Complete---")