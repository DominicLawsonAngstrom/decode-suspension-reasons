import json
import datetime
import math
import itertools, os

suspension_reasons = {
    "-": "Unknown",
    "A": "Stale Pricing",
    "B": "Settled",
    "C": "OutsideLineRange",
    "D": "BelowMinimumPrice",
    "E": "NoSelection",
    "F": "MatchVoided",
    "H": "NoMainLine",
    "I": "NoUnsettledLines"
    "J" "NoUnsuspendedLines",
    "K": "SettlementCorrection"
}

# ssr_path = "body{p[{m[{l[{s[{ssr}]}]}]}]}"
# lsr_path = "body-p-m-l-lsr"

def decode_suspension_reason(text: str) -> str:
    if type(text) != str:
        return None

    split_text = list(text)
    decoded_reasons_list = [suspension_reasons.get(char) for char in split_text]
    decoded_reasons = ", ".join(decoded_reasons_list)

    return decoded_reasons

def find_suspension_keys(body: dict, reason_combinations: dict):
        # iterate over a copy of the dictionary items to avoid runtime errors

        for key, value in list(body.items()):
            if isinstance(value, str):
                if key == "ssr":
                    try:
                        body["dssr"] = reason_combinations[value]
                    except KeyError as e:
                        body["dssr"] = decode_suspension_reason(value)
                        reason_combinations[value] = body["dssr"]
                    # print(f"""ssr: {body["ssr"]} -> dssr: {body["dssr"]}""")
                elif key == "lsr":
                    try:
                        body["dlsr"] = reason_combinations[value]
                    except KeyError as e:
                        body["dlsr"] = decode_suspension_reason(value)
                        reason_combinations[value] = body["dlsr"]
                else:
                    continue
            if isinstance(value, list):
                for item in value:
                    find_suspension_keys(item, reason_combinations)
            if isinstance(value, dict):
                find_suspension_keys(value, reason_combinations)

        return body

def main():

    # SAMPLE_DATA = None
    reason_combinations = {}

    files = os.listdir("./data")
    for file in files:
        print(os.path.join("data", file))
        with open(os.path.join("data", file), "r") as file:
            data = file.read()
            try:
                SAMPLE_DATA = json.loads(data)
            except Exception as e:
                try:
                    # ï»¿ weird string found
                    SAMPLE_DATA = json.loads(data[3:])
                except Exception as e:
                    print(f"Unexpected error: {e}")

        body = SAMPLE_DATA["body"]

        updated_body = find_suspension_keys(body, reason_combinations)
        print(reason_combinations)



    # print(updated_body)
times = []
for i in range(1):
    start_time = datetime.datetime.now()
    main()
    end_time = datetime.datetime.now()

    time_taken = end_time - start_time
    times.append(time_taken.total_seconds())

mean_time = sum(times)/len(times)
print(mean_time*1000)

# string = "abcdefhijk"
# combos = []

# for r in range(1, len(string)+1):
#     for combo in itertools.combinations(string, r):
#         alphabetical_order = "".join(sorted(combo))
#         if combo not in combos:
#             combos.append(combo)

# print(len(combos))