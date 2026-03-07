import pandas as pd
import snowflake.connector
import os

file = "data/dataset.xlsx"

# ANSI colors
YELLOW = "\033[93m"
BLUE = "\033[94m"
GREEN = "\033[92m"
RED = "\033[91m"
CYAN = "\033[96m"
RESET = "\033[0m"

df = pd.read_excel(file, header=None)

tests = {}
current_test = None
data = []

for index, row in df.iterrows():

    first_cell = str(row[0]).strip()

    if first_cell.startswith("TEST"):

        if current_test and data:
            tests[current_test] = pd.DataFrame(data)

        current_test = first_cell
        data = []
        continue

    if row.isnull().all():
        continue

    data.append(row.tolist())

if current_test and data:
    tests[current_test] = pd.DataFrame(data)

report_lines = []

all_test_data = ""

for test, table in tests.items():

    header = f"{YELLOW}========== {test} =========={RESET}"
    print(header)
    report_lines.append(header)

    for _, row in table.iterrows():

        row_text = " | ".join([str(x) for x in row])

        if "ONLY_IN_SRC" in row_text:
            colored_row = f"{RED}{row_text}{RESET}"

        elif "SRC" in row_text:
            colored_row = f"{BLUE}{row_text}{RESET}"

        elif "TGT" in row_text:
            colored_row = f"{GREEN}{row_text}{RESET}"

        else:
            colored_row = row_text

        print(colored_row)
        report_lines.append(colored_row)

    all_test_data += f"\n{test}\n"
    all_test_data += table.to_string(index=False)


# -------- Snowflake Connection --------

conn = snowflake.connector.connect(
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
    database=os.environ['SNOWFLAKE_DATABASE'],
    schema=os.environ['SNOWFLAKE_SCHEMA']
)

cursor = conn.cursor()

prompt = f"""
Analyze the following data reconciliation tests.

1. Identify SRC vs TGT mismatches
2. Detect ONLY_IN_SRC records
3. Provide root cause
4. Provide summary for each TEST

Data:
{all_test_data}
"""

query = f"""
SELECT SNOWFLAKE.CORTEX.COMPLETE(
'snowflake-arctic',
$$ {prompt} $$
);
"""

cursor.execute(query)

result = cursor.fetchone()[0]

summary_header = f"\n{CYAN}========== AI ANALYSIS SUMMARY =========={RESET}\n"

print(summary_header)
print(result)

report_lines.append(summary_header)
report_lines.append(result)

# Save colorful report
with open("analysis_report.txt", "w") as f:
    for line in report_lines:
        f.write(line + "\n")

cursor.close()
conn.close()