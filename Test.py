import pandas as pd
import snowflake.connector
import google.generativeai as genai
import json

# ---------------------------------------
# 1️⃣ Configure Gemini 2.5 Flash
# ---------------------------------------
genai.configure(api_key="YOUR_GEMINI_API_KEY")

model = genai.GenerativeModel(
    model_name="gemini-2.5-flash",
    generation_config={
        "temperature": 0,          # deterministic output
        "top_p": 0.95,
        "max_output_tokens": 2048
    }
)

# ---------------------------------------
# 2️⃣ Connect to Snowflake
# ---------------------------------------
conn = snowflake.connector.connect(
    user="YOUR_USER",
    password="YOUR_PASSWORD",
    account="YOUR_ACCOUNT",
    warehouse="YOUR_WAREHOUSE",
    database="YOUR_DATABASE",
    schema="YOUR_SCHEMA"
)

cursor = conn.cursor()

# ---------------------------------------
# 3️⃣ Fetch Metadata as JSON (Better than text)
# ---------------------------------------
metadata_query = """
SELECT table_name, column_name
FROM information_schema.columns
WHERE table_schema = CURRENT_SCHEMA()
ORDER BY table_name, column_name;
"""

cursor.execute(metadata_query)
metadata_rows = cursor.fetchall()

metadata_dict = {}

for table, column in metadata_rows:
    if table not in metadata_dict:
        metadata_dict[table] = []
    metadata_dict[table].append(column)

metadata_json = json.dumps(metadata_dict, indent=2)

# ---------------------------------------
# 4️⃣ Read Input CSV
# ---------------------------------------
df = pd.read_csv("input.csv")

generated_sql = []

# ---------------------------------------
# 5️⃣ Generate SQL Using Gemini 2.5 Flash
# ---------------------------------------
for index, row in df.iterrows():

    src_logic = row["Src logic"]
    target_logic = row["Target logic"]

    prompt = f"""
You are an expert Snowflake SQL developer.

Below is the Snowflake schema metadata in JSON format:
{metadata_json}

Task:
Convert the following business logic into valid Snowflake SQL.

Source Logic:
{src_logic}

Target Logic:
{target_logic}

Rules:
- Use ONLY tables and columns from metadata
- Do NOT hallucinate columns
- Follow Snowflake SQL syntax
- Return ONLY SQL query
"""

    response = model.generate_content(prompt)
    sql_query = response.text.strip()

    generated_sql.append(sql_query)

# ---------------------------------------
# 6️⃣ Optional: Validate SQL (Recommended)
# ---------------------------------------
validated_sql = []

for sql in generated_sql:
    try:
        cursor.execute(f"EXPLAIN {sql}")
        validated_sql.append(sql)
    except Exception as e:
        validated_sql.append(f"-- INVALID SQL\n-- {str(e)}\n{sql}")

# ---------------------------------------
# 7️⃣ Save Output CSV
# ---------------------------------------
df["Generated_SQL"] = validated_sql
df.to_csv("output_with_sql.csv", index=False)

print("✅ SQL generation completed using Gemini 2.5 Flash.")
