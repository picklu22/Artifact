import pandas as pd
import snowflake.connector
import google.generativeai as genai

# ---------------------------
# 1️⃣ Configure Gemini
# ---------------------------
genai.configure(api_key="YOUR_GEMINI_API_KEY")

model = genai.GenerativeModel("gemini-1.5-flash")

# ---------------------------
# 2️⃣ Connect to Snowflake
# ---------------------------
conn = snowflake.connector.connect(
    user="YOUR_USER",
    password="YOUR_PASSWORD",
    account="YOUR_ACCOUNT",
    warehouse="YOUR_WAREHOUSE",
    database="YOUR_DATABASE",
    schema="YOUR_SCHEMA"
)

cursor = conn.cursor()

# ---------------------------
# 3️⃣ Fetch Snowflake Metadata
# ---------------------------
metadata_query = """
SELECT table_name, column_name
FROM information_schema.columns
WHERE table_schema = CURRENT_SCHEMA()
ORDER BY table_name;
"""

cursor.execute(metadata_query)
metadata = cursor.fetchall()

# Convert metadata to readable format
metadata_text = ""
for table, column in metadata:
    metadata_text += f"Table: {table}, Column: {column}\n"

# ---------------------------
# 4️⃣ Read CSV File
# ---------------------------
df = pd.read_csv("input.csv")

generated_sql = []

# ---------------------------
# 5️⃣ Generate SQL using Gemini
# ---------------------------
for index, row in df.iterrows():

    src_logic = row["Src logic"]
    target_logic = row["Target logic"]

    prompt = f"""
    You are a Snowflake SQL expert.

    Below is Snowflake metadata:
    {metadata_text}

    Convert the following business logic into valid Snowflake SQL.

    Source Logic:
    {src_logic}

    Target Logic:
    {target_logic}

    Rules:
    - Use only available tables and columns from metadata
    - Generate clean Snowflake SQL
    - Do not hallucinate tables
    - Provide only SQL query
    """

    response = model.generate_content(prompt)
    sql_query = response.text.strip()

    generated_sql.append(sql_query)

# ---------------------------
# 6️⃣ Save Output
# ---------------------------
df["Generated_SQL"] = generated_sql
df.to_csv("output_with_sql.csv", index=False)

print("✅ SQL generation completed. Check output_with_sql.csv")
