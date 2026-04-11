import kagglehub
import os
import pandas as pd
import json

# =========================
# Download dataset
# =========================
path = kagglehub.dataset_download(
    "computingvictor/transactions-fraud-datasets"
)

print("Downloaded:", path)

# =========================
# Seeds destination
# =========================
destination_path = r"C:\Work\Study\Projects\end-to-end-financial-analytics-platform\dbt_project\financial_analytics_project\seeds"
os.makedirs(destination_path, exist_ok=True)

SAMPLE_SIZE = 1000
MAX_INT = 2147483647


# =========================
# JSON CONVERTER
# =========================
def convert_json(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        df = pd.json_normalize(data)

    elif isinstance(data, dict):
        df = pd.DataFrame(list(data.items()), columns=["key", "value"])

    else:
        raise Exception("Unsupported JSON format")

    return df


# =========================
# CLEAN FUNCTION
# =========================
def clean_dataframe(df):

    # limit size for dbt seed safety
    if len(df) > SAMPLE_SIZE:
        df = df.sample(SAMPLE_SIZE, random_state=42)

    # clean column names
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    # fill nulls
    df = df.fillna("")

    # =========================
    # FIX 1: handle large integers (CLAMP)
    # =========================
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].apply(
                lambda x: MAX_INT if pd.notnull(x) and x > MAX_INT else x
            )

    # =========================
    # FIX 2: convert to string for dbt seed stability
    # =========================
    df = df.astype(str)

    # =========================
    # FIX 3: truncate long fields (dbt crash fix)
    # =========================
    for col in df.columns:
        df[col] = df[col].str.slice(0, 500)

    return df


# =========================
# SAFE CSV LOADER
# =========================
def safe_csv_load(file_path):
    try:
        df = pd.read_csv(file_path, on_bad_lines="skip")
    except Exception:
        df = pd.read_csv(file_path, engine="python", on_bad_lines="skip")

    return clean_dataframe(df)


# =========================
# MAIN PIPELINE
# =========================
for file_name in os.listdir(path):
    source_file = os.path.join(path, file_name)

    try:

        # ========================
        # CSV FILES
        # ========================
        if file_name.endswith(".csv"):
            df = safe_csv_load(source_file)

            dest = os.path.join(destination_path, file_name)
            df.to_csv(dest, index=False)

            print(f"✔ CSV Ready: {file_name} ({len(df)} rows)")

        # ========================
        # JSON FILES
        # ========================
        elif file_name.endswith(".json"):
            df = convert_json(source_file)
            df = clean_dataframe(df)

            new_name = file_name.replace(".json", ".csv")
            dest = os.path.join(destination_path, new_name)

            df.to_csv(dest, index=False)

            print(f"✔ JSON Ready: {new_name} ({len(df)} rows)")

    except Exception as e:
        print(f"❌ Error in file: {file_name}")
        print(e)

print("\n✅ DONE - Ready for dbt seed")