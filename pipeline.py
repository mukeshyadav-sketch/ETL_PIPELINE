import logging
import requests
import pandas as pd
import sqlite3

# ---------------- LOGGING ----------------
logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ---------------- EXTRACT ----------------
def extract_data():
    try:
        logging.info("Calling external API")
        response = requests.get(
            "https://jsonplaceholder.typicode.com/users",
            timeout=5
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"API failure: {e}")
        return []

# ---------------- TRANSFORM ----------------
def transform_data(raw_data):
    rows = []
    for user in raw_data:
        rows.append({
            "user_id": user.get("id"),
            "name": user.get("name"),
            "username": user.get("username"),
            "email": user.get("email"),
            "phone": user.get("phone"),
            "website": user.get("website"),
            "city": user.get("address", {}).get("city"),
            "zipcode": user.get("address", {}).get("zipcode"),
            "latitude": user.get("address", {}).get("geo", {}).get("lat"),
            "longitude": user.get("address", {}).get("geo", {}).get("lng"),
            "company_name": user.get("company", {}).get("name"),
            "company_phrase": user.get("company", {}).get("catchPhrase"),
            "company_bs": user.get("company", {}).get("bs")
        })
    return pd.DataFrame(rows)

# ---------------- INSIGHTS ----------------
def generate_insights(df):
    print("\n--- DATA INSIGHTS ---")
    print(f"Total users: {len(df)}")
    print(f"Unique cities: {df['city'].nunique()}")
    print(f"Unique companies: {df['company_name'].nunique()}")
    print("Latitude range:", df["latitude"].min(), "to", df["latitude"].max())
    print("Longitude range:", df["longitude"].min(), "to", df["longitude"].max())

# ---------------- VALIDATE ----------------
def validate_data(df):
    valid = []
    rejected = []
    seen_ids = set()

    for _, row in df.iterrows():
        errors = []

        if row["user_id"] in seen_ids:
            errors.append("Duplicate user_id")
        seen_ids.add(row["user_id"])

        if "@" not in str(row["email"]):
            errors.append("Invalid email")

        if pd.isna(row["city"]):
            errors.append("City is null")

        zipcode = str(row["zipcode"]).replace("-", "")
        if len(zipcode) < 5:
            errors.append("Invalid zipcode")

        if errors:
            r = row.to_dict()
            r["errors"] = ", ".join(errors)
            rejected.append(r)
        else:
            valid.append(row.to_dict())

    return pd.DataFrame(valid), pd.DataFrame(rejected)

# ---------------- CSV ----------------
def save_csv(valid_df, rejected_df):
    valid_df.to_csv("valid_users.csv", index=False)
    rejected_df.to_csv("rejected_users.csv", index=False)
    logging.info("CSV files saved")

# ---------------- LOAD ----------------
def load_to_sqlite(valid_df):
    conn = sqlite3.connect("users.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            name TEXT,
            username TEXT,
            email TEXT,
            phone TEXT,
            website TEXT,
            city TEXT,
            zipcode TEXT,
            latitude TEXT,
            longitude TEXT,
            company_name TEXT,
            company_phrase TEXT,
            company_bs TEXT
        )
    """)

    for _, row in valid_df.iterrows():
        cursor.execute(
            "INSERT OR REPLACE INTO users VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            tuple(row)
        )

    conn.commit()
    conn.close()
    logging.info("Data loaded into SQLite")

# ---------------- MAIN ----------------
def main():
    raw = extract_data()
    if not raw:
        print("No data extracted")
        return

    df = transform_data(raw)

    generate_insights(df)   # ✅ THIS WAS THE ONLY MISSING PIECE

    valid_df, rejected_df = validate_data(df)

    save_csv(valid_df, rejected_df)
    load_to_sqlite(valid_df)

    print("Pipeline executed successfully")

# ---------------- RUN ----------------
if __name__ == "__main__":
    main()