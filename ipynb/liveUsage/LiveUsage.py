import pandas as pd
import json
from sqlalchemy import create_engine
import getpass
from tqdm import tqdm

tqdm.pandas()


# =========================
# INPUT KONEKCIJE
# =========================
def get_connection_uri():
    user = input("User: ")
    password = getpass.getpass("Password: ")
    host = input("Host (default localhost): ") or "localhost"
    port = input("Port (default 3306): ") or "3306"
    database = input("Database: ")

    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"


# =========================
# CLEAN JSON
# =========================
def has_any_null(x):
    try:
        data = json.loads(x) if x else {}

        for k in ["title", "programId", "duration", "location"]:
            if k not in data:
                return True

            val = data[k]

            if isinstance(val, str):
                val = val.strip().lower()

            if val in [None, "", "null", "no title", "0", "-1"]:
                return True

        return False
    except:
        return True


def extract(x):
    try:
        d = json.loads(x) if x else {}
        return (d.get("programId"), d.get("title"), d.get("duration"))
    except:
        return (None, None, None)


# =========================
# SAVE SQL FILE
# =========================
def save_as_sql(df, table_name, file_name):
    print(f"➡️ Snimam {file_name}...")

    with open(file_name, "w", encoding="utf-8") as f:
        f.write(f"DROP TABLE IF EXISTS {table_name};\n")
        f.write(f"CREATE TABLE {table_name} (\n")

        cols = [f"{col} TEXT" for col in df.columns]
        f.write(",\n".join(cols))
        f.write("\n);\n\n")

        for _, row in df.iterrows():
            values = []
            for val in row:
                if pd.isna(val):
                    values.append("NULL")
                else:
                    val = str(val).replace("'", "''")
                    values.append(f"'{val}'")

            f.write(f"INSERT INTO {table_name} VALUES ({', '.join(values)});\n")

    print(f"✔ {file_name} gotov")


# =========================
# MAIN
# =========================
def main():

    print("=== START ===")

    db_uri = "mysql+pymysql://statsuser:statspass@localhost:3306/statsdb"

    print("➡️ Spajam se na bazu...")
    engine = create_engine(db_uri)

    query = """
    SELECT *
    FROM statistic
    WHERE type = 'LiveUsage'
    """

    print("➡️ Učitavam podatke iz baze...")
    df = pd.read_sql(query, engine)
    print(f"✔ Učitano {len(df)} redova")

    # =========================
    # CLEAN
    # =========================
    print("➡️ Čišćenje JSON-a...")
    mask = df["extra"].apply(has_any_null)

    df_clean = df[~mask].reset_index(drop=True)
    print(f"✔ Nakon JSON filtera: {len(df_clean)} redova")

    print("➡️ Filtriram duration <= 350000...")
    df_clean = df_clean[df_clean["duration"] <= 350000].reset_index(drop=True)
    print(f"✔ Nakon duration filtera: {len(df_clean)} redova")

    # =========================
    # WATCH TIME
    # =========================
    print("➡️ Računam watch time po kanalu...")
    df_watch_time = df_clean.groupby(["cusRef"], as_index=False).agg(
        total_watch_time=("duration", "sum")
    )

    print("✔ Watch time gotov")

    # =========================
    # 1. NAJGLEDANIJI KANAL
    # =========================
    print("➡️ Računam najgledaniji kanal...")
    top_name = (
        df_watch_time.groupby("name")["devRef"]
        .nunique()
        .reset_index(name="device_count")
        .sort_values(by="device_count", ascending=False)
    )
    print("✔ Top name gotov")

    # =========================
    # 2. AVG WATCH TIME
    # =========================
    print("➡️ Računam prosječnu gledanost...")
    avg_watch = (
        df_watch_time.groupby("name")["total_watch_time"]
        .mean()
        .reset_index(name="avg_watch_time")
        .sort_values(by="avg_watch_time", ascending=False)
    )
    print("✔ Avg watch gotov")

    # =========================
    # 3. TOP TITLE (>=50%)
    # =========================
    print("➡️ Parsiram JSON za title...")
    df_tmp = df_clean[["devRef", "name", "extra", "duration"]].copy()

    df_tmp[["program_id", "title", "program_duration"]] = pd.DataFrame(
        df_tmp["extra"].apply(extract).tolist(), index=df_tmp.index
    )

    print("✔ Parsing gotov")

    df_tmp = df_tmp.dropna(subset=["program_id", "title", "program_duration"])

    print("➡️ Računam retention (50%)...")
    df_user = df_tmp.groupby(
        ["devRef", "program_id", "name", "title", "program_duration"],
        as_index=False,
    ).agg(user_watch_time=("duration", "sum"))

    df_user = df_user[df_user["user_watch_time"] >= 0.5 * df_user["program_duration"]]

    print("➡️ Računam top titles...")
    top_titles = (
        df_user.groupby(["program_id", "name", "title"])["devRef"]
        .count()
        .reset_index(name="viewer_count")
        .sort_values(by="viewer_count", ascending=False)
    )

    print("✔ Top titles gotov")

    # =========================
    # SAVE FILES
    # =========================
    save_as_sql(top_name, "top_name", "top_name.sql")
    save_as_sql(avg_watch, "avg_watch", "avg_watch.sql")
    save_as_sql(top_titles, "top_titles", "top_titles.sql")

    print("\n=== GOTOVO ===")


# =========================
# ENTRY POINT
# =========================
if __name__ == "__main__":
    main()
