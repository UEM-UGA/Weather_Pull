import requests
import pyodbc
import time
from datetime import datetime, timedelta, date

# ==========================================================
# CONFIGURATION
# ==========================================================

SERVER = "fmd-bsql3.msmyid.uga.edu"
DATABASE = "UEM_Dev"

CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
)

BRONZE_TABLE = "weather.bronze_weather_hourly_raw"
SILVER_TABLE = "weather.silver_weather_hourly"

LAT = 33.9519
LON = -83.3576

HOURLY_FIELDS = [
    "temperature_2m",
    "precipitation",
    "windspeed_10m",
    "relativehumidity_2m",
    "shortwave_radiation",
    "direct_radiation",
    "diffuse_radiation"
]

TOKEN = "8456900464:AAEn1Qnhz2ONsVb97OmgSh-uP7DP5_WsZGo"
CHAT_ID = "6324316572"

SUMMARY_PATH = r"C:\UGA_Weather\last_run_summary.txt"
RETRY_DELAY_SECONDS = 600  # 10 minutes

# ==========================================================
# TELEGRAM ALERT
# ==========================================================

def send_telegram_message(message_text):
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        requests.post(url, json={
            "chat_id": CHAT_ID,
            "text": message_text
        }, timeout=20)
    except Exception as e:
        print("Telegram alert failed:", str(e))

# ==========================================================
# SUMMARY WRITER
# ==========================================================

def write_summary(status, attempt, target_date,
                  bronze_inserted, bronze_skipped,
                  silver_inserted, silver_skipped,
                  duration_seconds, error_msg,
                  last_day_in_db, dates_filled):

    with open(SUMMARY_PATH, "w", encoding="utf-8") as f:
        f.write(f"status={status}\n")
        f.write(f"attempt={attempt}\n")
        f.write(f"run_time={datetime.now()}\n")
        f.write(f"target_date={target_date}\n")
        f.write(f"last_day_in_db={last_day_in_db}\n")
        f.write(f"dates_filled={dates_filled}\n")
        f.write(f"bronze_inserted={bronze_inserted}\n")
        f.write(f"bronze_skipped={bronze_skipped}\n")
        f.write(f"silver_inserted={silver_inserted}\n")
        f.write(f"silver_skipped={silver_skipped}\n")
        f.write(f"duration_seconds={int(duration_seconds)}\n")
        f.write(f"error={error_msg}\n")

# ==========================================================
# BUILD TELEGRAM MESSAGE FROM SUMMARY
# ==========================================================

def send_telegram_from_summary():
    try:
        with open(SUMMARY_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()

        summary = {}
        for line in lines:
            if "=" in line:
                k, v = line.strip().split("=", 1)
                summary[k] = v

        message = []
        message.append(f"Weather ETL: {summary.get('status')}")
        message.append(f"Attempt: {summary.get('attempt')}")
        message.append(f"Run Time: {summary.get('run_time')[:19]}")
        message.append(f"Last Day in DB: {summary.get('last_day_in_db')}")
        message.append(f"Dates Filled: {summary.get('dates_filled')}")
        message.append(f"Bronze Inserted: {summary.get('bronze_inserted')}")
        message.append(f"Bronze Skipped: {summary.get('bronze_skipped')}")
        message.append(f"Silver Inserted: {summary.get('silver_inserted')}")
        message.append(f"Duration (sec): {summary.get('duration_seconds')}")

        if summary.get("status") != "SUCCESS":
            message.append("Error:")
            message.append(summary.get("error")[:3500])

        send_telegram_message("\n".join(message))

    except Exception as e:
        print("Failed building telegram message:", str(e))

# ==========================================================
# FETCH DATA
# ==========================================================

def fetch_weather_data(start_date, end_date):
    print(f"Fetching data from Open-Meteo Archive API ({start_date} to {end_date})...")

    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": LAT,
        "longitude": LON,
        "hourly": ",".join(HOURLY_FIELDS),
        "start_date": start_date,
        "end_date": end_date,
        "timezone": "UTC"
    }

    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()

    print("API fetch successful.")
    return response.json()

# ==========================================================
# CORE ETL LOGIC
# ==========================================================

def run_etl(attempt_number):
    overall_start = time.time()

    print("--------------------------------------------------")
    print("Weather ETL STARTED")
    print("Attempt:", attempt_number)

    print("Connecting to DB...")
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    print(f"CONNECTED → {SERVER} | {DATABASE}")

    # --- Find the Absolute Last Day in the Database ---
    cursor.execute(f"SELECT MAX(CAST(weather_datetime AS DATE)) FROM {SILVER_TABLE}")
    max_date_row = cursor.fetchone()
    last_day_in_db = max_date_row[0].strftime("%Y-%m-%d") if max_date_row and max_date_row[0] else "None"
    print(f"Last recorded date in Silver table: {last_day_in_db}")

    # --- Identify Missing Dates in the Last 10 Days ---
    today = date.today()
    expected_dates = set((today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, 11))
    
    start_lookback = min(expected_dates)
    end_lookback = max(expected_dates)

    cursor.execute(f"""
        SELECT DISTINCT CAST(weather_datetime AS DATE) 
        FROM {SILVER_TABLE}
        WHERE CAST(weather_datetime AS DATE) >= ?
          AND CAST(weather_datetime AS DATE) <= ?
    """, start_lookback, end_lookback)
    
    existing_dates = set(row[0].strftime("%Y-%m-%d") for row in cursor.fetchall())
    
    missing_dates = sorted(list(expected_dates - existing_dates))

    if not missing_dates:
        print("Data for the last 10 days is already fully populated. No action needed.")
        conn.close()
        write_summary(
            status="✅SUCCESS", 
            attempt=attempt_number, 
            target_date="N/A", 
            bronze_inserted=0, 
            bronze_skipped=0, 
            silver_inserted=0, 
            silver_skipped=0, 
            duration_seconds=time.time() - overall_start, 
            error_msg="",
            last_day_in_db=last_day_in_db,
            dates_filled="None"
        )
        send_telegram_from_summary()
        return

    # Define the fetch window
    fetch_start_date = missing_dates[0]
    fetch_end_date = missing_dates[-1]
    filled_dates_str = ", ".join(missing_dates)
    
    print(f"Missing dates detected: {missing_dates}")
    print(f"API Fetch Window set to: {fetch_start_date} to {fetch_end_date}")

    # Fetch the data
    data = fetch_weather_data(fetch_start_date, fetch_end_date)

    hourly = data["hourly"]
    timestamps = hourly["time"]

    bronze_inserted = 0
    bronze_skipped = 0

    print("Inserting into Bronze...")

    for i in range(len(timestamps)):
        weather_dt = datetime.fromisoformat(timestamps[i])

        try:
            cursor.execute(f"""
                INSERT INTO {BRONZE_TABLE}
                (weather_datetime, temperature, precipitation,
                 wind_speed, humidity,
                 shortwave_radiation, direct_radiation,
                 diffuse_radiation, source_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                weather_dt,
                hourly["temperature_2m"][i],
                hourly["precipitation"][i],
                hourly["windspeed_10m"][i],
                hourly["relativehumidity_2m"][i],
                hourly["shortwave_radiation"][i],
                hourly["direct_radiation"][i],
                hourly["diffuse_radiation"][i],
                "daily_fetch"
            )

            bronze_inserted += 1

        except pyodbc.IntegrityError:
            bronze_skipped += 1

    conn.commit()
    print(f"Bronze Complete → Inserted: {bronze_inserted}, Skipped: {bronze_skipped}")

    print("Promoting to Silver...")

    cursor.execute(f"""
        INSERT INTO {SILVER_TABLE}
        (weather_datetime, temperature, precipitation,
         wind_speed, humidity,
         shortwave_radiation, direct_radiation,
         diffuse_radiation, ingestion_time)
        SELECT
            b.weather_datetime,
            b.temperature,
            b.precipitation,
            b.wind_speed,
            b.humidity,
            b.shortwave_radiation,
            b.direct_radiation,
            b.diffuse_radiation,
            GETDATE()
        FROM {BRONZE_TABLE} b
        WHERE CAST(b.weather_datetime AS DATE) >= ?
          AND CAST(b.weather_datetime AS DATE) <= ?
        AND NOT EXISTS (
            SELECT 1 FROM {SILVER_TABLE} s
            WHERE s.weather_datetime = b.weather_datetime
        )
    """, fetch_start_date, fetch_end_date)

    silver_inserted = cursor.rowcount if cursor.rowcount != -1 else 0
    silver_skipped = 0

    conn.commit()
    conn.close()

    duration = time.time() - overall_start

    print(f"Silver Complete → Inserted: {silver_inserted}")
    print("✅ETL SUCCESS")
    print("--------------------------------------------------")

    write_summary(
        status="✅SUCCESS",
        attempt=attempt_number,
        target_date=f"{fetch_start_date} to {fetch_end_date}",
        bronze_inserted=bronze_inserted,
        bronze_skipped=bronze_skipped,
        silver_inserted=silver_inserted,
        silver_skipped=silver_skipped,
        duration_seconds=duration,
        error_msg="",
        last_day_in_db=last_day_in_db,
        dates_filled=filled_dates_str
    )

    send_telegram_from_summary()

# ==========================================================
# MAIN WITH RETRY
# ==========================================================

def main():
    try:
        run_etl(attempt_number=1)
    except Exception as e:
        print("❌ETL FAILED:", str(e))
        write_summary("❌FAILED", 1, "", 0, 0, 0, 0, 0, str(e), "Unknown", "None")
        send_telegram_from_summary()

        print(f"Retrying in {RETRY_DELAY_SECONDS / 60} minutes...")
        time.sleep(RETRY_DELAY_SECONDS)

        try:
            run_etl(attempt_number=2)
        except Exception as e2:
            write_summary("❌FAILED", 2, "", 0, 0, 0, 0, 0, str(e2), "Unknown", "None")
            send_telegram_from_summary()

if __name__ == "__main__":
    main()