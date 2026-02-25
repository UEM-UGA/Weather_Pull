# ==========================================================
# TELEGRAM ALERT (FULL CONTEXT VERSION)
# ==========================================================
def send_telegram_alert_from_summary():

    try:
        with open(SUMMARY_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()

        summary = {}
        for line in lines:
            if "=" in line:
                k, v = line.strip().split("=", 1)
                summary[k] = v

        status = summary.get("status", "UNKNOWN")
        run_time = summary.get("run_time", "")
        target_date = summary.get("target_date", "")
        bronze_ins = summary.get("bronze_inserted", "0")
        bronze_skp = summary.get("bronze_skipped", "0")
        silver_ins = summary.get("silver_inserted", "0")
        silver_skp = summary.get("silver_skipped", "0")
        duration = summary.get("duration_seconds", "")
        error = summary.get("error", "")

        message = []
        message.append(f"Weather ETL: {status}")
        message.append(f"Run Time: {run_time}")
        message.append(f"Target Date: {target_date}")
        message.append(f"Bronze → Inserted: {bronze_ins}")
        message.append(f"Bronze → Skipped: {bronze_skp}")
        message.append(f"Silver → Inserted: {silver_ins}")
        message.append(f"Silver → Skipped: {silver_skp}")
        message.append(f"Duration (sec): {duration}")

        if status != "SUCCESS":
            message.append("Error:")
            message.append(error[:3500])

        text = "\n".join(message)

        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": CHAT_ID, "text": text}, timeout=20)

    except Exception as e:
        print("Telegram alert failed:", str(e))