import pandas as pd
import os
import pickle
import hashlib
import schedule
import time
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText

##############################
# CONFIGURATION
##############################

EXCEL_PATH = r"path\to\your_issuance_file.xlsx"
CHANGES_LOG_PATH = "changes_log.txt"
LAST_DATA_PICKLE = "last_data.pkl"
LAST_HASH_FILE = "last_hash.txt"

# Email settings
SMTP_SERVER = "smtp.yourdomain.com"
SMTP_PORT = 587
EMAIL_ADDRESS = "your_email@yourdomain.com"
EMAIL_PASSWORD = "your_email_password"

# Who gets the overdue/failure emails
ADMIN_EMAILS = ["admin1@yourdomain.com", "admin2@yourdomain.com"]

##############################
# HELPER FUNCTIONS
##############################

def compute_file_hash(file_path):
    """
    Compute SHA256 hash of the given file to detect tampering or changes.
    """
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def send_email(subject, body, to_addrs):
    """
    Sends an email with the specified subject and body to the given recipients.
    """
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_ADDRESS
    msg["To"] = ", ".join(to_addrs)

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, to_addrs, msg.as_string())
    except Exception as e:
        print(f"Error sending email: {e}")


def log_change(change_message):
    """
    Appends a change message to the changes_log.txt file with a timestamp.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(CHANGES_LOG_PATH, "a") as log_file:
        log_file.write(f"[{timestamp}] {change_message}\n")


def load_previous_data():
    """
    Loads previously stored DataFrame from a pickle file (if exists).
    Returns None if not found.
    """
    if os.path.exists(LAST_DATA_PICKLE):
        with open(LAST_DATA_PICKLE, "rb") as f:
            return pickle.load(f)
    return None


def save_current_data(df):
    """
    Saves current DataFrame to a pickle file for next comparison.
    """
    with open(LAST_DATA_PICKLE, "wb") as f:
        pickle.dump(df, f)


def load_previous_hash():
    """
    Loads the last known file hash (if exists).
    """
    if os.path.exists(LAST_HASH_FILE):
        with open(LAST_HASH_FILE, "r") as f:
            return f.read().strip()
    return None


def save_current_hash(h):
    """
    Stores the current file hash.
    """
    with open(LAST_HASH_FILE, "w") as f:
        f.write(h)


##############################
# CORE LOGIC
##############################

def check_excel():
    """
    1) Checks if the Excel file hash changed (possible tampering).
    2) If changed, compare row-by-row to log modifications.
    3) Send warning emails for items due tomorrow.
    4) Send 'failure' emails for items not returned after due date.
    """
    # ---- A) Check file hash for tampering or changes ----
    current_hash = compute_file_hash(EXCEL_PATH)
    previous_hash = load_previous_hash()

    if previous_hash and current_hash != previous_hash:
        # Possible file tampering or simply a legitimate update
        log_change("File hash changed - possible tampering or new entries.")
        print("WARNING: Excel file hash has changed. Checking row-by-row differences...")

    # ---- B) Read current Excel data ----
    df_current = pd.read_excel(EXCEL_PATH)
    
    # For consistency, ensure we work with predictable columns and types
    # Example: rename columns if needed, or confirm they are as expected:
    # df_current.columns = [
    #     "Name", "Roll Number", "RFID Tag", 
    #     "Date of Issuing", "Date of Return", "Email", "Returned"
    # ]
    
    # Convert date columns to datetime just in case
    df_current["Date of Issuing"] = pd.to_datetime(df_current["Date of Issuing"], errors="coerce")
    df_current["Date of Return"] = pd.to_datetime(df_current["Date of Return"], errors="coerce")
    
    # ---- C) Compare row-by-row to log changes ----
    df_previous = load_previous_data()

    if df_previous is not None:
        # Compare shapes first
        if df_current.shape != df_previous.shape:
            log_change(f"Row/column count changed from {df_previous.shape} to {df_current.shape}.")

        # We'll compare each cell for differences
        #  - We assume the same indexing/ordering in the sheet.  
        #  - For more robust comparisons, you might match by unique ID (e.g., Roll Number + RFID).
        min_rows = min(len(df_current), len(df_previous))
        min_cols = min(len(df_current.columns), len(df_previous.columns))

        for row in range(min_rows):
            for col in range(min_cols):
                val_current = df_current.iat[row, col]
                val_previous = df_previous.iat[row, col]
                if val_current != val_previous:
                    log_change(f"Row {row}, Col {col} changed from '{val_previous}' to '{val_current}'.")
        
        # If new rows were added or old rows removed, log them
        if len(df_current) > len(df_previous):
            for r in range(len(df_previous), len(df_current)):
                log_change(f"New row added at index {r}: {df_current.iloc[r].to_dict()}")
        elif len(df_current) < len(df_previous):
            for r in range(len(df_current), len(df_previous)):
                log_change(f"Row removed at old index {r}: {df_previous.iloc[r].to_dict()}")
    else:
        # No previous data => first run
        log_change("No previous data found. Initializing data storage.")

    # ---- D) Save current data & hash for next run ----
    save_current_data(df_current)
    save_current_hash(current_hash)

    # ---- E) Check Dates for Warnings & Failures ----
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)

    # We'll iterate through each row
    for idx, row in df_current.iterrows():
        # Make sure we have a valid date
        return_date = row["Date of Return"]
        returned_flag = str(row["Returned"]).strip().lower()
        roll_number = row["Roll Number"]
        email = row["Email"]

        if pd.isnull(return_date):
            # If there's no return date, skip
            continue

        return_date_only = return_date.date()

        # 1) If the return date is *tomorrow* => send a warning
        if return_date_only == tomorrow and returned_flag != "yes":
            subject = "Return Due Tomorrow"
            body = (
                f"Hello {row['Name']} (Roll No: {roll_number}),\n\n"
                "This is a reminder that your item is due tomorrow. "
                "Please ensure you return it on time.\n\n"
                "Regards,\nIssuance System"
            )
            send_email(subject, body, [email])

        # 2) If the return date is *before today* => item is overdue
        if return_date_only < today and returned_flag != "yes":
            subject = "Failure of Returning"
            body = (
                f"Roll Number {roll_number} has failed to return the item.\n\n"
                "They are now eligible for a no-due fine.\n\n"
                "Regards,\nIssuance System"
            )
            # Send to Admin(s) or a group of people
            send_email(subject, body, ADMIN_EMAILS)


##############################
# SCHEDULING & MAIN
##############################

def main():
    # Run immediately once
    check_excel()

    # Schedule to run every 1 hour
    schedule.every(1).hours.do(check_excel)

    while True:
        schedule.run_pending()
        time.sleep(60)  # Check once per minute if it's time to run

if __name__ == "__main__":
    main()
