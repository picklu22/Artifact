import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Read environment variables (GitHub Secrets)
EMAIL_ADDRESS = os.environ.get("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD")
TO_EMAIL = os.environ.get("TO_EMAIL")

# File to attach
FILE_PATH = "report.csv"   # Make sure this file exists

# Create email
msg = MIMEMultipart()
msg["From"] = EMAIL_ADDRESS
msg["To"] = TO_EMAIL
msg["Subject"] = "GitHub Action CSV Report"

body = "Hello,\n\nPlease find attached the CSV report.\n\nThanks."
msg.attach(MIMEText(body, "plain"))

# Attach CSV file
with open(FILE_PATH, "rb") as attachment:
    part = MIMEBase("application", "octet-stream")
    part.set_payload(attachment.read())

encoders.encode_base64(part)

part.add_header(
    "Content-Disposition",
    f"attachment; filename={os.path.basename(FILE_PATH)}",
)

msg.attach(part)

# Send email
try:
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
    server.send_message(msg)
    server.quit()
    print("Email with attachment sent successfully!")
except Exception as e:
    print("Error sending email:", e)
    raise
