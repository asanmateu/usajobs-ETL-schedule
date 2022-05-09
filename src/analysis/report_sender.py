from constants import RECIPIENT_EMAIL, SENDER_EMAIL, SENDER_PASSWORD, SMTP_SERVER, SMTP_PORT
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import date
import os
import sys
import smtplib
import ssl


def send_reports(reports_path: str):
    """
    Loops through present CSV files in reports_path,
    and sends them via email to recipient.

    Returns None
    """
    # Set up email parameters
    msg = MIMEMultipart()
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECIPIENT_EMAIL
    msg["Subject"] = "Antonio - Data Analysis Reports {}".format(date.today())
    msg.attach(MIMEText("Please find attached reports for today's analysis."))

    context = ssl.create_default_context()

    # Loop through CSV files in reports_path, attach to email
    try:
        for file in os.listdir(reports_path):
            with open(os.path.join(reports_path, file), "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    "attachment; filename={}".format(file),
                )
                msg.attach(part)
    except FileNotFoundError:
        print("No reports found in {}".format(reports_path))
        sys.exit(1)

    try:
        # Send email using SMTP
        smtp_server = smtplib.SMTP(SMTP_SERVER, int(SMTP_PORT))
        smtp_server.ehlo()
        # Start TLS for security
        smtp_server.starttls(context=context)
        # Identify ourselves to smtp gmail client
        smtp_server.ehlo()
        # Identify to server this time with encrypted connection
        smtp_server.login(SENDER_EMAIL, SENDER_PASSWORD)
        # Send email
        smtp_server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, msg.as_string())
        # Quit server
        smtp_server.quit()
    except Exception as e:
        print(e)
        sys.exit(1)
