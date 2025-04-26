import os
import aiosmtplib
from email.message import EmailMessage
from datetime import datetime
from dotenv import load_dotenv
from model.transaction import Transaction
from model.user import User

# Load environment variables
load_dotenv()

# SMTP settings from environment
SMTP_SERVER = os.environ.get("SMTP_SERVER")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))  # default 587 if not set
SMTP_USERNAME = os.environ.get("SMTP_USERNAME")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD")

async def send_email_alert(subject: str, body: str, to_email: str):
    msg = EmailMessage()
    msg["From"] = SMTP_USERNAME
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        await aiosmtplib.send(
            msg,
            hostname=SMTP_SERVER,
            port=SMTP_PORT,
            start_tls=True,
            username=SMTP_USERNAME,
            password=SMTP_PASSWORD,
        )
        print("✅ Email alert sent successfully!")
    except Exception as e:
        print(f"❌ Error sending email: {e}")
        

def prepare_transaction_email(user: User, new_transaction: Transaction):
    
        subject = "Bank Alert: Transaction Notification"
        body = f"""   Dear {user["firstname"]},

        A transaction has been made on your account
        
        
        Transaction Type: {new_transaction['type']}
        Amount: ${new_transaction['amount']:.2f}
        Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
       
        If this was not you, please contact our support team immediately.

        Thank you,
        Netbank Plc
        """
        return subject,  body