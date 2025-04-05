import aiosmtplib
from email.message import EmailMessage
from datetime import datetime

from model.transaction import Transaction
from model.user import User

# ✅ Correct Outlook SMTP settings
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USERNAME = "matovie22@gmail.com"
SMTP_PASSWORD = "egbveryflhexedon"  # Use App Password if needed

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