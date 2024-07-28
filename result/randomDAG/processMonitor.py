import psutil
import smtplib
from email.mime.text import MIMEText
import time

def is_process_running(process_name):
    """Check if there is any running process that contains the given name."""

    for proc in psutil.process_iter(['name']):
        
        if process_name.lower() in proc.info['name'].lower():
            return True
    return False

def send_email(subject, body, to_email):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = 'adt107132@gm.ntcu.edu.tw'
    msg['To'] = to_email

    # SMTP server configuration
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login('adt107132@gm.ntcu.edu.tw', 'vmherdveplybmzzj')

    # Send the email
    server.send_message(msg)
    server.quit()

# Replace 'process_name' with the name of the process you want to monitor
process_name = 'go'
recipient_email = 't3742238@gmail.com'


while True:
    if not is_process_running(process_name):
        # send_email('Process Shutdown Alert', f'{process_name} has stopped running.', recipient_email)
        break
    time.sleep(60)  # Check every 60 seconds
