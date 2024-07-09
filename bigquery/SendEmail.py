import psycopg2
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5
import os


# PostgreSQLReader Class
class PostgreSQLReader:
    def __init__(self, host, dbname, user, private_key_path, encrypted_password_path):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.private_key_path = private_key_path
        self.encrypted_password_path = encrypted_password_path
        self.connection = None

    def decrypt_password(self):
        with open(self.private_key_path, 'rb') as key_file:
            private_key = RSA.import_key(key_file.read())
        with open(self.encrypted_password_path, 'rb') as enc_file:
            encrypted_password = enc_file.read()
        cipher = PKCS1_v1_5.new(private_key)
        decrypted_password = cipher.decrypt(encrypted_password, None)
        return decrypted_password.decode('utf-8')

    def connect(self):
        password = self.decrypt_password()
        print(f"Decrypted password: {password}")  # Debugging line to ensure password is correctly decrypted
        self.connection = psycopg2.connect(
            host=self.host,
            dbname=self.dbname,
            user=self.user,
            password=password,
            sslmode='disable',
            options="-c gssencmode=disable"
        )

    def fetch_data(self, query):
        if self.connection is None:
            self.connect()
        df = pd.read_sql_query(query, self.connection)
        return df

    def close(self):
        if self.connection:
            self.connection.close()


# Email Sending Function
def send_email(subject, body, to_email, from_email, smtp_server, smtp_port, smtp_user, smtp_password, attachment_path):
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))

    # Attach the Excel file
    with open(attachment_path, 'rb') as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(attachment_path)}')
        msg.attach(part)

    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(smtp_user, smtp_password)
    server.sendmail(from_email, to_email, msg.as_string())
    server.quit()


# Main Function
def main():
    # PostgreSQL connection details
    pg_details = {
        "host": "pg_host.manoj.com",
        "dbname": "manojdb",
        "user": "user_manoj",
        "private_key_path": "/path/to/private_key.pem",
        "encrypted_password_path": "/path/to/encrypted_password.bin"
    }

    queries = [
        "SELECT col1, col2, col3, col4 FROM table1",
        "SELECT col1, col2, col3, col4 FROM table2",
        "SELECT col1, col2, col3, col4 FROM table3",
        "SELECT col1, col2, col3, col4 FROM table4"
    ]

    pg_reader = PostgreSQLReader(**pg_details)

    html_tables = ""
    excel_file_path = "query_results.xlsx"

    with pd.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
        for i, query in enumerate(queries):
            df = pg_reader.fetch_data(query)
            html_tables += f"<h2>Table {i + 1}</h2>"
            html_tables += df.to_html(index=False, border=0)
            df.to_excel(writer, sheet_name=f'Table {i + 1}', index=False)

    pg_reader.close()

    # Add CSS styling
    html_content = f"""
    <html>
    <head>
        <style>
            table {{
                width: 100%;
                border-collapse: collapse;
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
            }}
            th {{
                background-color: #f2f2f2;
            }}
            tr:nth-child(even) {{
                background-color: #f9f9f9;
            }}
            tr:hover {{
                background-color: #ddd;
            }}
        </style>
    </head>
    <body>
        {html_tables}
    </body>
    </html>
    """

    # Email details
    subject = "PostgreSQL Query Results"
    to_email = "recipient@example.com"
    from_email = "your_email@example.com"
    smtp_server = "smtp.example.com"
    smtp_port = 587
    smtp_user = "your_email@example.com"
    smtp_password = "your_email_password"

    # Send the email
    send_email(subject, html_content, to_email, from_email, smtp_server, smtp_port, smtp_user, smtp_password,
               excel_file_path)


if __name__ == "__main__":
    main()
