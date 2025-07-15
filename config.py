import os

SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
GMAIL_USER = 'ahmadalfajri173@gmail.com'
GMAIL_PASS = 'hubungi Ahmad'

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'tracer'
}

UPLOAD_FOLDER = 'assets'
ALLOWED_EXTENSIONS = {'pdf', 'docx', 'xlsx', 'jpg', 'jpeg', 'png'}

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Green-API WA Config
ID_INSTANCE = "hubungi Ahmad"
API_TOKEN = "hubungi Ahmad"
