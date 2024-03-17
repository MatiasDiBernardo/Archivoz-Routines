from googleapiclient.discovery import build
from google.oauth2 import service_account

SCOPE = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = 'archivoz-backup-de3a274eace7.json'

def authenticate():
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPE)
    return creds

def upload_file(file_path, name, drive_folder):
    """Uploads a file to the google drive.

    Args:
        file_path (str): Path to the file to upload.
        name (str): Name to save the file.
        drive_folder(str): Drive folder ID where the file will be uploades.
    """
    creds = authenticate()
    service = build('drive', 'v3', credentials=creds)

    file_metadata = {
        "name" : name, 
        "parents": [drive_folder]
    }

    file = service.files().create(
        body=file_metadata,
        media_body=file_path
    ).execute()