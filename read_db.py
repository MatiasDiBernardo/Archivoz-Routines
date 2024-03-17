import sqlite3
import pandas as pd
from datetime import date
import os
import zipfile

from upload_drive import upload_file

def process_data(row_users, row_recs):
    """
    Create dictionary with the most important data from the
    Usuarios and Grabacions fields on the db.
    """
    data = {
        "ID": [], 
        "Fecha":[],
        "Nombre": [], 
        "Region": [], 
        "Edad": [],
        "Mail": [], 
        "Audios Donados" : [],
        "Minutos Donados": [], 
        "Peticion Custom TTS": [], 
        "Custom TTS Usos": [], 
    }
    
    for info in row_users:
        user_id = info[0]
        data["ID"].append(user_id)
        data["Nombre"].append(info[1])
        data["Edad"].append(info[2])
        data["Region"].append(info[3])
        data["Mail"].append(info[4])
        data["Peticion Custom TTS"].append(0)  # Agregar a la db en users con Boolean, seria info[5]
        data["Custom TTS Usos"].append(0)  # Mismo que arriba
    
        cantidad_audios = 0
        minutos_audio = 0
        date_last_update = date.today()  # To cover case when the user doesn't record anything
        for rec in row_recs:
            if user_id == rec[2]:
                cantidad_audios += 1
                minutos_audio += 2  # Acá sería rec[6] donde agrego la duración de la grabación
                date_last_update = rec[1].split(" ")[0]

        data["Fecha"].append(date_last_update)
        data["Audios Donados"].append(cantidad_audios)
        data["Minutos Donados"].append(minutos_audio)

    return data

def get_data_from_db():
    """
    Gets the information from the .db file into python list.
    """
    # Connect to the database
    conn = sqlite3.connect('instance/data_base.db')
    
    # Create a cursor object
    cursor_user = conn.cursor()
    cursor_rec = conn.cursor()
    
    # Execute SQL query to fetch all fields from the "Usuarios" table
    cursor_user.execute("SELECT * FROM Usuario")
    cursor_rec.execute("SELECT * FROM Grabacion")
    
    # Fetch all rows
    users_data = cursor_user.fetchall()
    rec_data = cursor_rec.fetchall()
    
    display_data = False
    if display_data:
        # Display the field names
        fields_user = [desc[0] for desc in cursor_user.description]
        fields_rec = [desc[0] for desc in cursor_rec.description]
        print(fields_rec)
        print(fields_user)
    
        # Display the data
        for row in users_data:
            print(row)

    # Close the connection
    conn.close()
    
    return users_data, rec_data

def only_new_data(df_old, df_new):
    """ This function compares the old data frame (last week) 
    with the current one and creats a new dict with the progress
    of the week.
    Args:
        df_old(pd.DataFrame): Old data.
        df_new(pd.DataFrame): New data.
    Returns
        (pd.DataFrame): Information of progress.
    """
    df_old = df_old.to_dict()
    df_new = df_new.to_dict()

    users_new = list(df_new["ID"].values()) 
    users_old = list(df_old["ID"].values()) 
    new_users = len(users_new) - len(users_old)

    min_new = list(df_new["Minutos Donados"].values()) 
    min_old = list(df_old["Minutos Donados"].values()) 
    new_min = sum(min_new) - sum(min_old)

    audios_new = list(df_new["Audios Donados"].values()) 
    audios_old = list(df_old["Audios Donados"].values()) 
    new_audios = sum(audios_new) - sum(audios_old)

    tts_new = list(df_new["Peticion Custom TTS"].values()) 
    tts_old = list(df_old["Peticion Custom TTS"].values()) 
    new_tts_req = sum(tts_new) - sum(tts_old)

    uses_new = list(df_new["Custom TTS Usos"].values()) 
    uses_old = list(df_old["Custom TTS Usos"].values()) 
    new_tts_uses = sum(uses_new) - sum(uses_old)

    dicc = {"Fecha"           : [date.today()], 
            "Usuarios Nuevos" : [new_users], 
            "Audios Donados"  : [new_audios],
            "Minutos Donados" : [new_min], 
            "Peticion Custom TTS" : [new_tts_req], 
            "Usos Custom TTS" : [new_tts_uses]}

    return dicc

# Get old data
excel_file_path = 'Archivoz data general.xlsx'
df_old = pd.read_excel(excel_file_path)

# Get the current data
users_data, rec_data = get_data_from_db()
data_new = process_data(users_data, rec_data)
df_new = pd.DataFrame(data_new)

# Calculate stats
data_of_the_week = only_new_data(df_old, df_new)

# Updates the excel with the new data
df_new.to_excel(excel_file_path, index=False)

# Save the progress of the week in excel
excel_stats_path = 'Archivoz stats.xlsx'
df_stats_old = pd.read_excel(excel_stats_path)
df_stats = pd.concat([df_stats_old, pd.DataFrame(data_of_the_week)], ignore_index=True)
df_stats.to_excel(excel_stats_path, index=False)

# Make db zip with the current date
bck_name = f"Backup DB {date.today()}.zip"
zip_filename = os.path.join("db_backups", bck_name)

with zipfile.ZipFile(zip_filename, 'w') as zip_file:
    zip_file.write(os.path.join("instance", "data_base.db"))

# Upload files to drive
upload_file(excel_file_path, excel_file_path, os.environ.get('FOLDER_STATS'))
upload_file(excel_stats_path, excel_stats_path, os.environ.get('FOLDER_STATS'))

upload_file(zip_filename, zip_filename, os.environ.get('FOLDER_BACKUP'))

