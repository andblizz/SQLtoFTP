import psycopg2
import time
import ftplib
from loguru import logger
import os

# SQL query
sql = "COPY (SELECT * FROM book) TO STDOUT WITH CSV DELIMITER ';'"
# Log file creation.
logger.add('debug.log', colorize=True, format='{time} {level} {message}', level='DEBUG')
# An endless loop is used if the script is always running.
# If you are running the script on a schedule, then just remove the "while True:" and "time.sleep()".
while True:
    start_time = time.time()
    # Database connection and file formation.
    try:
        con = psycopg2.connect(
            database="testdb",
            user="postgres",
            password=os.environ['db_password'],
            host="127.0.0.1",
            port="5432"
        )
        print("Database opened successfully")
        cur = con.cursor()
    except ConnectionError:
        logger.error('An error occurred: ConnectionError')
    except psycopg2.OperationalError:
        logger.error('An error occurred: OperationalError')
    try:
        with open("testdb.csv", "w") as file:
            cur.copy_expert(sql, file)
            print('File updated in %s seconds' % (time.time() - start_time))
    except PermissionError:
        logger.error('An error occurred: PermissionError')
    con.close()
    # Uploading a file to FTP.
    host = os.environ['ftp_host']
    ftp_user = os.environ['ftp_user']
    ftp_password = os.environ['ftp_password']
    print('Attempting to connect to an FTP server', host)
    ftp = ftplib.FTP(host, ftp_user, ftp_password)
    # Checking the current state of folders. We put in the variable list the list of directory contents.
    directory_list = ftp.nlst()
    print(directory_list)
    # Open file to send.
    with open("testdb.csv", 'rb') as file:
        # Send the file
        ftp.storbinary('STOR testdb3.csv', file)
        print('Файл', file, 'успешно загружен')
    # Adjust time to update the file
    time.sleep(15)
