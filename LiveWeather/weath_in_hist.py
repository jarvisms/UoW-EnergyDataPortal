# Written by Alice Stamp
# Reads all xls files containing weather station data and uploads all data to the database

import glob, MySQLdb, xlrd, datetime
from xlrd.sheet import ctype_text
docs = glob.glob('*.xls')   #collects all files together
server = "SERVER"
SQLUser = "USERNAME"
SQLPassword = "PASSWORD"
conn = MySQLdb.connect(server, SQLUser, SQLPassword, "db_EnergyDataPortal")
cursor = conn.cursor()
for entry in docs:
    data = []
    i = 0
    file = xlrd.open_workbook(entry)
    sheet = file.sheet_by_index(0)  #selects first sheet
    for x in range(sheet.nrows):
        vals = sheet.row_values(x)
        if type(vals[0]) != float:  #if heading row found, ignore
            continue
        else:
            vals[0] = xlrd.xldate_as_tuple(vals[0], file.datemode)  #rename the date item into py datetime
            vals[0] = datetime.datetime(*vals[0]).strftime('%Y-%m-%d %H:%M:%S')
        if len(vals) == 10:
            del vals[1]        #dealing with various formats of data
        elif len(vals) == 11:  #discard unwanted data
            del vals[1: 3]
        elif len(vals) == 15:
            del vals[11:], vals[1:3]
        cursor.execute("INSERT INTO RTWeather VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", tuple(vals))
        conn.commit()
        del data
        data = []
        i = 0
conn.close()
