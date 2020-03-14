# This code takes historic averaged data worked out by had in a spreadsheet and adds it to the database

import glob, MySQLdb, xlrd, datetime
from xlrd.sheet import ctype_text
allday = []
file = xlrd.open_workbook('SPREADSHEET.xls')
sheet = file.sheet_by_index(0)  #selects first sheet
server = "SERVER"
SQLUser = "USERNAME"
SQLPassword = "PASSWORD"
conn = MySQLdb.connect(server, SQLUser, SQLPassword, "db_EnergyDataPortal")
cursor = conn.cursor()
for x in range(sheet.nrows):
    vals = sheet.row_values(x)
    dt = vals[1]
    dt = datetime.datetime.strptime(dt, '%a %b %d %H:%M:%S %Z %Y')
    dt = dt - datetime.timedelta(days=6)
    dt = dt.replace(minute=0, hour=0, second=0)
    if len(vals[0]) <= 1:
        continue
    else:
        place = vals[0]
        loc = ', '.join(place.split(' > '))
        for i in range(672):
            newdate = dt + i*datetime.timedelta(minutes=15)
            allday.append( tuple([newdate.strftime('%Y-%m-%d %H:%M:%S'), loc, int(vals[2])]) )
        cursor.executemany("INSERT INTO wifi2 VALUES (%s, %s, %s)", allday)
        conn.commit()
        del allday
        allday = []
conn.close()
#this is the edited second version, used to put in weekly average data
