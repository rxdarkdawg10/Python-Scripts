import dataextract as tde
import pyodbc as td
import getpass, os, datetime, csv, time
from collections import OrderedDict

# Get Teradata User Information
uid = raw_input("Enter UID for Teradata: ")
pwd = getpass.getpass()

# Start Timer
startTime = time.clock()

# Set Teradata Connection
print "Connecting to Teradata..."
conn = td.connect('DRIVER={TERADATA};DBCNAME=<DBCNAME>;UID='+uid+';PWD='+pwd+';QUIETMODE=YES;')

# Get SQL Query --if left empty, will run imbedded query
query = raw_input("Enter query to run: ")

# Create cursor for SELECT Query
curs = conn.cursor()

# Display Progress
print "Executing Query..."

# Set Query
if query == "":
    curs.execute("""SELECT * FROM FOOBAR""")
else:
    curs.execute(query)

# Create new CSV File for Consumption

csvName = raw_input("Give the file a name :")
try:
    csvFile = open(csvName+'.csv','wb')
except:
    os.remove(csvName+'.csv')
    csvFile = open(csvName+'.csv','wb')

# Push records into CSV file
print "Writting to CSV file for Consumption"

writer = csv.writer(csvName+'.csv')
writer.writerow([i[0] for i in curs.description])
writer.writerows(curs)

# Close Connection 
conn.close()

csvFile.close()

# TDE File Creation
# TDE Functions
def add_tde_col(colnum, row, val, t):
    dateformat = '%Y-%m-%d'
    datetimeformat = '%Y-%m-%d %H:%M:%S'

    if t == tdeTypes['INTEGER']:
        try:
            convert = int(val)
            row.setInteger(colnum, convert)
        except ValueError:
            row.setNull(colnum)

    elif t == tdeTypes['DOUBLE']:
        try:
            convert = float(val)
            row.setDouble(colnum, convert)
        except ValueError:
            row.setNull(colnum)

    elif t == tdeTypes['BOOLEAN']:
        try:
            convert = int(val)
            if convert > -1 and convert <= 1:
                row.setBoolean(colnum, convert)
            else:
                row.setNull(colnum)
        except ValueError:
            row.setNull(colnum)
    
    elif t == tdeTypes['DATE']:
        try:
            d = datetime.datetime.strptime(val, dateformat)
            row.setDate(colnum, d.year, d.month, d.day)
        except ValueError:
            row.setNull(colnum)

    elif t == tdeTypes['DATETIME']:
        try:
            d = datetime.datetime.strptime(val, datetimeformat)
            row.setDateTime(colnum, d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond)
        except ValueError:
            row.setNull(colnum)

    elif t == tdeTypes['CHAR_STRING']:
        row.setCharString(colnum, val)

    elif t == tdeTypes['UNICODE_STRING']:
        row.setString(colnum, val)

    elif t == tdeTypes['DURATION']:
        print "Don't know what to do here. ^_^"

    else:
        print 'Something is missing here.'
        row.setNull(colnum)

# Tableau DataTypes Dictionary Creation
tdeTypes = {'INTEGER': 7, 'DOUBLE': 10, 'BOOLEAN': 11, 'DATE': 12, 'DATETIME': 13, 'DURATION': 14, 'CHAR_STRING': 15, 'UNICODE_STRING': 16}

# This part is unique for each extract. 
# Will need to modify.. 
# TODO: Make this more dynamic
################################################################################################
################################################################################################

csvSchema = []
csvSchema.append({'<COLUMNNAME>': tdeTypes['INTEGER']})

################################################################################################
################################################################################################

# Try to create the TDE File
try:
    tdeFile = tde.Extract(csvName+'.tde')

except:
    os.remove(csvName+'.tde')
    tdeFile = tde.Extract(csvName+'.tde')


# Open newly created CSV File
csvFile = open(csvName+'.csv',"r")
reader = csv.reader(csvFile)
print 'Reading records from %s' % (csvName+'.csv')

# Create TDE Table definition
tdeTableDef = tde.TableDefinition()

# Build TDE Table Definition from csv schema above
print 'Defined table schema:'
for index, item in enumerate(csvSchema):
    for k, v in item.items():
        print 'Column %i: %s <%s>' % (index, k, tdeTypes.keys()[tdeTypes.values().index(v)])
        tdeTableDef.addColumn(k, v)

# Add table to TDE File
tdeTable = tdeFile.addTable("Extract", tdeTableDef)

# Iterate through rows and columns of csv and adding them to TDE File
print 'Writing records to %s' % (csvName+'.tde')

rownum = 0
for row in reader:
    if rownum == 0:
        header = row
    else:
        colnum = 0
        tdeRow = tde.Row(tdeTableDef)
        for col in row:
            if colnum+1 > len(csvSchema):
                print 'Something is missing here.'
                break
            add_tde_col(colnum, tdeRow, row[colnum], csvSchema[colnum].values()[0])
            colnum += 1
        tdeTable.insert(tdeRow)
        tdeRow.close()
    rownum += 1

print '%i rows added in total in %f seconds' % (rownum-1, time.clock()-startTime)
print 'Closing TDE and CSV File...'
tdeFile.close()
csvFile.close()
