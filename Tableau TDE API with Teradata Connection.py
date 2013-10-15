import dataextract as tde
import pyodbc as odbc
import getpass, os, datetime, csv, time, sys, getopt, subprocess

global dir
global tp
global ts
global td
global outputfile
global fiscalWeek
global startTime

def runQuery():
    startTime = time.clock()

    # Get Teradata User Information
    uid = raw_input("Enter UID for Teradata: ")
    pwd = getpass.getpass()

    # Set Teradata Connection
    print "Connecting to Teradata..."
    conn = odbc.connect('DRIVER={TERADATA};DBCNAME=<DBCNAME;UID='+uid+';PWD='+pwd+';QUIETMODE=YES;')

    # Create cursor for SELECT Query
    curs = conn.cursor()

    # Display Progress
    print "Executing Query..."

    # Set Query
    curs.execute("""SELECT * FROM FOOBAR""")


    ## File Loop for CSV Adjustment / Moves
    #fileDir = dir
    #for root, dirs, filenames in os.walk(fileDir):
    #    for file in filenames:
    #        if ".csv" in file:
    #            os.rename(file, file.replace(".csv","<week/date>.csv",1))
            

    # Create new CSV File for Consumption
    csvName = outputfile
    try:
        csvFile = open(csvName+'.csv','wb')
    except:
        os.remove(csvName+'.csv')
        csvFile = open(csvName+'.csv','wb')

    # Push records into CSV file
    print "Writting to CSV file for Consumption"
    print "%i records returned" % (curs.rowcount)

    writer = csv.writer(csvFile)
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

def createTDEFile():
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
        tdeFile = tde.Extract(outputfile+'.tde')

    except:
        os.remove(outputfile+'.tde')
        tdeFile = tde.Extract(outputfile+'.tde')


    # Open newly created CSV File
    csvFile = open(outputfile+'.csv',"r")
    reader = csv.reader(csvFile)
    print 'Reading records from %s' % (outputfile+'.csv')

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

def RunUpdateProcess():
    subprocess.check_output("C:\Program Files (x86)\Tableau\Tableau 8.0\bin\tableau addfiletoextract --file '"+ dir + "\\" + outputfile + '.csv' + "' --server "+ ts +" --project '"+ tp +"' --datasource '"+ td +"' --username <user> --password <pass>",
                            stderror = subprocess.STDOUT,
                            shell=False)
    print stderror

def main(argv):
    dir = ''
    tp = ''
    ts = ''
    td = ''
    outputfile = ''
    fiscalWeek = ''
    try:
        opts, args = getopt.getopt(argv, "hv:d:tp:ts:td:v:fw:o:",["help","directory=","tableau-project=","tableau-server=","tableau-datasource=","version=","fiscalweek","output-file="])
    except getopt.GetoptError:
        print 'Tableau Data Extract File Generation from Teradata using SQL Query'
        print ''
        print 'USAGE: file -d <directory> -tp <projectname>' 
        print '       -ts <server-url> -td <datasourcename>'
        print '       -o <datastorefilename>'
        print '-h,  --help                  Returns this help file'
        print '-v,  --version               Version information'
        print '-d,  --directory             Directory of file store'
        print '-tp, --tableau-project       Tableau Project Name'
        print '-ts, --tableau-server        Tableau Server Address (http)'
        print '-td, --tableau-datasource    Tableau Data Source Name'
        print '-o,  --output-file           Filename to store data as.'
        print '                             This file will be the naem of'
        print '                             the file stored on the local'
        print '                             system.'
        print '-fw, --fiscalweek            Week for query to be ran for'
        sys.exit(2)
    for opt, arg in opts:
        if opt in("-h","--help"):
            print 'Tableau Data Extract File Generation from Teradata using SQL Query'
            print ''
            print 'USAGE: file -d <directory> -tp <projectname>' 
            print '       -ts <server-url> -td <datasourcename>'
            print '       -o <datastorefilename>'
            print '-h,  --help                  Returns this help file'
            print '-v,  --version               Version information'
            print '-d,  --directory             Directory of file store'
            print '-tp, --tableau-project       Tableau Project Name'
            print '-ts, --tableau-server        Tableau Server Address (http)'
            print '-td, --tableau-datasource    Tableau Data Source Name'
            print '-o,  --output-file           Filename to store data as.'
            print '                             This file will be the naem of'
            print '                             the file stored on the local'
            print '                             system.'
            print '-fw, --fiscalweek            Week for query to be ran for'
            sys.exit()
        elif opt in ("-d","--directory"):
            dir = arg
        elif opt in ("tp","--tableau-project"):
            tp = arg
        elif opt in ("ts","--tableau-server"):
            ts = arg
        elif opt in ("td","--tableau-datasource"):
            td = arg
        elif opt in ("-o","--output-file"):
            outputfile = arg
        elif opt in ("-fw","--fiscalweek"):
            fiscalWeek = arg
    if dir == '' or tp == '' or ts == '' or td == '' or outputfile == '' or fiscalWeek == '':
        print 'Tableau Data Extract File Generation from Teradata using SQL Query'
        print ''
        print 'USAGE: file -d <directory> -tp <projectname>' 
        print '       -ts <server-url> -td <datasourcename>'
        print '       -o <datastorefilename>'
        print '-h,  --help                  Returns this help file'
        print '-v,  --version               Version information'
        print '-d,  --directory             Directory of file store'
        print '-tp, --tableau-project       Tableau Project Name'
        print '-ts, --tableau-server        Tableau Server Address (http)'
        print '-td, --tableau-datasource    Tableau Data Source Name'
        print '-o,  --output-file           Filename to store data as.'
        print '                             This file will be the naem of'
        print '                             the file stored on the local'
        print '                             system.'
        print '-fw, --fiscalweek            Week for query to be ran for'
        sys.exit()
    else:
        runQuery()
        createTDEFile()
        RunUpdateProcess()

# Main Load function
if __name__ == "__main__":
    main(sys.argv[1:])
