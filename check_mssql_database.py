#!/usr/bin/env python
################### check_mssql_database.py ####################
# Version 1.1.0
# Date : Jan 25th 2012
# Author  : Nicholas Scott ( scot0357 at gmail.com )
# Help : scot0357 at gmail.com
# Licence : GPL - http://www.fsf.org/licenses/gpl.txt
# TODO : Bug Testing, Feature Adding
# Changelog:
# 1.1.0 -   Fixed port bug allowing for non default ports | Thanks CBTSDon
#           Added mode error checking which caused non-graceful exit | Thanks mike from austria
# 1.2.0 -   Added ability to monitor instances
#           Added check to see if pymssql is installed
# 1.3.0 -   Added ability specify MSSQL instances
#################################################################
# Import Libraries

import time, sys
from optparse import OptionParser, OptionGroup

try:
    import pymssql
except:
    print "Pymssql is required for operation. Please install it."
    sys.exit(4)

# These arrays dictates what is fed to the required args.
# Space 1 :: short flag
# Space 2 :: long flag name which will also be the name of the stored variable
# Space 3 :: Help string
# Space 4 :: Default value
requiredArgs = [
                ( '-H' , 'hostname' , 'Specify MSSQL Server Address'    , False ),
                ( '-U' , 'user'     , 'Specify MSSQL User Name'         , False ),
                ( '-P' , 'password' , 'Specify MSSQL Password'          , False ),
                ( '-T' , 'table'    , 'Specify MSSQL Table'             , False ),
               ]

threshArgs   = [
                ( '-I' , 'instance' , 'Specify instance' , False),
                ( '-p' , 'port'     , 'Specify port' , False),
                ( '-w' , 'warning' , 'Specify min:max threshold'        , 0 ),
                ( '-c' , 'critical', 'Specify min:max threshold'        , 0 ),
               ]

modeArgs     = [
                ( '' , 'datasize'       , 'Check size of database'      ),
                ( '' , 'logfileusage'   , 'Check Log File Usage'        ),
                ( '' , 'activetrans'    , 'Check Active Transactions'   ),
                ( '' , 'transpsec'      , 'Check Transactions/Sec'      ),
                ( '' , 'logcachehit'    , 'Check Log Cache Hit Ratio'   ),
                ( '' , 'time2connect'   , 'Check Time to Connect [DEFAULT IF NONE GIVEN]'       ),
                ( '' , 'loggrowths'     , 'Check Log Growths'           ),
                ( '' , 'logshrinks'     , 'Check Log Shrinks'           ),
                ( '' , 'logtruncs'      , 'Check Log Truncations'       ),
                ( '' , 'logwait'        , 'Check Log Flush Wait Times'  ),
                ( '' , 'logflushes'     , 'Check Log Flushes/Sec'       ),
               ]

def errorDict( error ):
    errorTome =  {  
                    -1  : "Unable to access SQL Server.",
                    -2  : "Can access server but cannot query.",
                    -3  : "Zero on in divisor.",
               }
    retTome = { 'code' : 2,
                'info' : errorTome[error],
                'label': '',
                }
    return retTome
    
# The function takes three arguments, three lists specified above, ensures
# proper input from the user and then returns a dictionary with keys of each
# variable entered correlating to their value.
def parseArgs( req , thresh , mode ):
    tome = {}
    usage = "usage: %prog -H hostname -U user -P password -T table --mode"
    parser = OptionParser(usage=usage)
    # Declare Required Options
    required = OptionGroup(parser, "Required Options")
    for arg in req:
        required.add_option( arg[0] , '--' + arg[1] , help = arg[2] , default = arg[3] )
    parser.add_option_group(required)
    # Declare Threshold Options
    threshold = OptionGroup(parser, "Threshold Options")
    for arg in thresh:
        threshold.add_option( arg[0] , '--' + arg[1] , help = arg[2] , default = arg[3] )
    parser.add_option_group(threshold)
    # Declare Mode Options
    mod = OptionGroup(parser, "Mode Options")
    for arg in modeArgs:
        mod.add_option( '--' + arg[1] , action="store_true" , help = arg[2] , default = False )
    parser.add_option_group(mod)
    # Parse all args in options.args, convert to iterable dictionary
    ( options, args ) = parser.parse_args()
    for arg in required.option_list:
        tome[ arg.dest ] = getattr(options, arg.dest)
        if not tome[ arg.dest ]:
            print "All arguments listed in Required Options must have a value."
            parser.print_help()
            sys.exit(3)
    for arg in threshold.option_list:
        tome[ arg.dest ] =  getattr(options, arg.dest)
    temp = False
    for arg in mod.option_list:
        if getattr(options, arg.dest) and temp:
            print "Must choose one and only Mode Option."
            parser.print_usage()
            sys.exit(3)
        elif getattr(options, arg.dest):
            tome['mode'] = arg.dest
            temp = True
    return tome

# Gets passed the warning/critical threshold strings and returns a tuple
# of the upper/lower boundary. Compliant with current Nagios guidelines.
def establish_thresholds( threshString ):
    try:
        at      = False
        container = ( '' , '' )
        if not threshString:
            return container
        elif ':' in threshString:
            if '@' in threshString:
                threshString = threshString[1:]
                at = True
            newStr = threshString.split(':')
            if '~' in newStr[0]:
                container = ( '' , int(newStr[1]) )
            elif not newStr[1]:
                container = ( float(newStr[0]) , '' )
            else:
                container = ( float(newStr[0]) , float(newStr[1]) )
                if at:
                    container = ( container[1] - .01 , container[0] + .01 )
        else:
            container = ( 0 , int(threshString) )
        return container
    except:
        return 1

# Takes the value and the crit and warning tuples as arguments a returns
# the corresponding return code.
def get_return_code( value , crit , warn ):
    crit = establish_thresholds(crit)
    warn = establish_thresholds(warn)
    if value < 0:
        return 2    
    code = 0
    if warn[0] and value < warn[0]:
        code = 1
    if crit[0] and value < crit[0]:
        code = 2
    if warn[1] and value > warn[1]:
        code = 1
    if crit[1] and value > crit[1]:
        code = 2
    return code

# Takes the info dictionary of the value and the return code and creates
# the return string (with performance data.)
def get_return_string( tome ):
    retcode = tome['code']
    retString = ''
    if retcode == 0:
        retString += 'OK:'
    elif retcode == 1:
        retString += 'WARNING:'
    elif retcode == 2:
        retString += 'CRITICAL:'
    else:
        retString += 'UNKNOWN:'
    retString += tome['info']
    if tome['label']:
        retString += '|' + tome['label'] + '=' + str(tome['value']) + tome['uom'] + ';;;;'
    return retString

# For use in actual check function. Takes an empty dictionary, the name
# of the metric being checked, the units it will have and what its value
# is. Return a dictionary with proper keys for each item.
def get_return_tome( name , units , value ):
    tome = {}
    tome['value']   = value
    tome['uom']     = units
    tome['info']    = name + " is " + str(value) + str(units)
    tome['label']   = name.replace( ' ' , '_' )
    return tome

# Properly exits from the program.
def doExit( tome ):
    print get_return_string( tome )
    sys.exit(tome['code'])

# Connect to MSSQL database. Given hostname, port and vuser, vpassword.
def connectDB(hostname, vport, vuser, vpassword, vinstance):
    host = hostname
    if vinstance:
        host += "\\" + vinstance
    if vport:
        host += ":" + vport
    try:
        conn = pymssql.connect(host = host, user = vuser, password = vpassword, database = 'master')
        return conn
    except:
        return -1

def logfileusage(conn , table):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Percent Log Used' AND instance_name='" + table + "';")
        return get_return_tome( 'Log File Usage' , '%' , cur.fetchone()[0] )
    except:
        return -2 # Return unable to query database

def logcachehit( conn , table):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Log Cache Hit Ratio' AND instance_name='" + table + "';")
        top = cur.fetchone()[0]
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Log Cache Hit Ratio Base' AND instance_name='" + table + "';")
        bot = cur.fetchone()[0]
        if bot == 0:
            value = 0
        else:
            value = (float(top) / bot) * 100
        return get_return_tome( 'Log Cache Hit Ratio' , '%' , round(value , 2) )    
    except:
        return -2 # Return unable to query database

def activetrans( conn , table):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Active Transactions' AND instance_name='" + table + "';")
        return get_return_tome( 'Active Transactions' , '' , int(cur.fetchone()[0]) )
    except:
        return -2 # Return unable to query database

def logfileusage(conn , table):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Percent Log Used' AND instance_name='" + table + "';")
        return get_return_tome( 'Log File Usage' , '%' , cur.fetchone()[0] )
    except:
        return -2 # Return unable to query database

def logflushes(  conn , table):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Log Flushes/sec' AND instance_name='" + table + "';")
        begin = cur.fetchone()[0]
        time.sleep(3)
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Log Flushes/sec' AND instance_name='" + table + "';")
        total = ( cur.fetchone()[0] - begin ) / 3
        return get_return_tome( 'Log Flushes Per Second' , '' , total )
    except:
        return -2 # Return unable to query database
        
def transpsec(   conn , table):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Transactions/sec' AND instance_name='" + table + "';")
        begin = cur.fetchone()[0]
        time.sleep(3)
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Transactions/sec' AND instance_name='" + table + "';")
        total = ( cur.fetchone()[0] - begin ) / 3
        return get_return_tome( 'Transactions Per Second' , '' , total )
    except:
        return -2 # Return unable to query database

def loggrowths(  conn , table):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Log Growths' AND instance_name='" + table + "';")
        return get_return_tome( 'Log Growths' , '' , int(cur.fetchone()[0]) )
    except:
        return -2 # Return unable to query database

def logshrinks(  conn , table):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Log Shrinks' AND instance_name='" + table + "';")
        return get_return_tome( 'Log Shrinks' , '' , int(cur.fetchone()[0]) )
    except:
        return -2 # Return unable to query database

def logtruncs(  conn , table):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Log Truncations' AND instance_name='" + table + "';")
        return get_return_tome( 'Log Truncations' , '' , int(cur.fetchone()[0]) )
    except:
        return -2 # Return unable to query database

def logwait(  conn , table):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Log Flush Wait Time' AND instance_name='" + table + "';")
        return get_return_tome( 'Log Flush Wait Time' , 'ms' , int(cur.fetchone()[0]) )
    except:
        return -2 # Return unable to query database

def datasize(  conn , table):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Data File(s) Size (KB)' AND instance_name='" + table + "';")
        return get_return_tome( 'Size of database' , 'KB' , int(cur.fetchone()[0]) )
    except:
        return -2 # Return unable to query database

def time2connect( index ):
    try:
        begin = time.time()
        conn  = connectDB(  index['hostname'] , index['port'] , index['user'],\
                            index['password'] , index['instance'] )
        end   = time.time()
        return get_return_tome( 'Time to connect' , 's' , round( end - begin , 3 ) )
    except:
        return -1

def main( req , thresh , mode ):
    index = parseArgs( req , thresh , mode )
    try:
        index['mode']
    except:
        index['mode'] = 'time2connect'
    conn  = connectDB(  index['hostname'] , index['port'] , index['user'],\
                        index['password'] , index['instance'] )
    if not isinstance( conn , int ):
        if index['mode'] == 'time2connect':
            conn.close()
            retTome     = time2connect( index )
        else:
            retTome         = eval(index['mode'])( conn , index['table'])
        if isinstance( retTome , dict ):
            retTome['code'] = get_return_code( retTome['value'] , index['critical'] , index['warning']) 
        else:
            retTome = errorDict( retTome )
        conn.close()
    else:
        retTome = errorDict ( conn )
    doExit( retTome )

main( requiredArgs , threshArgs , modeArgs )

