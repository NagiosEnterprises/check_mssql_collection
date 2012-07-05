#!/usr/bin/env python
################### check_mssql_server.py ####################
# Version 1.2.0
# Date : Jan 25th 2012
# Author  : Nicholas Scott ( scot0357 at gmail.com )
# Help : scot0357 at gmail.com
# Licence : GPL - http://www.fsf.org/licenses/gpl.txt
# TODO : Bug Testing, Feature Adding
# Changelog : 
# 1.0.2 -   Fixed Uptime Counter to be based off of database
#           Fixed divide by zero error in transpsec
# 1.1.0 -   Fixed port bug allowing for non default ports | Thanks CBTSDon
#           Added batchreq, sqlcompilations, fullscans, pagelife | Thanks mike from austria
#           Added mode error checking which caused non-graceful exit | Thanks mike from austria
# 1.2.0 -   Added ability to specify instances
#################################################################

# Import Libraries
import pymssql, time, sys
from optparse import OptionParser, OptionGroup

# These arrays dictates what is fed to the required args.
# Space 1 :: short flag
# Space 2 :: long flag name which will also be the name of the stored variable
# Space 3 :: Help string
# Space 4 :: Default value
requiredArgs = [
                ( '-H' , 'hostname' , 'Specify MSSQL Server Address'    , False ),
                ( '-U' , 'user'     , 'Specify MSSQL User Name'         , False ),
                ( '-P' , 'password' , 'Specify MSSQL Password'          , False ),
               ]

threshArgs   = [
                ( '-I' , 'instance' , 'Specify instance'                , False),
                ( '-p' , 'port'     , 'Specify port. [Default: 1433]'   , False),                
                ( '-w' , 'warning'  , 'Specify min:max threshold'       , 0 ),
                ( '-c' , 'critical' , 'Specify min:max threshold'       , 0 ),
               ]

modeArgs     = [
                ( '' , 'bufferhitratio' , 'Buffer Cache Hit Ratio'      ),
                ( '' , 'pagelooks'      , 'Page Lookups Per Second'     ),
                ( '' , 'freepages'      , 'Free Pages (Cumulative)'     ),
                ( '' , 'totalpages'     , 'Total Pages (Cumulative)'    ),
                ( '' , 'targetpages'    , 'Target Pages'                ),
                ( '' , 'databasepages'  , 'Database Pages'              ),
                ( '' , 'stolenpages'    , 'Stolen Pages'                ),
                ( '' , 'lazywrites'     , 'Lazy Writes / Sec'           ),
                ( '' , 'readahead'      , 'Readahead Pages / Sec'       ),
                ( '' , 'pagereads'      , 'Page Reads / Sec'            ),
                ( '' , 'checkpoints'    , 'Checkpoint Pages / Sec'      ),
                ( '' , 'pagewrites'     , 'Page Writes / Sec'           ),
                ( '' , 'lockrequests'   , 'Lock Requests / Sec'         ),
                ( '' , 'locktimeouts'   , 'Lock Timeouts / Sec'         ),
                ( '' , 'deadlocks'      , 'Deadlocks / Sec'             ),
                ( '' , 'lockwaits'      , 'Lockwaits / Sec'             ),
                ( '' , 'lockwait'       , 'Lock Wait Average Time (ms)' ),
                ( '' , 'averagewait'    , 'Average Wait Time (ms)'      ),
                ( '' , 'pagesplits'     , 'Page Splits / Sec'           ),
                ( '' , 'cachehit'       , 'Cache Hit Ratio'             ),
                ( '' , 'time2connect'   , 'Check Time to Connect [DEFAULT]'     ),
                ( '' , 'batchreq'       , 'Batch Requests / Sec'        ),
                ( '' , 'sqlcompilations', 'SQL Compilations / Sec'      ),
                ( '' , 'fullscans'      , 'Full Scans / Sec'            ),
                ( '' , 'pagelife'       , 'Page Life Expectancy'        ),
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
    print host
    #sys.exit(1)
    try:
        conn = pymssql.connect(host = host, user = vuser, password = vpassword, database = 'master')
        return conn
    except:
        return -1
        
def bufferhitratio(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Buffer cache hit ratio' AND instance_name='';")
        top = cur.fetchone()[0]
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Buffer cache hit ratio base' AND instance_name='';")
        bot = cur.fetchone()[0]
        return get_return_tome( 'Buffer Cache Hit Ratio' , '%' , round(float(top) / bot * 100, 2))
    except:
        return -2 # Return unable to query database

def pagelooks(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Page lookups/sec' AND instance_name='';")
        top = cur.fetchone()[0]
        return get_return_tome( 'Page Lookups / Sec' , '' , round(float(top) / uptime, 2) )
    except:
        return -2 # Return unable to query database 
        
def freepages(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Free pages' AND instance_name='';")
        top = cur.fetchone()[0]
        return get_return_tome( 'Free Pages' , '' , int(top) )
    except:
        return -2 # Return unable to query database 

def totalpages(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Total pages' AND instance_name='';")
        top = cur.fetchone()[0]
        return get_return_tome( 'Total pages' , '' , int(top) )
    except:
        return -2 # Return unable to query database

def targetpages(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Target pages' AND instance_name='';")
        top = cur.fetchone()[0]
        return get_return_tome( 'Target pages' , '' , int(top) )
    except:
        return -2 # Return unable to query database

def databasepages(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Database pages' AND instance_name='';")
        top = cur.fetchone()[0]
        return get_return_tome( 'Database pages' , '' , int(top) )
    except:
        return -2 # Return unable to query database

def stolenpages(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Stolen pages' AND instance_name='';")
        top = cur.fetchone()[0]
        return get_return_tome( 'Stolen pages' , '' , int(top) )
    except:
        return -2 # Return unable to query database

def lazywrites(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Lazy writes/sec' AND instance_name='';")
        top = cur.fetchone()[0]
        return get_return_tome( 'Lazy Writes / Sec' , '' , round(float(top) / uptime, 2) )
    except:
        return -2 # Return unable to query database 

def readahead(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Readahead pages/sec' AND instance_name='';")
        top = cur.fetchone()[0]
        return get_return_tome( 'Readahead Pages / Sec' , '' , round(float(top) / uptime, 2) )
    except:
        return -2 # Return unable to query database 

def pagereads(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Page reads/sec' AND instance_name='';")
        top = cur.fetchone()[0]
        return get_return_tome( 'Page Reads / Sec' , '' , round(float(top) / uptime, 2) )
    except:
        return -2 # Return unable to query database 

def pagewrites(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Page writes/sec' AND instance_name='';")
        top = cur.fetchone()[0]
        return get_return_tome( 'Page Writes / Sec' , '' , round(float(top) / uptime, 2) )
    except:
        return -2 # Return unable to query database 

def checkpoints(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Checkpoint pages/sec' AND instance_name='';")
        top = cur.fetchone()[0]
        return get_return_tome( 'Checkpoint Pages / Sec' , '' , round(float(top) / uptime, 2) )
    except:
        return -2 # Return unable to query database 

def lockrequests(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Lock Requests/sec' AND instance_name='_Total';")
        top = cur.fetchone()[0]
        return get_return_tome( 'Lock Requests / Sec' , '' , round(float(top) / uptime, 2) )
    except:
        return -2 # Return unable to query database 

def locktimeouts(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Lock timeouts/sec' AND instance_name='_Total';")
        top = cur.fetchone()[0]
        return get_return_tome( 'Lock Timeouts / Sec' , '' , round(float(top) / uptime, 2) )
    except:
        return -2 # Return unable to query database 

def deadlocks(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Number of Deadlocks/sec' AND instance_name='_Total';")
        top = cur.fetchone()[0]
        return get_return_tome( 'Lock Requests / Sec' , '' , round(float(top) / uptime, 2) )
    except:
        return -2 # Return unable to query database 

def lockwaits(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Lock Waits/sec' AND instance_name='_Total';")
        top = cur.fetchone()[0]
        return get_return_tome( 'Lock Waits / Sec' , '' , round(float(top) / uptime, 2) )
    except:
        return -2 # Return unable to query database 

def lockwait(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Lock Wait Time (ms)' AND instance_name='_Total';")
        return get_return_tome( 'Lock Wait Time Average' , 'ms' , cur.fetchone()[0] )
    except:
        return -2 # Return unable to query database

def averagewait(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Average Wait Time (ms)' AND instance_name='_Total';")
        return get_return_tome( 'Average Wait Time' , 'ms' , cur.fetchone()[0] )
    except:
        return -2 # Return unable to query database

def time2connect( index ):
    try:
        begin = time.time()
        conn  = connectDB(  index['hostname'] , index['port'] , index['user'],\
                            index['password'] )
        end   = time.time()
        return get_return_tome( 'Time to connect' , 's' , round( end - begin , 3 ) )
    except:
        return -1

def cachehit(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Cache Hit Ratio' AND instance_name='_Total' AND object_name='SQLServer:Plan Cache';")
        top = cur.fetchone()[0]
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Cache Hit Ratio Base' AND instance_name='_Total' AND object_name='SQLServer:Plan Cache';")
        bot = cur.fetchone()[0]
        return get_return_tome( 'Cache Hit Ratio' , '%' , round(float(top) / bot * 100, 2))
    except:
        return -2 # Return unable to query database
        
def pagesplits(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Page Splits/sec' AND object_name='SQLServer:Access Methods';")
        top = cur.fetchone()[0]
        return get_return_tome( 'Page Splits / Sec' , '' , round(float(top) / uptime, 2) )
    except:
        return -2 # Return unable to query database 

def batchreq(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Batch Requests/sec' AND object_name='SQLServer:SQL Statistics';")
        top = cur.fetchone()[0]
        return get_return_tome( 'Batch Requests / sec' , '' , round(float(top) / uptime, 2) )
    except:
        return -2 # Return unable to query database
        
def sqlcompilations(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='SQL Compilations/sec' AND object_name='SQLServer:SQL Statistics';")
        top = cur.fetchone()[0]
        return get_return_tome( 'SQL Compilations / sec' , '' , round(float(top) / uptime, 2) )
    except:
        return -2 # Return unable to query database
        
def fullscans(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Full Scans/sec' AND object_name='SQLServer:Access Methods';")
        top = cur.fetchone()[0]
        return get_return_tome( 'Full Scans / sec' , '' , round(float(top) / uptime, 2) )
    except:
        return -2 # Return unable to query database
        
def pagelife(conn , table, uptime ):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cntr_value FROM sysperfinfo WHERE counter_name='Page life expectancy' AND object_name='SQLServer:Buffer Manager';")
        return get_return_tome( 'Page Life Expectancy' , 's' , cur.fetchone()[0] )
    except:
        return -2 # Return unable to query database

def time2connect( index ):
    try:
        begin = time.time()
        conn  = connectDB(  index['hostname'] , index['port'] , index['user'],\
                            index['password'] )
        end   = time.time()
        return get_return_tome( 'Time to connect' , 's' , round( end - begin , 3 ) )
    except:
        return -1

def main( req , thresh , mode ):
    index = parseArgs( req , thresh , mode )
    index['table'] = 'master'
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
            cur = conn.cursor()
            cur.execute("SELECT DATEDIFF( ss , crdate , GETDATE()) FROM sysdatabases WHERE name = 'tempdb'")
            uptime          = cur.fetchone()[0]
            retTome         = eval(index['mode'])( conn , index['table'] , uptime )
        if isinstance( retTome , dict ):
            retTome['code'] = get_return_code( retTome['value'] , index['critical'] , index['warning']) 
        else:
            retTome = errorDict( retTome )
        conn.close()
    else:
        retTome = errorDict ( conn )
    doExit( retTome )

main( requiredArgs , threshArgs , modeArgs )
