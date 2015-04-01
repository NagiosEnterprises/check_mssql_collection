#!/usr/bin/env python
################### check_mssql_database.py ############################
# Version 2.0.2
# Date : Apr 4 2013
# Author  : Nicholas Scott ( scot0357 at gmail.com )
# Help : scot0357 at gmail.com
# Licence : GPL - http://www.fsf.org/licenses/gpl.txt
#
# Changelog : 
# 1.0.2 -   Fixed Uptime Counter to be based off of database
#           Fixed divide by zero error in transpsec
# 1.1.0 -   Fixed port bug allowing for non default ports | Thanks CBTSDon
#           Added batchreq, sqlcompilations, fullscans, pagelife | Thanks mike from austria
#           Added mode error checking which caused non-graceful exit | Thanks mike from austria
# 1.2.0 -   Added ability to specify instances
# 2.0.0 -   Complete rewrite of the structure, re-evaluated some queries
#           to hopefully make them more portable | Thanks CFriese
#           Updated the way averages are taken, no longer needs tempdb access
# 2.0.1 -   Fixed try/finally statement to accomodate Python 2.4 for
#           legacy systems
# 2.0.2 -   Fixed issues where the SQL cache hit queries were yielding improper results
#           when done on large systems | Thanks CTrahan
# 2.0.3 -   Remove misleading description of lockwait, removing the word Average -SW
# Modified 01/22/2015 Removed extraneous ';' from output. -BD-G
########################################################################

import pymssql
import time
import sys
import tempfile
try:
    import cPickle as pickle
except:
    import pickle
from optparse import OptionParser, OptionGroup

BASE_QUERY = "SELECT cntr_value FROM sysperfinfo WHERE counter_name='%s' AND instance_name='';"
INST_QUERY = "SELECT cntr_value FROM sysperfinfo WHERE counter_name='%s' AND instance_name='%s';"
OBJE_QUERY = "SELECT cntr_value FROM sysperfinfo WHERE counter_name='%s';"
DIVI_QUERY = "SELECT cntr_value FROM sysperfinfo WHERE counter_name LIKE '%s%%' AND instance_name='%s';"

MODES     = {
    
    'bufferhitratio'    : { 'help'      : 'Buffer Cache Hit Ratio',
                            'stdout'    : 'Buffer Cache Hit Ratio is %s%%',
                            'label'     : 'buffer_cache_hit_ratio',
                            'unit'      : '%',
                            'query'     : DIVI_QUERY % ('Buffer cache hit ratio', ''),
                            'type'      : 'divide',
                            'modifier'  : 100,
                            },
    
    'pagelooks'         : { 'help'      : 'Page Lookups Per Second',
                            'stdout'    : 'Page Lookups Per Second is %s',
                            'label'     : 'page_lookups',
                            'query'     : BASE_QUERY % 'Page lookups/sec',
                            'type'      : 'delta'
                            },
    
    'freepages'         : { 'help'      : 'Free Pages (Cumulative)',
                            'stdout'    : 'Free pages is %s',
                            'label'     : 'free_pages',
                            'type'      : 'standard',
                            'query'     : BASE_QUERY % 'Free pages'
                            },
                            
    'totalpages'        : { 'help'      : 'Total Pages (Cumulative)',
                            'stdout'    : 'Total pages is %s',
                            'label'     : 'totalpages',
                            'type'      : 'standard',
                            'query'     : BASE_QUERY % 'Total pages',
                            },
                            
    'targetpages'       : { 'help'      : 'Target Pages',
                            'stdout'    : 'Target pages are %s',
                            'label'     : 'target_pages',
                            'type'      : 'standard',
                            'query'     : BASE_QUERY % 'Target pages',
                            },
                            
    'databasepages'     : { 'help'      : 'Database Pages',
                            'stdout'    : 'Database pages are %s',
                            'label'     : 'database_pages',
                            'type'      : 'standard',
                            'query'     : BASE_QUERY % 'Database pages',
                            },
    
    'stolenpages'       : { 'help'      : 'Stolen Pages',
                            'stdout'    : 'Stolen pages are %s',
                            'label'     : 'stolen_pages',
                            'type'      : 'standard',
                            'query'     : BASE_QUERY % 'Stolen pages',
                            },
    
    'lazywrites'        : { 'help'      : 'Lazy Writes / Sec',
                            'stdout'    : 'Lazy Writes / Sec is %s/sec',
                            'label'     : 'lazy_writes',
                            'query'     : BASE_QUERY % 'Lazy writes/sec',
                            'type'      : 'delta'
                            },
    
    'readahead'         : { 'help'      : 'Readahead Pages / Sec',
                            'stdout'    : 'Readahead Pages / Sec is %s/sec',
                            'label'     : 'readaheads',
                            'query'     : BASE_QUERY % 'Readahead pages/sec',
                            'type'      : 'delta',
                            },
                            
    
    'pagereads'         : { 'help'      : 'Page Reads / Sec',
                            'stdout'    : 'Page Reads / Sec is %s/sec',
                            'label'     : 'page_reads',
                            'query'     : BASE_QUERY % 'Page reads/sec',
                            'type'      : 'delta'
                            },
    
    'checkpoints'       : { 'help'      : 'Checkpoint Pages / Sec',
                            'stdout'    : 'Checkpoint Pages / Sec is %s/sec',
                            'label'     : 'checkpoint_pages',
                            'query'     : BASE_QUERY % 'Checkpoint pages/Sec',
                            'type'      : 'delta'
                            },
                            
    
    'pagewrites'        : { 'help'      : 'Page Writes / Sec',
                            'stdout'    : 'Page Writes / Sec is %s/sec',
                            'label'     : 'page_writes',
                            'query'     : BASE_QUERY % 'Page writes/sec',
                            'type'      : 'delta',
                            },
    
    'lockrequests'      : { 'help'      : 'Lock Requests / Sec',
                            'stdout'    : 'Lock Requests / Sec is %s/sec',
                            'label'     : 'lock_requests',
                            'query'     : INST_QUERY % ('Lock requests/sec', '_Total'),
                            'type'      : 'delta',
                            },
    
    'locktimeouts'      : { 'help'      : 'Lock Timeouts / Sec',
                            'stdout'    : 'Lock Timeouts / Sec is %s/sec',
                            'label'     : 'lock_timeouts',
                            'query'     : INST_QUERY % ('Lock timeouts/sec', '_Total'),
                            'type'      : 'delta',
                            },
    
    'deadlocks'         : { 'help'      : 'Deadlocks / Sec',
                            'stdout'    : 'Deadlocks / Sec is %s/sec',
                            'label'     : 'deadlocks',
                            'query'     : INST_QUERY % ('Number of Deadlocks/sec', '_Total'),
                            'type'      : 'delta',
                            },
    
    'lockwaits'         : { 'help'      : 'Lockwaits / Sec',
                            'stdout'    : 'Lockwaits / Sec is %s/sec',
                            'label'     : 'lockwaits',
                            'query'     : INST_QUERY % ('Lock Waits/sec', '_Total'),
                            'type'      : 'delta',
                            },
    
    'lockwait'          : { 'help'      : 'Lock Wait Time (ms)',
                            'stdout'    : 'Lock Wait Time (ms) is %sms',
                            'label'     : 'lockwait',
                            'unit'      : 'ms',
                            'query'     : INST_QUERY % ('Lock Wait Time (ms)', '_Total'),
                            'type'      : 'standard',
                            },
    
    'averagewait'       : { 'help'      : 'Average Wait Time (ms)',
                            'stdout'    : 'Average Wait Time (ms) is %sms',
                            'label'     : 'averagewait',
                            'unit'      : 'ms',
                            'query'     : DIVI_QUERY % ('Average Wait Time', '_Total'),
                            'type'      : 'divide',
                            },
    
    'pagesplits'        : { 'help'      : 'Page Splits / Sec',
                            'stdout'    : 'Page Splits / Sec is %s/sec',
                            'label'     : 'page_splits',
                            'query'     : OBJE_QUERY % 'Page Splits/sec',
                            'type'      : 'delta',
                            },
    
    'cachehit'          : { 'help'      : 'Cache Hit Ratio',
                            'stdout'    : 'Cache Hit Ratio is %s%%',
                            'label'     : 'cache_hit_ratio',
                            'query'     : DIVI_QUERY % ('Cache Hit Ratio', '_Total'),
                            'type'      : 'divide',
                            'unit'      : '%',
                            'modifier'  : 100,
                            },
    
    'batchreq'          : { 'help'      : 'Batch Requests / Sec',
                            'stdout'    : 'Batch Requests / Sec is %s/sec',
                            'label'     : 'batch_requests',
                            'query'     : OBJE_QUERY % 'Batch Requests/sec',
                            'type'      : 'delta',
                            },
    
    'sqlcompilations'   : { 'help'      : 'SQL Compilations / Sec',
                            'stdout'    : 'SQL Compilations / Sec is %s/sec',
                            'label'     : 'sql_compilations',
                            'query'     : OBJE_QUERY % 'SQL Compilations/sec',
                            'type'      : 'delta',
                            },
    
    'fullscans'         : { 'help'      : 'Full Scans / Sec',
                            'stdout'    : 'Full Scans / Sec is %s/sec',
                            'label'     : 'full_scans',
                            'query'     : OBJE_QUERY % 'Full Scans/sec',
                            'type'      : 'delta',
                            },
    
    'pagelife'          : { 'help'      : 'Page Life Expectancy',
                            'stdout'    : 'Page Life Expectancy is %s/sec',
                            'label'     : 'page_life_expectancy',
                            'query'     : OBJE_QUERY % 'Page life expectancy',
                            'type'      : 'standard'
                            },
    
    #~ 'debug'             : { 'help'      : 'Used as a debugging tool.',
                            #~ 'stdout'    : 'Debugging: ',
                            #~ 'label'     : 'debug',
                            #~ 'query'     : DIVI_QUERY % ('Average Wait Time', '_Total'),
                            #~ 'type'      : 'divide' 
                            #~ },
    
    'time2connect'      : { 'help'      : 'Time to connect to the database.' },
    
    'test'              : { 'help'      : 'Run tests of all queries against the database.' },

}

def return_nagios(options, stdout='', result='', unit='', label=''):
    if is_within_range(options.critical, result):
        prefix = 'CRITICAL: '
        code = 2
    elif is_within_range(options.warning, result):
        prefix = 'WARNING: '
        code = 1
    else:
        prefix = 'OK: '
        code = 0
    strresult = str(result)
    try:
        stdout = stdout % (strresult)
    except TypeError, e:
        pass
    stdout = '%s%s|%s=%s%s;%s;%s;;' % (prefix, stdout, label, strresult, unit, options.warning or '', options.critical or '')
    raise NagiosReturn(stdout, code)

class NagiosReturn(Exception):
    
    def __init__(self, message, code):
        self.message = message
        self.code = code

class MSSQLQuery(object):
    
    def __init__(self, query, options, label='', unit='', stdout='', host='', modifier=1, *args, **kwargs):
        self.query = query
        self.label = label
        self.unit = unit
        self.stdout = stdout
        self.options = options
        self.host = host
        self.modifier = modifier
    
    def run_on_connection(self, connection):
        cur = connection.cursor()
        cur.execute(self.query)
        self.query_result = cur.fetchone()[0]
    
    def finish(self):
        return_nagios(  self.options,
                        self.stdout,
                        self.result,
                        self.unit,
                        self.label )
    
    def calculate_result(self):
        self.result = float(self.query_result) * self.modifier
    
    def do(self, connection):
        self.run_on_connection(connection)
        self.calculate_result()
        self.finish()

class MSSQLDivideQuery(MSSQLQuery):
    
    def __init__(self, *args, **kwargs):
        super(MSSQLDivideQuery, self).__init__(*args, **kwargs)
    
    def calculate_result(self):
        if self.query_result[1] != 0:
            self.result = (float(self.query_result[0]) / self.query_result[1]) * self.modifier
        else:
            self.result = float(self.query_result[0]) * self.modifier
    
    def run_on_connection(self, connection):
        cur = connection.cursor()
        cur.execute(self.query)
        self.query_result = [x[0] for x in cur.fetchall()]

class MSSQLDeltaQuery(MSSQLQuery):
    
    def make_pickle_name(self):
        tmpdir = tempfile.gettempdir()
        tmpname = hash(self.host + self.query)
        self.picklename = '%s/mssql-%s.tmp' % (tmpdir, tmpname)
    
    def calculate_result(self):
        self.make_pickle_name()
        
        try:
            tmpfile = open(self.picklename)
        except IOError:
            tmpfile = open(self.picklename, 'w')
            tmpfile.close()
            tmpfile = open(self.picklename)
        try:
            try:
                last_run = pickle.load(tmpfile)
            except EOFError, ValueError:
                last_run = { 'time' : None, 'value' : None }
        finally:
            tmpfile.close()
        
        if last_run['time']:
            old_time = last_run['time']
            new_time = time.time()
            old_val  = last_run['query_result']
            new_val  = self.query_result
            self.result = ((new_val - old_val) / (new_time - old_time)) * self.modifier
        else:
            self.result = None
        
        new_run = { 'time' : time.time(), 'query_result' : self.query_result }
        
        #~ Will throw IOError, leaving it to aquiesce
        tmpfile = open(self.picklename, 'w')
        pickle.dump(new_run, tmpfile)
        tmpfile.close()

def parse_args():
    usage = "usage: %prog -H hostname -U user -P password -T table --mode"
    parser = OptionParser(usage=usage)
    
    required = OptionGroup(parser, "Required Options")
    required.add_option('-H' , '--hostname', help='Specify MSSQL Server Address', default=None)
    required.add_option('-U' , '--user', help='Specify MSSQL User Name', default=None)
    required.add_option('-P' , '--password', help='Specify MSSQL Password', default=None)
    parser.add_option_group(required)
    
    connection = OptionGroup(parser, "Optional Connection Information")
    connection.add_option('-I', '--instance', help='Specify instance', default=None)
    connection.add_option('-p', '--port', help='Specify port.', default=None)
    parser.add_option_group(connection)
    
    nagios = OptionGroup(parser, "Nagios Plugin Information")
    nagios.add_option('-w', '--warning', help='Specify warning range.', default=None)
    nagios.add_option('-c', '--critical', help='Specify critical range.', default=None)
    parser.add_option_group(nagios)
    
    mode = OptionGroup(parser, "Mode Options")
    global MODES
    for k, v in zip(MODES.keys(), MODES.values()):
        mode.add_option('--%s' % k, action="store_true", help=v.get('help'), default=False)
    parser.add_option_group(mode)
    options, _ = parser.parse_args()
    
    if not options.hostname:
        parser.error('Hostname is a required option.')
    if not options.user:
        parser.error('User is a required option.')
    if not options.password:
        parser.error('Password is a required option.')
    
    if options.instance and options.port:
        parser.error('Cannot specify both instance and port.')
    
    options.mode = None
    for arg in mode.option_list:
        if getattr(options, arg.dest) and options.mode:
            parser.error("Must choose one and only Mode Option.")
        elif getattr(options, arg.dest):
            options.mode = arg.dest
    
    return options

def is_within_range(nagstring, value):
    if not nagstring:
        return False
    import re
    import operator
    first_float = r'(?P<first>(-?[0-9]+(\.[0-9]+)?))'
    second_float= r'(?P<second>(-?[0-9]+(\.[0-9]+)?))'
    actions = [ (r'^%s$' % first_float,lambda y: (value > float(y.group('first'))) or (value < 0)),
                (r'^%s:$' % first_float,lambda y: value < float(y.group('first'))),
                (r'^~:%s$' % first_float,lambda y: value > float(y.group('first'))),
                (r'^%s:%s$' % (first_float,second_float), lambda y: (value < float(y.group('first'))) or (value > float(y.group('second')))),
                (r'^@%s:%s$' % (first_float,second_float), lambda y: not((value < float(y.group('first'))) or (value > float(y.group('second')))))]
    for regstr,func in actions:
        res = re.match(regstr,nagstring)
        if res: 
            return func(res)
    raise Exception('Improper warning/critical format.')

def connect_db(options):
    host = options.hostname
    if options.instance:
        host += "\\" + options.instance
    elif options.port:
        host += ":" + options.port
    start = time.time()
    mssql = pymssql.connect(host = host, user = options.user, password = options.password, database='master')
    total = time.time() - start
    return mssql, total, host

def main():
    options = parse_args()
    
    mssql, total, host = connect_db(options)
    
    if options.mode =='test':
        run_tests(mssql, options, host)
        
    elif not options.mode or options.mode == 'time2connect':
        return_nagios(  options,
                        stdout='Time to connect was %ss',
                        label='time',
                        unit='s',
                        result=total )
                        
    else:
        execute_query(mssql, options, host)

def execute_query(mssql, options, host=''):
    sql_query = MODES[options.mode]
    sql_query['options'] = options
    sql_query['host'] = host
    query_type = sql_query.get('type')
    if query_type == 'delta':
        mssql_query = MSSQLDeltaQuery(**sql_query)
    elif query_type == 'divide':
        mssql_query = MSSQLDivideQuery(**sql_query)
    else:
        mssql_query = MSSQLQuery(**sql_query)
    mssql_query.do(mssql)

def run_tests(mssql, options, host):
    failed = 0
    total  = 0
    del MODES['time2connect']
    del MODES['test']
    for mode in MODES.keys():
        total += 1
        options.mode = mode
        try:
            execute_query(mssql, options, host)
        except NagiosReturn:
            print "%s passed!" % mode
        except Exception, e:
            failed += 1
            print "%s failed with: %s" % (mode, e)
    print '%d/%d tests failed.' % (failed, total)
    
if __name__ == '__main__':
    try:
        main()
    except pymssql.OperationalError, e:
        print e
        sys.exit(3)
    except pymssql.InterfaceError, e:
        print e
        sys.exit(3)
    except IOError, e:
        print e
        sys.exit(3)
    except NagiosReturn, e:
        print e.message
        sys.exit(e.code)
    #~ except Exception, e:
        #~ print "Caught unexpected error. This could be caused by your sysperfinfo not containing the proper entries for this query, and you may delete this service check."
        #~ sys.exit(3)
