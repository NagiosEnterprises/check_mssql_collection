#!/usr/bin/env python

########################################################################
# check_mssql_database - A Nagios plugin to check Microsoft SQL Server
# Copyright (C) 2017 Nagios Enterprises
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#
################### check_mssql_database.py ############################
# Version    : 2.1.2
# Date       : 06/18/2019
# Maintainer : Nagios Enterprises, LLC
# License    : GPLv2 (LICENSE.md / https://www.gnu.org/licenses/old-licenses/gpl-2.0.html)
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

BASE_QUERY = "SELECT cntr_value FROM sys.sysperfinfo WHERE counter_name='%s' AND instance_name='%%s';"
DIVI_QUERY = "SELECT cntr_value FROM sys.sysperfinfo WHERE counter_name LIKE '%s%%%%' AND instance_name='%%s';"

MODES = {
    
    'logcachehit'       : { 'help'      : 'Log Cache Hit Ratio',
                            'stdout'    : 'Log Cache Hit Ratio is %s%%',
                            'label'     : 'log_cache_hit_ratio',
                            'unit'      : '%',
                            'query'     : DIVI_QUERY % 'Log Cache Hit Ratio',
                            'type'      : 'divide',
                            'modifier'  : 100,
                            },
    
    'activetrans'       : { 'help'      : 'Active Transactions',
                            'stdout'    : 'Active Transactions is %s',
                            'label'     : 'log_file_usage',
                            'unit'      : '',
                            'query'     : BASE_QUERY % 'Active Transactions',
                            'type'      : 'standard',
                            },
    
    'logflushes'         : { 'help'     : 'Log Flushes Per Second',
                            'stdout'    : 'Log Flushes Per Second is %s/sec',
                            'label'     : 'log_flushes_per_sec',
                            'query'     : BASE_QUERY % 'Log Flushes/sec',
                            'type'      : 'delta'
                            },
    
    'logfileusage'      : { 'help'      : 'Log File Usage',
                            'stdout'    : 'Log File Usage is %s%%',
                            'label'     : 'log_file_usage',
                            'unit'      : '%',
                            'query'     : BASE_QUERY % 'Percent Log Used',
                            'type'      : 'standard',
                            },
    
    'transpsec'         : { 'help'      : 'Transactions Per Second',
                            'stdout'    : 'Transactions Per Second is %s/sec',
                            'label'     : 'transactions_per_sec',
                            'query'     : BASE_QUERY % 'Transactions/sec',
                            'type'      : 'delta'
                            },
    
    'loggrowths'        : { 'help'      : 'Log Growths',
                            'stdout'    : 'Log Growths is %s',
                            'label'     : 'log_growths',
                            'query'     : BASE_QUERY % 'Log Growths',
                            'type'      : 'standard'
                            },
    
    'logshrinks'        : { 'help'      : 'Log Shrinks',
                            'stdout'    : 'Log Shrinks is %s',
                            'label'     : 'log_shrinks',
                            'query'     : BASE_QUERY % 'Log Shrinks',
                            'type'      : 'standard'
                            },
    
    'logtruncs'         : { 'help'      : 'Log Truncations',
                            'stdout'    : 'Log Truncations is %s',
                            'label'     : 'log_truncations',
                            'query'     : BASE_QUERY % 'Log Truncations',
                            'type'      : 'standard'
                            },
    
    'logwait'           : { 'help'      : 'Log Flush Wait Time',
                            'stdout'    : 'Log Flush Wait Time is %sms',
                            'label'     : 'log_wait_time',
                            'unit'      : 'ms',
                            'query'     : BASE_QUERY % 'Log Flush Wait Time',
                            'type'      : 'standard'
                            },
    
    'datasize'          : { 'help'      : 'Database Size',
                            'stdout'    : 'Database size is %sKB',
                            'label'     : 'KB',
                            'query'     : BASE_QUERY % 'Data File(s) Size (KB)',
                            'type'      : 'standard'
                            },
    
    'time2connect'      : { 'help'      : 'Time to connect to the database.' },
    
    'test'              : { 'help'      : 'Run tests of all queries against the database.' },
}

def return_nagios(options, stdout='', result='', unit='', label=''):
    invert = False
    w = options.warning
    c = options.critical

    # Check for ranges
    if ':' in options.critical and ':' in options.warning:
        c = options.critical.split(':')[1]
        w = options.warning.split(':')[1]

    # Check if we should invert the warning/critical (this should change someday)
    if int(c) < int(w):
        invert = True

    if is_within_range(options.critical, result, invert):
        prefix = 'CRITICAL: '
        code = 2
    elif is_within_range(options.warning, result, invert):
        prefix = 'WARNING: '
        code = 1
    else:
        prefix = 'OK: '
        code = 0

    strresult = str(result)
    stdout = stdout % (strresult)
    stdout = '%s%s|%s=%s%s;%s;%s;;' % (prefix, stdout, label, strresult, unit, options.warning or '', options.critical or '')
    raise NagiosReturn(stdout, code)

class NagiosReturn(Exception):
    
    def __init__(self, message, code):
        self.message = message
        self.code = code

class MSSQLQuery(object):
    
    def __init__(self, query, options, label='', unit='', stdout='', host='', modifier=1, *args, **kwargs):
        self.query = query % options.table
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
    
    def calculate_result(self):
        if self.query_result[1] == 0:
            self.result = 0
        else:
            self.result = (float(self.query_result[0]) / self.query_result[1]) * self.modifier
    
    def run_on_connection(self, connection):
        cur = connection.cursor()
        cur.execute(self.query)
        self.query_result = [x[0] for x in cur.fetchall()]

class MSSQLDeltaQuery(MSSQLQuery):
    
    def make_pickle_name(self):
        tmpdir = tempfile.gettempdir()
        tmpname = hash(self.host + self.options.table + self.query)
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
            except EOFError as ValueError:
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
            self.result = 0
        
        new_run = { 'time' : time.time(), 'query_result' : self.query_result }
        
        #~ Will throw IOError, leaving it to acquiesce
        tmpfile = open(self.picklename, 'w')
        pickle.dump(new_run, tmpfile)
        tmpfile.close()

def is_within_range(nagstring, value, invert = False):
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
            if invert:
                return not func(res)
            else:
                return func(res)
    raise Exception('Improper warning/critical format.')

def parse_args():
    usage = "usage: %prog -H hostname -U user -P password -T table --mode"
    parser = OptionParser(usage=usage)
    
    required = OptionGroup(parser, "Required Options")
    required.add_option('-H', '--hostname', help='Specify MSSQL Server Address', default=None)
    required.add_option('-U', '--user', help='Specify MSSQL User Name', default=None)
    required.add_option('-P', '--password', help='Specify MSSQL Password', default=None)
    required.add_option('-T', '--table', help='Specify the table to check', default=None) 
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
    for k, v in zip(list(MODES.keys()), list(MODES.values())):
        mode.add_option('--%s' % k, action="store_true", help=v.get('help'), default=False)
    parser.add_option_group(mode)
    options, _ = parser.parse_args()
    
    if not options.hostname:
        parser.error('Hostname is a required option.')
    if not options.user:
        parser.error('User is a required option.')
    if not options.password:
        parser.error('Password is a required option.')
    if not options.table:
        parser.error('Table is a required option.')
    
    if options.instance and options.port:
        parser.error('Cannot specify both instance and port.')
    
    options.mode = None
    for arg in mode.option_list:
        if getattr(options, arg.dest) and options.mode:
            parser.error("Must choose one and only Mode Option.")
        elif getattr(options, arg.dest):
            options.mode = arg.dest
    
    return options

def connect_db(options):
    host = options.hostname
    if options.instance:
        host += "\\" + options.instance
    elif options.port:
        host += ":" + options.port
    start = time.time()
    mssql = pymssql.connect(host = host, user = options.user, password = options.password, database=options.table)
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
    for mode in list(MODES.keys()):
        total += 1
        options.mode = mode
        try:
            execute_query(mssql, options, host)
        except NagiosReturn:
            print("%s passed!" % mode)
        except Exception as e:
            failed += 1
            print("%s failed with: %s" % (mode, e))
    print('%d/%d tests failed.' % (failed, total))
    
if __name__ == '__main__':
    try:
        main()
    except pymssql.OperationalError as e:
        print(e)
        sys.exit(3)
    except IOError as e:
        print(e)
        sys.exit(3)
    except NagiosReturn as e:
        print(e.message)
        sys.exit(e.code)
    except Exception as e:
        print(type(e))
        print("Caught unexpected error. This could be caused by your sysperfinfo not containing the proper entries for this query, and you may delete this service check.")
        sys.exit(3)
