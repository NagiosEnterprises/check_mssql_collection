check_mssql_collection
======================

check_mssql_collection is a set of Nagios plugins for checking the status of a
Microsoft SQL Server.

Installation
------------

A modified version of these plugins come installed by default on Nagios XI.

If you're using Nagios Core, do the following:
0. Make sure python, epel, pip, and pymssql are installed.

1. Download this project as a .zip onto your Core installation.

```
cd /tmp
wget https://github.com/NagiosEnterprises/check_mssql_collection/archive/master.zip
```
2. Unzip the project
```
unzip master.zip
```
3. Transfer the python scripts to the /usr/local/nagios/libexec/ directory
```
mv check_mssql_connection-master/*.py /usr/local/nagios/libexec/
```
4. Set the user 'nagios' as owner
```
chown nagios.nagios /usr/local/nagios/libexec/*.py 
```
5. Configure the commands.cfg file to add commands using those plugins.
```
nano /usr/local/nagios/etc/commands.cfg
```
You will need to experiment with the usage of these scripts to determine which
commands will suit your Core installaion best. Here is an example command to
test connectivity to your MSSQL database:
```
/usr/local/nagios/libexec/check_mssql_database.py -H xxx.xxx.xxx.xxx -U user -P passwd -T tablename --time2connect -w 1 -c 5000
```
If you fill in your hostname, username, and password, and choose a table in the
database, this check should almost always return with a warning.

6. Restart the nagios service.
```
service nagios restart
```
Changes
-------

Changes can be seen at the CHANGELOG file.

Contributors
------------

Please see the file at CONTRIBUTORS for a full list of authors, contributors,
and maintainers.

Current Version
----------------------

The current version of these scripts can be found at:

  https://github.com/NagiosEnterprises/check_mssql_collection/

Other open-source Nagios software can be found at:

  https://github.com/NagiosEnterprises/

License
-------

These plugins are released under GPLv3. See the full license at the LICENSE 
file.

Questions?
----------

If you have questions about these plugins, or encounter problems getting things
working along the way, your best bet for an answer or quick resolution is to check the
[Nagios Support Forums](https://support.nagios.com/forum/viewforum.php?f=5).
