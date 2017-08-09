Server Changelog
================

## 2.1.0 
 * Added server cpu usage, memory usage, and connection counters (campenberger)

## 2.0.3
 * Remove misleading description of lockwait, removing the word Average (SW)

## 2.0.2 
 * Fixed issues where the SQL cache hit queries were yielding improper results when done on large systems (CTrahan)

## 2.0.1 
 * Fixed try/finally statement to accomodate Python 2.4 for legacy systems (NS)

## 2.0.0 
 * Complete rewrite of the structure, re-evaluated some queries to hopefully make them more portable (CFriese)
 * Updated the way averages are taken, no longer needs tempdb access (NS)

## 1.2.0 
 * Added ability to specify instances (NS)

## 1.1.0 
 * Fixed port bug allowing for non default ports (CBTSDon)
 * Added batchreq, sqlcompilations, fullscans, pagelife (Thanks mike from austria)
 * Added mode error checking which caused non-graceful exit (Thanks mike from austria)

## 1.0.2 
 * Fixed Uptime Counter to be based off of database (NS)
 * Fixed divide by zero


Database Changelog
==================

## 2.1.1
 * Fixed range values that were inverse (JO)

## 2.1.0
 * Updated release with spelling fixes

## 2.0.1 
 * Fixed bug where temp file was named same as other for host and numbers were coming back bogus. (NS)

## 2.0.0 
 * Complete Revamp/Rewrite based on the server version of this plugin (NS)

## 1.3.0 
 * Added ability specify MSSQL instances (NS)

## 1.2.0 
 * Added ability to monitor instances (NS)
 * Added check to see if pymssql is installed (NS)

## 1.1.0 
 * Fixed port bug allowing for non default ports (CBTSDon)
 * Added mode error checking which caused non-graceful exit (Thanks mike from austria)
