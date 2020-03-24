@echo off
cd %~d0%~p0
call mvn clean install -Dmaven.test.skip
pause