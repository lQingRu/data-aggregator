@echo off
setlocal

set BASE_DIR=%~dp0
set WRAPPER_PROPERTIES=%BASE_DIR%\.mvn\wrapper\maven-wrapper.properties

for /f "tokens=2 delims==" %%i in ('findstr /b "distributionUrl=" "%WRAPPER_PROPERTIES%"') do set DIST_URL=%%i
for %%i in ("%DIST_URL%") do set DIST_FILE=%%~nxi
set DIST_NAME=%DIST_FILE:-bin.tar.gz=%

if "%MAVEN_USER_HOME%"=="" set MAVEN_USER_HOME=%USERPROFILE%\.m2
set DIST_DIR=%MAVEN_USER_HOME%\wrapper\dists\%DIST_NAME%
set MAVEN_BIN=%DIST_DIR%\%DIST_NAME%\bin\mvn.cmd

if not exist "%MAVEN_BIN%" (
  mkdir "%DIST_DIR%" 2>nul
  powershell -NoProfile -ExecutionPolicy Bypass -Command "Invoke-WebRequest -Uri '%DIST_URL%' -OutFile '%DIST_DIR%\%DIST_NAME%-bin.tar.gz'"
  powershell -NoProfile -ExecutionPolicy Bypass -Command "tar -xzf '%DIST_DIR%\%DIST_NAME%-bin.tar.gz' -C '%DIST_DIR%'"
)

call "%MAVEN_BIN%" %*
