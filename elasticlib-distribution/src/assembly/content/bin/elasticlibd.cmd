@echo off
rem Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
rem
rem Licensed under the Apache License, Version 2.0 (the "License");
rem you may not use this file except in compliance with the License.
rem You may obtain a copy of the License at
rem
rem      http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.

setlocal

TITLE ElasticLib Node

for %%A in (%~dp0\..) do set root=%%~fA
set elasticlib_home=%root%\node\home
if not "%2" == "" set elasticlib_home=%~f2
set name=ElasticLib-node-service
set classpath=%root%\node\*;%root%\lib\*
set main_class=org.elasticlib.node.App
set log_opts=--LogPath "%elasticlib_home%\logs" --LogPrefix "%name%" --StdError NUL --StdOutput NUL

rem Detect Arch
if NOT DEFINED JAVA_HOME (
echo JAVA_HOME environment variable is not defined!
exit /b 1
)

"%JAVA_HOME%\bin\java" -version 2>&1 | find "64-Bit" >nul:

if errorlevel 1 goto x86
set executable=%root%\bin\elasticlibd-x64.exe
goto selectCommand

:x86
set executable=%root%\bin\elasticlibd-x86.exe

:selectCommand
if ""%1"" == ""run"" goto doRun
if ""%1"" == ""install"" goto doInstall
if ""%1"" == ""remove"" goto doRemove
if ""%1"" == ""manage"" goto doManage
if ""%1"" == ""start"" goto doStart
if ""%1"" == ""stop"" goto doStop

echo Usage: elasticlibd ^<install^|remove^|manage^|start^|stop^|run^> ^[elasticlib_home^]
exit /b 1

:doRun
java -classpath "%classpath%" ^
     -Delasticlib.home="%elasticlib_home%" ^
     -Dlogback.configurationFile="%elasticlib_home%\logback.xml" ^
     %main_class% ^
     "%elasticlib_home%"
exit /b 0

:doInstall
"%executable%" //IS//%name% ^
    --DisplayName "ElasticLib" ^
    --Description "ElasticLib node service" ^
    --Startup manual ^
    --StartClass %main_class% ^
    --StopClass %main_class% ^
    --StartMethod main ^
    --StopMethod close ^
    ++JvmOptions -Delasticlib.home="%elasticlib_home%" ^
    ++JvmOptions -Dlogback.configurationFile="%elasticlib_home%\logback.xml" ^
    ++StartParams "%elasticlib_home%" ^
    --Classpath "%classpath%" ^
    --StartPath "%elasticlib_home%" ^
    --Jvm auto ^
    --StartMode jvm ^
    --StopMode jvm ^
    %log_opts%

if ERRORLEVEL 1 exit /b 1
echo Elasticlib node installed as a service
exit /b 0

:doRemove
"%executable%" //DS//%name% %log_opts%
if ERRORLEVEL 1 exit /b 1
echo Elasticlib node service removed
exit /b 0

:doManage
"%root%\bin\elasticlibd-mgr.exe" //ES//%name%
if ERRORLEVEL 1 exit /b 1
exit /b 0

:doStart
"%executable%" //ES//%name% %log_opts%
if ERRORLEVEL 1 exit /b 1
echo Elasticlib node started
exit /b 0

:doStop
"%executable%" //SS//%name% %log_opts%
if ERRORLEVEL 1 exit /b 1
echo Elasticlib node stopped
exit /b 0
