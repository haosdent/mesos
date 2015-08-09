:: Licensed to the Apache Software Foundation (ASF) under one
:: or more contributor license agreements.  See the NOTICE file
:: distributed with this work for additional information
:: regarding copyright ownership.  The ASF licenses this file
:: to you under the Apache License, Version 2.0 (the
:: "License"); you may not use this file except in compliance
:: with the License.  You may obtain a copy of the License at
::
::     http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.

@echo off

:: Set Variables
set SOLUTION_FILE=%~1
:::: NEED_UPGRADE:
::::   Upgrade the solution file to match current Visual Studio or not.
set NEED_UPGRADE=%~2
:::: PROJECTS:
::::   Delimitered by '#'
set PROJECTS=%~3
set CONFIGURATION=Release
set PLATFORM=Win32
setlocal EnableDelayedExpansion

set PROG_FILES=%ProgramFiles%
if not "%ProgramFiles(x86)%" == "" (
  set PROG_FILES=%ProgramFiles(x86)%
)

:: Check if Visual Studio 2015 is installed
set MSVC_DIR="%PROG_FILES%)\Microsoft Visual Studio 14.0"
if exist %MSVC_DIR% (
   set COMPILER_VER="2015"
   goto setup_env
)

echo No compiler : Microsoft Visual Studio (2015) is not installed.
goto end

:setup_env

call %MSVC_DIR%\VC\vcvarsall.bat x86

:build

if "%NEED_UPGRADE%" == "TRUE" (
  devenv /upgrade %SOLUTION_FILE%
)

if not "%PROJECTS%" == "" (
  set PROJECTS_TARGET=/t:%PROJECTS:#=;%
)

msbuild ^
    %SOLUTION_FILE% %PROJECTS_TARGET% ^
    /p:Configuration=%CONFIGURATION%;Platform=%PLATFORM%

:end
exit /b