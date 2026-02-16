@echo off
setlocal enabledelayedexpansion
title Quantum File Ingester - Spring Boot + Apache Camel
echo ========================================
echo   Quantum File Ingester
echo   Spring Boot 3.2.4 + Apache Camel 4.4.1
echo   Version: 2.0.0-SNAPSHOT
echo ========================================
echo.

set JAVA_HOME=C:\Program Files\Java\jdk-21
set MAVEN_HOME=E:\Projects\Java\Maven\apache-maven-3.8.6
set PATH=%JAVA_HOME%\bin;%MAVEN_HOME%\bin;%PATH%
set BACKEND_PORT=8081

cd /d %~dp0

echo [INFO] JAVA_HOME = %JAVA_HOME%
echo [INFO] MAVEN_HOME = %MAVEN_HOME%
echo.

:: ========================================
:: Verificar que quantum-common este instalado en Maven local
:: ========================================
echo [INFO] Verificando dependencia quantum-common...
if exist "%USERPROFILE%\.m2\repository\com\quantum\quantum-common\1.0.0\quantum-common-1.0.0.jar" (
    echo [OK] quantum-common 1.0.0 encontrado en Maven local.
) else (
    echo [WARN] quantum-common 1.0.0 NO encontrado en Maven local.
    echo [WARN] Ejecuta primero: cd quantum-common ^&^& start-backend.bat
    echo [WARN] Intentando continuar de todas formas...
)
echo.

:: ========================================
:: Verificar si el puerto esta en uso y matar el proceso
:: ========================================
echo [INFO] Verificando puerto %BACKEND_PORT%...
set "PORT_FREED=0"

for /f "tokens=5" %%a in ('netstat -aon 2^>nul ^| findstr ":%BACKEND_PORT% " ^| findstr "LISTENING"') do (
    echo [WARN] Puerto %BACKEND_PORT% en uso por PID: %%a
    echo [INFO] Terminando proceso PID %%a con PowerShell...
    powershell -Command "try { Stop-Process -Id %%a -Force -ErrorAction Stop; Write-Host '[OK] Proceso %%a terminado.' } catch { Write-Host '[WARN] PowerShell fallo, usando taskkill...'; cmd /c 'taskkill /T /F /PID %%a' 2>$null; if ($LASTEXITCODE -eq 0) { Write-Host '[OK] Proceso %%a terminado con taskkill.' } else { Write-Host '[ERROR] No se pudo terminar PID %%a.' } }"
    set "PORT_FREED=1"
)

if "!PORT_FREED!"=="1" (
    echo [INFO] Esperando liberacion del puerto...
    timeout /t 3 /nobreak >nul
)

:: Verificar que el puerto quedo libre
set "PORT_BUSY=0"
for /f "tokens=5" %%a in ('netstat -aon 2^>nul ^| findstr ":%BACKEND_PORT% " ^| findstr "LISTENING"') do (
    set "PORT_BUSY=1"
)
if "!PORT_BUSY!"=="1" (
    echo [WARN] Puerto aun en uso. Solicitando permisos de Administrador...
    powershell -Command "Start-Process powershell -Verb RunAs -Wait -ArgumentList '-Command', 'Get-NetTCPConnection -LocalPort %BACKEND_PORT% -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }'"
    timeout /t 3 /nobreak >nul

    set "PORT_STILL=0"
    for /f "tokens=5" %%b in ('netstat -aon 2^>nul ^| findstr ":%BACKEND_PORT% " ^| findstr "LISTENING"') do (
        set "PORT_STILL=1"
    )
    if "!PORT_STILL!"=="1" (
        echo [ERROR] No se pudo liberar el puerto %BACKEND_PORT%. Abortando.
        pause
        exit /b 1
    )
)
echo [OK] Puerto %BACKEND_PORT% disponible.
echo.

:: ========================================
:: Crear carpeta logs y generar nombre de archivo (ddmmyyhhmm.log)
:: ========================================
if not exist logs mkdir logs

set "DD=%date:~0,2%"
set "MM=%date:~3,2%"
set "YY=%date:~8,2%"
set "HH=%time:~0,2%"
set "MI=%time:~3,2%"
set "HH=!HH: =0!"
set "LOG_FILE=logs\!DD!!MM!!YY!!HH!!MI!.log"

for %%F in ("!LOG_FILE!") do set "LOG_FULL=%%~fF"

echo [INFO] Log: !LOG_FULL!
echo.
echo ========================================
echo   Puerto:     %BACKEND_PORT%
echo   Servidor:   http://localhost:%BACKEND_PORT%
echo   Health:     http://localhost:%BACKEND_PORT%/actuator/health
echo   Metrics:    http://localhost:%BACKEND_PORT%/actuator/prometheus
echo   Dependencia: quantum-common 1.0.0
echo ========================================
echo.
echo [INFO] Presione Ctrl+C para detener.
echo.

:: ========================================
:: Ejecutar Spring Boot con salida a log y consola
:: ========================================
echo ======================================== >> "!LOG_FULL!"
echo   Quantum File Ingester - Inicio: %date% %time% >> "!LOG_FULL!"
echo ======================================== >> "!LOG_FULL!"

set "PS_SCRIPT=%TEMP%\quantum-ingester-run.ps1"
(
    echo $ErrorActionPreference = 'Continue'
    echo $logFile = '!LOG_FULL!'
    echo Write-Host "[INFO] Ejecutando mvn spring-boot:run..."
    echo Write-Host "[INFO] Log: $logFile"
    echo ^& mvn spring-boot:run 2^>^&1 ^| ForEach-Object {
    echo     $line = $_
    echo     Write-Host $line
    echo     $line ^| Out-File -FilePath $logFile -Append -Encoding utf8
    echo }
) > "!PS_SCRIPT!"

powershell -NoProfile -ExecutionPolicy Bypass -File "!PS_SCRIPT!"

del "!PS_SCRIPT!" >nul 2>&1

echo.
echo [INFO] File Ingester detenido. Log guardado en !LOG_FULL!
pause
