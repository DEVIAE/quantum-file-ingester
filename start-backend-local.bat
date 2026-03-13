@echo off
title Quantum File Ingester - LOCAL
echo ============================================
echo   Quantum File Ingester - Modo LOCAL
echo   Broker : format-normalizer tcp://localhost:61616
echo   Input  : E:\data\normalized (JSONL)
echo ============================================
echo.
set JAVA_HOME=C:\Program Files\Java\jdk-21
set MAVEN_HOME=E:\Projects\Java\Maven\apache-maven-3.8.6
set PATH=%JAVA_HOME%\bin;%MAVEN_HOME%\bin;%PATH%
set BACKEND_PORT=8081

cd /d %~dp0

echo [INFO] Creando directorios de datos...
call :mkdir_if_missing E:\data\normalized
call :mkdir_if_missing E:\data\processed
call :mkdir_if_missing E:\data\failed
call :mkdir_if_missing E:\data\output
echo.

echo [INFO] Verificando quantum-common 1.1.0...
if exist "%USERPROFILE%\.m2\repository\com\quantum\quantum-common\1.1.0\quantum-common-1.1.0.jar" (
    echo [OK] quantum-common 1.1.0 encontrado.
) else (
    echo [ERROR] quantum-common 1.1.0 NO encontrado. Ejecuta primero: cd quantum-common ^&^& start-backend.bat
    pause
    exit /b 1
)
echo.

echo [INFO] Verificando que format-normalizer este corriendo (broker en :61616)...
powershell -NoProfile -Command "try { $t = New-Object Net.Sockets.TcpClient; $t.Connect('localhost',61616); $t.Close(); Write-Host '[OK] Broker detectado en :61616' } catch { Write-Host '[WARN] Broker no detectado - asegurate de iniciar format-normalizer primero' }"
echo.

echo [INFO] Verificando puerto %BACKEND_PORT%...
for /f "tokens=5" %%a in ('netstat -ano 2^>nul ^| findstr /C:":%BACKEND_PORT% "') do (
    echo [WARN] Puerto %BACKEND_PORT% en uso por PID %%a, liberando...
    taskkill /T /F /PID %%a >nul 2^>^&1
)
echo.

if not exist "logs" mkdir logs
set LOGTS=%DATE:~6,4%%DATE:~3,2%%DATE:~0,2%%TIME:~0,2%%TIME:~3,2%
set LOGTS=%LOGTS: =0%
set LOG_FILE=logs\%LOGTS%.log

echo [INFO] Arrancando quantum-file-ingester con perfil LOCAL...
echo [INFO] Puerto  : %BACKEND_PORT%
echo [INFO] Input   : E:\data\normalized  (JSONL del normalizer)
echo [INFO] Broker  : tcp://localhost:61616  (embebido en format-normalizer)
echo [INFO] ELK     : DESHABILITADO
echo [INFO] Log     : %LOG_FILE%
echo.
echo [READY] Health: http://localhost:%BACKEND_PORT%/actuator/health
echo.

mvn spring-boot:run -Dspring-boot.run.profiles=local 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%LOG_FILE%'"
goto :eof

:mkdir_if_missing
if not exist "%~1" (
    mkdir "%~1"
    echo [OK] Creado: %~1
) else (
    echo [OK] Ya existe: %~1
)
goto :eof
