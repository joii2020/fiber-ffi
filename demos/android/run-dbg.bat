@echo off
setlocal

call .\gradlew.bat clean assembleDebug
if errorlevel 1 exit /b %errorlevel%

adb install -r app\build\outputs\apk\debug\app-debug.apk
if errorlevel 1 exit /b %errorlevel%

adb shell am force-stop com.example.fiberdemo
adb shell am start -n com.example.fiberdemo/.MainActivity
