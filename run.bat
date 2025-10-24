@echo off
REM Run HopitalSparkSQL with proper JVM arguments for Java 21 compatibility

cd /d "%~dp0"

REM Build the project first
echo Building project...
mvn clean package -DskipTests

REM Run the application with proper JVM arguments
echo Running application...
java ^
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED ^
  --add-opens=java.base/java.lang=ALL-UNNAMED ^
  -cp target\SparkSQL-1.0-SNAPSHOT.jar ^
  com.example.sparksql.HopitalSparkSQL

pause

