# Spark-Bitnami

> This is to test spark-sql library in local device a.k.a : Under Docker using docker compose

## Notes

- Bind App/Mian.py to container using docker compose volume or we can copy it manually to container
- docker cp -L ./App/Main.py spark-master:/opt/bitnami/spark/scripts/Main.py
- localhost:8080 for spark app UI
- localhost:4040 for spark running monitoring UI
- localhost:8888 for jupyterlab testing purposes without sumbit spark standalone
- run_spark.sh

## Development Stack

- Docker Dekstop
- Visual Sudio Code
- Dockerfile & docker-compose.yml
- Image as spark application connected to visual studio code

## Docker Command

- docker-compose up --build
- docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/scripts/Main.py
- docker exec -it spark-master /bin/bash >>> connect to specific container
- docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/scripts/Main.py > ./logs/app1.log 2>&1
- docker exec -it spark-master /bin/bash -c 'spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/scripts/Dqc.py'
- docker exec -it -d >> -d means running from background of docker but cannot write log into local >> applicable only in docker dir
- docker exec -d spark-master /bin/bash -c '/mnt/apps/run_spark.sh --sparkname shtest --jobname shtest' >>> final by using arguments

## Container Access

- ctrl+p shortcut for command pallete
- remote container >>> Remote-Containers: Reopen in Container
- return back to local >>> Remote-Containers: Reopen Folder Locally

## WSL Installation

- Powershell
- wsl -l -v >> after install docker desktop
- install ubuntu for linux base purpose while running apps in containers docker.sock etc
- wsl --instal -d Ubuntu
- default user : development pass : development
- Enable Ubuntu toggle in docker desktop
- Enable daemon for Airflow purposes