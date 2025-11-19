@echo off
REM Kiểm tra tham số truyền vào

IF "%1"=="build" (
    echo Dang build cac image...
    docker build -t spark-jobs:latest ./spark-jobs
    docker build -t train-job:latest ./spark-jobs
    docker build -t fraud-train:latest ./fraud-train
    docker-compose up -d --build
    GOTO End
)

IF "%1"=="up" (
    echo Dang khoi dong container...
    docker-compose up -d
    GOTO End
)

IF "%1"=="down" (
    echo Dang tat he thong...
    docker-compose down -v
    GOTO End
)

echo Vui long nhap lenh: run.bat [build | up | down]

:End