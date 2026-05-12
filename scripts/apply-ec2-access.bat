@echo off
setlocal

set "INPUT_IP=%~1"
if "%INPUT_IP%"=="" set "INPUT_IP=%MY_IP%"
if "%INPUT_IP%"=="" set "INPUT_IP=%PUBLIC_IP%"
if "%INPUT_IP%"=="" set "INPUT_IP=%TF_VAR_ssh_cidr%"

for /f "usebackq tokens=*" %%i in (`curl -fsS https://checkip.amazonaws.com 2^>nul`) do set "CURRENT_IP=%%i"
if "%INPUT_IP%"=="" (
  if not defined CURRENT_IP (
    echo Usage: %~nx0 ^<your-public-ip^>
    echo Or set MY_IP, PUBLIC_IP, or TF_VAR_ssh_cidr before running this script.
    exit /b 1
  )
  set "INPUT_IP=%CURRENT_IP%"
)

set "SSH_CIDR=%INPUT_IP%"
echo %INPUT_IP% | findstr /C:"/" >nul
if errorlevel 1 set "SSH_CIDR=%INPUT_IP%/32"

set "TF_VAR_ssh_cidr=%SSH_CIDR%"
echo TF_VAR_ssh_cidr=%TF_VAR_ssh_cidr%

terraform apply -auto-approve -target=aws_internet_gateway.igw -target=aws_route.public_default -target=aws_route_table_association.public_assoc -target=aws_security_group.ec2 -target=aws_instance.kafka_mm2 -target=aws_iam_user_policy.ec2_instance_connect
