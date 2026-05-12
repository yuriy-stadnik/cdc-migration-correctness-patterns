@echo off
setlocal

set "INPUT_IP=%~1"
if "%INPUT_IP%"=="" set "INPUT_IP=%MY_IP%"
if "%INPUT_IP%"=="" set "INPUT_IP=%PUBLIC_IP%"
if "%INPUT_IP%"=="" set "INPUT_IP=%TF_VAR_ssh_cidr%"

set "AWS_REGION=%~2"
if "%AWS_REGION%"=="" set "AWS_REGION=us-east-1"

set "KEY_PATH=%~3"
if "%KEY_PATH%"=="" set "KEY_PATH=%USERPROFILE%\.ssh\temp_ec2_key"

for /f "usebackq tokens=*" %%i in (`curl -fsS https://checkip.amazonaws.com 2^>nul`) do set "CURRENT_IP=%%i"
if "%INPUT_IP%"=="" (
  if not defined CURRENT_IP (
    echo Usage: %~nx0 ^<your-public-ip^> [aws-region] [ssh-key-path]
    exit /b 1
  )
  set "INPUT_IP=%CURRENT_IP%"
)

if defined CURRENT_IP (
  if not "%INPUT_IP%"=="%CURRENT_IP%" (
    if not "%INPUT_IP%"=="%CURRENT_IP%/32" (
      echo Warning: current outbound IPv4 appears to be %CURRENT_IP%, but SSH CIDR was set from %INPUT_IP%.
      echo If SSH times out, rerun with: %~nx0 %CURRENT_IP% %AWS_REGION% "%KEY_PATH%"
    )
  )
)

set "SSH_CIDR=%INPUT_IP%"
echo %INPUT_IP% | findstr /C:"/" >nul
if errorlevel 1 set "SSH_CIDR=%INPUT_IP%/32"
set "TF_VAR_ssh_cidr=%SSH_CIDR%"

if not exist "%USERPROFILE%\.ssh" mkdir "%USERPROFILE%\.ssh"

if not exist "%KEY_PATH%" (
  ssh-keygen -t ed25519 -f "%KEY_PATH%" -N "" -C "temp-ec2-instance-connect"
  if errorlevel 1 exit /b 1
)

terraform apply -auto-approve -target=aws_internet_gateway.igw -target=aws_route.public_default -target=aws_route_table_association.public_assoc -target=aws_security_group.ec2 -target=aws_instance.kafka_mm2 -target=aws_iam_user_policy.ec2_instance_connect
if errorlevel 1 exit /b 1

for /f "usebackq tokens=*" %%i in (`terraform output -raw ec2_instance_id`) do set "INSTANCE_ID=%%i"
for /f "usebackq tokens=*" %%i in (`terraform output -raw ec2_public_ip`) do set "INSTANCE_PUBLIC_IP=%%i"
for /f "usebackq tokens=*" %%i in (`terraform output -raw sg_ec2_id`) do set "EC2_SECURITY_GROUP_ID=%%i"

echo EC2 instance: %INSTANCE_ID%
echo EC2 public IP: %INSTANCE_PUBLIC_IP%
echo EC2 security group: %EC2_SECURITY_GROUP_ID%
echo Allowed SSH CIDR: %SSH_CIDR%

for /l %%a in (1,1,6) do (
  aws ec2-instance-connect send-ssh-public-key ^
    --region "%AWS_REGION%" ^
    --instance-id "%INSTANCE_ID%" ^
    --instance-os-user ec2-user ^
    --ssh-public-key "file://%KEY_PATH%.pub"

  if not errorlevel 1 goto ssh_connect
  if %%a==6 (
    echo Failed to send SSH public key after waiting for IAM policy propagation.
    exit /b 1
  )

  echo EC2 Instance Connect is not authorized yet. Waiting for IAM policy propagation...
  timeout /t 10 /nobreak >nul
)

:ssh_connect
ssh -o StrictHostKeyChecking=accept-new -i "%KEY_PATH%" ec2-user@%INSTANCE_PUBLIC_IP%
