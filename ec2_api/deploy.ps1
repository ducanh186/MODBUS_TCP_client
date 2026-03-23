# ==============================================================================
# deploy.ps1  -  Deploy ec2_api Flask app to EC2 via AWS SSM
#
# Uses AWS Systems Manager (SSM) instead of SSH - NO .pem key needed,
# NO port 22, the EC2 instance just needs SSM Agent + IAM role.
#
# Prerequisites:
#   1. AWS CLI v2 configured locally: aws configure
#   2. EC2 instance has IAM Instance Profile with these policies:
#      - AmazonSSMManagedInstanceCore  (SSM agent)
#      - AmazonS3ReadOnlyAccess        (to download deploy package)
#      - AmazonSSMReadOnlyAccess       (to read secrets from SSM)
#   3. SSM Agent running on EC2 (pre-installed on Amazon Linux 2023)
#   4. S3 bucket for deploy artifacts (reuse existing or create one)
#   5. Security Group: inbound TCP on $API_PORT from your IP
#
# Usage:
#   cd "d:\CODE\MODBUS TCP\MODBUS_TCP_client\ec2_api"
#   .\deploy.ps1
# ==============================================================================

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ==============================================================================
# SECTION 1 - CONFIG  (edit before first deploy)
# ==============================================================================
$INSTANCE_ID = "i-02805ff53bc331db6"       # EC2 Instance ID (from AWS Console)
$EC2_USER    = "ec2-user"                   # Amazon Linux = ec2-user | Ubuntu = ubuntu
$REGION      = "ap-northeast-1"
$DEPLOY_BUCKET = "al-lab-s3-20260226-1772180861"  # S3 bucket to stage deploy zip
$DEPLOY_KEY    = "deploy/ec2_api.tar.gz"          # S3 key for the archive

$REMOTE_DIR = "/home/$EC2_USER/ec2_api"
$SERVICE    = "ec2_api"
$API_PORT   = "8000"
$WORKERS    = "2"

# Non-secret environment variables for the systemd unit.
# Secrets (DB_PASS, SERVER_SECRET) are loaded from SSM Parameter Store at runtime.
$ENV_VARS = [ordered]@{
    AWS_REGION = "ap-northeast-1"
    S3_BUCKET  = "al-lab-s3-20260226-1772180861"
    DB_HOST    = "al-pg-lab.cncesm4eswfg.ap-northeast-1.rds.amazonaws.com"
    DB_PORT    = "5432"
    DB_NAME    = "postgres"
    DB_USER    = "postgres"
    API_PORT   = $API_PORT
    SSM_PREFIX = "/bess/app"
}
# ==============================================================================

$SCRIPT_DIR = Split-Path -Parent $PSCommandPath
$STAGE_DIR  = Join-Path $env:TEMP "ec2_api_deploy_stage"

function Write-Step([int]$n, [string]$msg) {
    Write-Host ""
    Write-Host "[$n/6] $msg" -ForegroundColor Cyan
}

# Convert multi-line script -> SSM parameters JSON file, send command, return command ID
function Send-SsmScript([string]$script, [int]$timeoutSec = 120) {
    # Split script into lines, build proper JSON via ConvertTo-Json
    $lines = @($script -split "`n" | ForEach-Object { $_.TrimEnd("`r") })
    $paramObj = @{ commands = $lines }

    # Build full CLI input JSON
    $cliInput = @{
        InstanceIds    = @($INSTANCE_ID)
        DocumentName   = "AWS-RunShellScript"
        Parameters     = $paramObj
        TimeoutSeconds = $timeoutSec
    } | ConvertTo-Json -Depth 4 -Compress
    $cliFile = Join-Path $env:TEMP "ssm_cli_input.json"
    # Write UTF-8 WITHOUT BOM - AWS CLI rejects BOM
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($cliFile, $cliInput, $utf8NoBom)

    $raw = aws ssm send-command --cli-input-json "file://$cliFile" --region $REGION --output json 2>&1
    if ($LASTEXITCODE -ne 0) { throw "SSM send-command failed: $raw" }
    $sendResult = $raw | ConvertFrom-Json
    return $sendResult.Command.CommandId
}

# Wait for SSM command to complete -> return stdout
function Wait-SsmCommand([string]$commandId) {
    Write-Host "  Waiting for SSM command $commandId ..." -NoNewline
    $result = $null
    do {
        Start-Sleep -Seconds 3
        Write-Host "." -NoNewline
        # Capture stdout only (no 2>&1) so stderr never corrupts the JSON
        $rawOutput = aws ssm get-command-invocation `
            --command-id $commandId `
            --instance-id $INSTANCE_ID `
            --region $REGION `
            --output json
        if ($LASTEXITCODE -ne 0) {
            # Invocation record not yet created (race) - keep waiting
            continue
        }
        $jsonStr = ($rawOutput | Out-String).Trim()
        try {
            $result = $jsonStr | ConvertFrom-Json
        } catch {
            # Malformed output (transient) - keep waiting
            $result = $null
        }
    } while ($result -eq $null -or $result.Status -eq "InProgress" -or $result.Status -eq "Pending")
    Write-Host ""

    if ($result.StandardOutputContent) {
        Write-Host $result.StandardOutputContent
    }
    if ($result.Status -ne "Success") {
        if ($result.StandardErrorContent) { Write-Host $result.StandardErrorContent -ForegroundColor Red }
        throw "SSM command failed with status: $($result.Status)"
    }
    return $result
}

# ==============================================================================
# STEP 1 - Preflight checks
# ==============================================================================
Write-Step 1 "Preflight checks"

if ($INSTANCE_ID -eq "i-XXXXXXXXXXXXXXXXX") { throw "Edit `$INSTANCE_ID in deploy.ps1 first." }
if (-not (Test-Path "$SCRIPT_DIR\app.py"))   { throw "Run deploy.ps1 from inside ec2_api\." }

# Verify AWS CLI works
$callerId = aws sts get-caller-identity --region $REGION --output json 2>&1 | ConvertFrom-Json
if (-not $callerId.Account) { throw "AWS CLI not configured. Run 'aws configure' first." }
Write-Host "  AWS Account: $($callerId.Account)"

# Verify instance is managed by SSM
$ssmInfo = aws ssm describe-instance-information `
    --filters "Key=InstanceIds,Values=$INSTANCE_ID" `
    --region $REGION --output json 2>&1 | ConvertFrom-Json
if (-not $ssmInfo.InstanceInformationList -or $ssmInfo.InstanceInformationList.Count -eq 0) {
    throw "Instance $INSTANCE_ID is not registered with SSM. Check IAM role + SSM Agent."
}
$pingStatus = $ssmInfo.InstanceInformationList[0].PingStatus
if ($pingStatus -ne "Online") { throw "Instance SSM status: $pingStatus (must be Online)." }
Write-Host "  Instance $INSTANCE_ID - SSM Online"

# ==============================================================================
# STEP 2 - Stage files locally (exclude junk)
# ==============================================================================
Write-Step 2 "Staging files to temp directory"

if (Test-Path $STAGE_DIR) { Remove-Item $STAGE_DIR -Recurse -Force }
New-Item -ItemType Directory -Path $STAGE_DIR | Out-Null

robocopy $SCRIPT_DIR $STAGE_DIR /E `
    /XD "__pycache__" ".venv" "*.egg-info" `
    /XF "*.pyc" ".env" "deploy.ps1" `
    /NFL /NDL /NJH /NJS
if ($LASTEXITCODE -ge 8) { throw "robocopy failed with exit $LASTEXITCODE" }

# Generate systemd service unit
$envBlock = ($ENV_VARS.GetEnumerator() |
    ForEach-Object { "Environment=`"$($_.Key)=$($_.Value)`"" }) -join "`n"

$serviceContent = @"
[Unit]
Description=BESS EC2 API (Flask/Gunicorn)
After=network.target

[Service]
Type=simple
User=$EC2_USER
WorkingDirectory=$REMOTE_DIR
ExecStart=$REMOTE_DIR/.venv/bin/gunicorn -w $WORKERS -b 0.0.0.0:$API_PORT app:app
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

$envBlock

[Install]
WantedBy=multi-user.target
"@
$serviceUnitPath = Join-Path $STAGE_DIR "${SERVICE}.service"
$serviceUtf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($serviceUnitPath, $serviceContent, $serviceUtf8NoBom)

$fileCount = (Get-ChildItem $STAGE_DIR -Recurse -File).Count
Write-Host "  Staged $fileCount files + systemd unit."

# ==============================================================================
# STEP 3 - Package & upload to S3
# ==============================================================================
Write-Step 3 "Packaging and uploading to S3"

$tarFile = Join-Path $env:TEMP "ec2_api_deploy.tar.gz"
if (Test-Path $tarFile) { Remove-Item $tarFile -Force }

# Use tar (built-in on Windows 10+)
Push-Location $STAGE_DIR
tar -czf $tarFile *
Pop-Location

$sizeMB = [math]::Round((Get-Item $tarFile).Length / 1MB, 2)
Write-Host "  Archive: $sizeMB MB"

aws s3 cp $tarFile "s3://${DEPLOY_BUCKET}/${DEPLOY_KEY}" --region $REGION --quiet
if ($LASTEXITCODE -ne 0) { throw "S3 upload failed." }
Write-Host "  Uploaded to s3://${DEPLOY_BUCKET}/${DEPLOY_KEY}"

# ==============================================================================
# STEP 4 - SSM: Download archive from S3 on EC2
# ==============================================================================
Write-Step 4 "SSM: downloading archive on EC2"

$downloadCmd = @"
#!/bin/bash
set -euo pipefail
mkdir -p $REMOTE_DIR
cd $REMOTE_DIR
aws s3 cp "s3://${DEPLOY_BUCKET}/${DEPLOY_KEY}" /tmp/ec2_api_deploy.tar.gz --region $REGION
tar -xzf /tmp/ec2_api_deploy.tar.gz -C $REMOTE_DIR
rm -f /tmp/ec2_api_deploy.tar.gz
chown -R ${EC2_USER}:${EC2_USER} $REMOTE_DIR
echo "Downloaded and extracted OK"
"@

$cmdId = Send-SsmScript $downloadCmd 120
Wait-SsmCommand $cmdId | Out-Null

# ==============================================================================
# STEP 5 - SSM: Install venv + pip + systemd service
# ==============================================================================
Write-Step 5 "SSM: installing dependencies and configuring service"

$installCmd = @"
#!/bin/bash
set -euo pipefail

cd $REMOTE_DIR

echo "==> Creating virtualenv..."
sudo -u $EC2_USER python3 -m venv .venv --upgrade-deps
source .venv/bin/activate

echo "==> Installing packages..."
pip install -q -r requirements.txt

echo "==> Installing systemd service..."
cp ${REMOTE_DIR}/${SERVICE}.service /etc/systemd/system/${SERVICE}.service
systemctl daemon-reload
systemctl enable $SERVICE
systemctl restart $SERVICE

echo "==> Waiting for service..."
sleep 3
if ! systemctl is-active --quiet $SERVICE; then
  systemctl is-active $SERVICE || true
  echo "==> systemctl status ($SERVICE)"
  systemctl status $SERVICE --no-pager -l || true
  echo "==> journalctl tail ($SERVICE)"
  journalctl -u $SERVICE --no-pager -n 80 -o short-plain || true
  exit 3
fi
echo "Install OK"
"@

$cmdId = Send-SsmScript $installCmd 300
Wait-SsmCommand $cmdId | Out-Null

# ==============================================================================
# STEP 6 - SSM: Smoke test
# ==============================================================================
Write-Step 6 "SSM: smoke test"

$smokeCmd = @"
#!/bin/bash
echo "--- Service Active Check ---"
systemctl is-active $SERVICE
echo ""
echo "--- HTTP check ---"
curl -sf http://localhost:${API_PORT}/ -o /dev/null -w "HTTP %{http_code}" 2>/dev/null || echo "HTTP check skipped (root may 404)"
echo ""
echo "--- Last 5 log lines ---"
journalctl -u $SERVICE --no-pager -n 5 -o short-plain 2>/dev/null || echo "no logs yet"
"@

$cmdId = Send-SsmScript $smokeCmd 60
Wait-SsmCommand $cmdId | Out-Null

# ==============================================================================
# Done
# ==============================================================================
# Get public IP via describe-instances (no SSM needed)
$publicIp = aws ec2 describe-instances `
    --instance-ids $INSTANCE_ID `
    --region $REGION `
    --query "Reservations[0].Instances[0].PublicIpAddress" `
    --output text 2>&1
if (-not $publicIp -or $publicIp -eq "None") { $publicIp = "UNKNOWN" }

Write-Host ""
Write-Host "============================================" -ForegroundColor Green
Write-Host " Deployment complete! (via SSM)" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
Write-Host ""
Write-Host "  App URL : http://${publicIp}:${API_PORT}"
Write-Host "  Logs    : aws ssm start-session --target $INSTANCE_ID --region $REGION"
Write-Host "            then: sudo journalctl -u $SERVICE -f"
Write-Host "  Status  : aws ssm send-command --instance-ids $INSTANCE_ID --document-name AWS-RunShellScript --parameters 'commands=[`"systemctl status $SERVICE`"]' --region $REGION"
Write-Host ""
