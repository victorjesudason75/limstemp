. "$PSScriptRoot/HL7Wrappers.ps1"

# Configuration with paths relative to the script location
$configPath = $env:HL7ConfigPath
if (-not $configPath) {
    $configPath = "$PSScriptRoot/../config/Settings.json"
}

$logDir = $env:HL7LogPath
if (-not $logDir) {
    $logDir = "$PSScriptRoot/../logs"
}
$logPath = Join-Path $logDir "HL7Processor_$(Get-Date -Format 'yyyyMMdd').log"

$config = @{
    ConfigPath = $configPath
    LogPath = $logPath
    DefaultStartID = 100000
    MaxRetryAttempts = 3
    RetryDelaySeconds = 5
}

# Ensure required folders exist
$logDir = Split-Path $config.LogPath
if (-not (Test-Path $logDir)) { 
    Write-Host "Creating log directory: $logDir" -ForegroundColor Cyan
    New-Item -ItemType Directory -Path $logDir | Out-Null 
}

if (-not (Test-Path $config.ConfigPath)) {
    $msg = "Config file not found at $($config.ConfigPath)"
    Write-Host $msg -ForegroundColor Red
    throw $msg
}

function SCHED_HL7_IN_MSG_READ {
    param([int]$MaxMessagesLoop = 100)
    
    $HL7FilesQry = @"
SELECT HL7facilityDetails.HL7_IN_FILE_PATH,
       HL7facilityDetails.HL7_PROCESSED_FILE_PATH,
       HL7facilityDetails.HL7_ERROR_FILE_PATH,
       HL7facilityDetails.HL7_MSG_TEMP_IN
FROM HL7_FACILITY_DETAILS HL7facilityDetails
JOIN FACILITY facility ON facility.Z_HL7_FACILITY_DETAILS = HL7facilityDetails.ENTRY_NAME
WHERE HL7facilityDetails.ACTIVE = 'T' AND facility.Z_HL7 = 'T'
"@
    
    try {
        Write-Host "=== Starting HL7 Message Processing ===" -ForegroundColor Green
        Write-Host "Querying active facilities..." -ForegroundColor Cyan
        $facilities = Invoke-SqlQuery -Query $HL7FilesQry -Config $config
        
        Write-Host "Found $($facilities.Count) active facilities" -ForegroundColor Green

        foreach ($facility in $facilities) {
            Write-Host "`nProcessing Facility: $($facility.ENTRY_NAME)" -ForegroundColor Yellow
            Write-Host "Input Path: $($facility.HL7_IN_FILE_PATH)" -ForegroundColor Gray

            $inputDirectory = $facility.HL7_IN_FILE_PATH
            $processedDirectory = $facility.HL7_PROCESSED_FILE_PATH
            $errorDirectory = $facility.HL7_ERROR_FILE_PATH

            if (-not (Test-Path $inputDirectory)) {
                $msg = "Input directory not found: $inputDirectory"
                Write-Host $msg -ForegroundColor Red
                Create-LIMSLog -Message $msg -Config $config
                continue
            }

            $files = Get-DirectoryFiles $inputDirectory '*.hl7'
            Write-Host "Found $($files.Count) files to process" -ForegroundColor Cyan
            
            foreach ($file in $files) {
                Write-Host "`nProcessing File: $($file.Name)" -ForegroundColor White
                try {
                    $HL7String = Get-FileContents $file.FullName
                    $handle = HL7-Parse $HL7String
                    $orderNumber = HL7-FieldFromSegment $handle 'ORC' 1 'Placer Order Number' 'Entity Identifier'
                    $sendingApplication = HL7-FieldFromSegment $handle 'MSH' 1 'Sending Application' 'Namespace ID'
                    HL7-DiscardMessage $handle | Out-Null

                    if (-not $orderNumber -or -not $sendingApplication) {
                        $msg = "Missing required fields in file $($file.Name)"
                        Write-Host $msg -ForegroundColor Yellow
                        Create-LIMSLog -Message $msg -Config $config
                        $errorFileName = Join-Path $errorDirectory $file.Name
                        $movedFile = Rename-File $file.FullName $errorFileName -Config $config
                        Write-Host "Moved to error directory: $movedFile" -ForegroundColor Yellow
                        continue
                    }
                    
                    # Use helper to get consistent timestamp with millisecond precision
                    $dateTimeHL7Out = HL7-FormatDate (Get-Date)
                    $newFileName = Join-Path $processedDirectory "$($dateTimeHL7Out)-$orderNumber-$sendingApplication.txt"
                    $movedFile = Rename-File $file.FullName $newFileName -Config $config
                    $msg = "Processed: $($file.Name) -> $(Split-Path $movedFile -Leaf)"
                    Write-Host $msg -ForegroundColor Green
                    Create-LIMSLog -Message $msg -Config $config
                }
                catch {
                    $msg = "ERROR Processing $($file.Name): $_"
                    Write-Host $msg -ForegroundColor Red
                    Create-LIMSLog -Message $msg -Config $config
                    continue
                }
            }
        }

        Write-Host "`n=== Processing Database Messages ===" -ForegroundColor Green
        $moreMessagesToProcess = $true
        $totalProcessed = 0
        
        while ($moreMessagesToProcess) {
            $query = "SELECT ENTRY_CODE, MSG_CATEGORY, HL7_STRING FROM T_HL7_MESSAGE_IN WHERE STATUS = 'N' ORDER BY ENTRY_CODE"
            $messages = Invoke-SqlQuery -Query $query -Config $config
            $numMessages = $messages.Count
            
            if ($numMessages -eq 0) {
                Write-Host "No pending messages found." -ForegroundColor Gray
                break
            }

            Write-Host "Processing batch of $numMessages messages..." -ForegroundColor Cyan
            
            foreach ($msg in $messages) {
                Write-Host "Message ID: $($msg.ENTRY_CODE) [$($msg.MSG_CATEGORY)]" -ForegroundColor White
                try {
                    $result = HL7_IN_INITIAL -EntryCode $msg.ENTRY_CODE -Template $msg.MSG_CATEGORY -String $msg.HL7_STRING -Config $config
                    if ($result) {
                        $totalProcessed++
                        Write-Host "Processed successfully" -ForegroundColor Green
                    }
                }
                catch {
                    $msg = "ERROR Processing $($msg.ENTRY_CODE): $_"
                    Write-Host $msg -ForegroundColor Red
                    Create-LIMSLog -Message $msg -Config $config
                }
            }
            
            if ($numMessages -lt $MaxMessagesLoop) { $moreMessagesToProcess = $false }
        }

        Write-Host "`n=== Finalizing Processed Messages ===" -ForegroundColor Green
        $qryHL7In = "SELECT ENTRY_CODE, STATUS FROM T_HL7_MESSAGE_IN WHERE STATUS IN ('P','E')"
        $records = Invoke-SqlQuery -Query $qryHL7In -Config $config
        
        if ($records.Count -gt 0) {
            Write-Host "Found $($records.Count) completed messages to finalize" -ForegroundColor Cyan
            foreach ($rec in $records) {
                Write-Host "Finalizing $($rec.ENTRY_CODE) [Status: $($rec.STATUS)]" -ForegroundColor White
                try {
                    HL7_CREATE_MESSAGE -HL7MessageEntryCode $rec.ENTRY_CODE -HL7InInitialStatus $rec.STATUS -Config $config
                    Write-Host "Finalized successfully" -ForegroundColor Green
                }
                catch {
                    $msg = "ERROR Finalizing $($rec.ENTRY_CODE): $_"
                    Write-Host $msg -ForegroundColor Red
                    Create-LIMSLog -Message $msg -Config $config
                }
            }
        }
        else {
            Write-Host "No completed messages to finalize" -ForegroundColor Gray
        }

        Write-Host "`n=== Processing Complete ===" -ForegroundColor Green
        Write-Host "Total messages processed: $totalProcessed" -ForegroundColor Green
        Write-Host "Log file: $($config.LogPath)" -ForegroundColor Gray
    }
    catch {
        $msg = "CRITICAL ERROR: $_"
        Write-Host $msg -ForegroundColor Red -BackgroundColor Black
        Create-LIMSLog -Message $msg -Config $config
        throw
    }
}

function HL7_CREATE_MESSAGE {
    param(
        [string]$HL7MessageEntryCode,
        [string]$HL7InInitialStatus,
        [hashtable]$Config
    )
    
    try {
        Write-Host "Creating output for message $HL7MessageEntryCode" -ForegroundColor Cyan
        
        $now = Get-Date
        $dateTimeHL7Out = HL7-FormatDate $now
        $query = "SELECT MSG_CATEGORY, HL7_STRING FROM T_HL7_MESSAGE_IN WHERE ENTRY_CODE = ?"
        $params = @{EntryCode = $HL7MessageEntryCode}
        $record = (Invoke-SqlQuery -Query $query -Parameters $params -Config $config)[0]
        
        $messageType = 'ORU^R01'
        $messageControlID = "$dateTimeHL7Out-$HL7MessageEntryCode"
        $HL7String = $record.HL7_STRING
        
        if ($HL7String) {
            $fileName = "$PSScriptRoot/../samples/out_${HL7MessageEntryCode}.hl7"
            $tempFile = New-TemporaryFile
            $finalPath = Rename-File -Old $tempFile -New $fileName -Config $config
            Set-Content -Path $finalPath -Value $HL7String
            $msg = "Created output file: $(Split-Path $finalPath -Leaf)"
            Write-Host $msg -ForegroundColor Green
            Create-LIMSLog -Message $msg -Config $config
        }
        else {
            $msg = "No HL7 content found for $HL7MessageEntryCode"
            Write-Host $msg -ForegroundColor Yellow
            Create-LIMSLog -Message $msg -Config $config
        }
    }
    catch {
        $msg = "ERROR creating output for $HL7MessageEntryCode: $_"
        Write-Host $msg -ForegroundColor Red
        Create-LIMSLog -Message $msg -Config $config
        throw
    }
}

function HL7_IN_INITIAL {
    param(
        [string]$EntryCode,
        [string]$Template,
        [string]$String,
        [hashtable]$Config
    )
    
    $hl7Handle = $null
    try {
        Write-Host "Processing message $EntryCode [$Template]" -ForegroundColor Cyan
        
        $hl7Handle = HL7-Parse -Message $String
        
        $messageType = HL7-FieldFromSegment -Handle $hl7Handle -Segment 'MSH' -Rep 1 -Field 'Message Type' -Component ''
        $messageControlId = HL7-FieldFromSegment -Handle $hl7Handle -Segment 'MSH' -Rep 1 -Field 'Message Control ID' -Component ''
        
        if (-not $messageType) { throw "Message type not found" }
        if (-not $messageControlId) { throw "Message Control ID not found" }

        $updateQuery = @"
UPDATE T_HL7_MESSAGE_IN 
SET MESSAGE_TYPE = ?,
    MESSAGE_CONTROL_ID = ?,
    STATUS = 'P',
    PROCESSED_DATE = ?
WHERE ENTRY_CODE = ?
"@
        $params = @{
            MessageType = $messageType
            MessageControlId = $messageControlId
            ProcessedDate = (Get-Date)
            EntryCode = $EntryCode
        }
        
        $rowsAffected = Invoke-SqlQuery -Query $updateQuery -Parameters $params -Config $config -NonQuery
        $msg = "Updated database ($rowsAffected rows)"
        Write-Host $msg -ForegroundColor Green
        Create-LIMSLog -Message $msg -Config $config
        return $true
    }
    catch {
        $msg = "ERROR processing $EntryCode: $_"
        Write-Host $msg -ForegroundColor Red
        Create-LIMSLog -Message $msg -Config $config
        
        $errorQuery = "UPDATE T_HL7_MESSAGE_IN SET STATUS = 'E', ERROR_MESSAGE = ?, PROCESSED_DATE = ? WHERE ENTRY_CODE = ?"
        $errorParams = @{
            ErrorMessage = $_.Exception.Message
            ProcessedDate = (Get-Date)
            EntryCode = $EntryCode
        }
        Invoke-SqlQuery -Query $errorQuery -Parameters $errorParams -Config $config -NonQuery
        throw
    }
    finally {
        if ($hl7Handle) { HL7-DiscardMessage -Handle $hl7Handle | Out-Null }
    }
}