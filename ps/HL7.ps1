. "$PSScriptRoot/HL7Wrappers.ps1"

# Configuration with paths relative to the script location
$config = @{
    ConfigPath = "$PSScriptRoot/../Config/settings.json"
    LogPath = "$PSScriptRoot/../Logs/HL7Processor_$(Get-Date -Format 'yyyyMMdd').log"
    DefaultStartID = 100000
    MaxRetryAttempts = 3
    RetryDelaySeconds = 5
}

# Ensure required folders exist
$logDir = Split-Path $config.LogPath
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }

if (-not (Test-Path $config.ConfigPath)) {
    throw "Config file not found at $($config.ConfigPath)"
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
        $facilities = Invoke-SqlQuery -Query $HL7FilesQry -Config $config
        
        foreach ($facility in $facilities) {
            $inputDirectory = $facility.HL7_IN_FILE_PATH
            $processedDirectory = $facility.HL7_PROCESSED_FILE_PATH
            $errorDirectory = $facility.HL7_ERROR_FILE_PATH

            if (-not (Test-Path $inputDirectory)) {
                Create-LIMSLog -Message "Input directory not found: $inputDirectory" -Config $config
                continue
            }

            $files = Get-DirectoryFiles $inputDirectory '*'
            foreach ($file in $files) {
                try {
                    $HL7String = Get-FileContents $file.FullName
                    $handle = HL7-Parse $HL7String
                    $orderNumber = HL7-FieldFromSegment $handle 'ORC' 1 'Placer Order Number' 'Entity Identifier'
                    $sendingApplication = HL7-FieldFromSegment $handle 'MSH' 1 'Sending Application' 'Namespace ID'
                    HL7-DiscardMessage $handle | Out-Null

                    if (-not $orderNumber -or -not $sendingApplication) {
                        Create-LIMSLog -Message "Order Number OR Sending Application not Found in file $($file.Name)" -Config $config
                        $errorFileName = "$errorDirectory$($file.Name)"
                        Rename-File $file.FullName $errorFileName
                        continue
                    }
                    
                    $dateTimeHL7Out = HL7-FormatDate (Get-Date)
                    $newFileName = "$processedDirectory$($dateTimeHL7Out)-$orderNumber-$sendingApplication.txt"
                    Rename-File $file.FullName $newFileName
                    Create-LIMSLog -Message "Processed file: $($file.Name) -> $newFileName" -Config $config
                }
                catch {
                    Create-LIMSLog -Message "Error processing file $($file.Name): $_" -Config $config
                    continue
                }
            }
        }

        $moreMessagesToProcess = $true
        while ($moreMessagesToProcess) {
            $query = "SELECT ENTRY_CODE, MSG_CATEGORY, HL7_STRING FROM T_HL7_MESSAGE_IN WHERE STATUS = 'N' ORDER BY ENTRY_CODE"
            $messages = Invoke-SqlQuery -Query $query -Config $config
            $numMessages = $messages.Count
            if ($numMessages -lt $MaxMessagesLoop) { $moreMessagesToProcess = $false }
            
            foreach ($msg in $messages) {
                try {
                    HL7_IN_INITIAL -EntryCode $msg.ENTRY_CODE -Template $msg.MSG_CATEGORY -String $msg.HL7_STRING -Config $config
                }
                catch {
                    Create-LIMSLog -Message "Error processing message $($msg.ENTRY_CODE): $_" -Config $config
                }
            }
        }

        $qryHL7In = "SELECT ENTRY_CODE, STATUS FROM T_HL7_MESSAGE_IN WHERE STATUS IN ('P','E')"
        $records = Invoke-SqlQuery -Query $qryHL7In -Config $config
        foreach ($rec in $records) {
            try {
                HL7_CREATE_MESSAGE -HL7MessageEntryCode $rec.ENTRY_CODE -HL7InInitialStatus $rec.STATUS -Config $config
            }
            catch {
                Create-LIMSLog -Message "Error creating output message for $($rec.ENTRY_CODE): $_" -Config $config
            }
        }
    }
    catch {
        Create-LIMSLog -Message "Critical error in SCHED_HL7_IN_MSG_READ: $_" -Config $config
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
            Rename-File -Old (New-TemporaryFile) -New $fileName
            Set-Content -Path $fileName -Value $HL7String
            Create-LIMSLog -Message "Created output HL7 file: $fileName" -Config $config
        }
    }
    catch {
        Create-LIMSLog -Message "Error in HL7_CREATE_MESSAGE for $HL7MessageEntryCode: $_" -Config $config
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
        Create-LIMSLog -Message "Starting processing of HL7 message $EntryCode" -Config $config
        
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
        
        Invoke-SqlQuery -Query $updateQuery -Parameters $params -Config $config -NonQuery
        Create-LIMSLog -Message "Successfully processed message $EntryCode ($messageControlId)" -Config $config
        return $true
    }
    catch {
        Create-LIMSLog -Message "Error processing message $EntryCode: $_" -Config $config
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

Export-ModuleMember -Function SCHED_HL7_IN_MSG_READ, HL7_CREATE_MESSAGE, HL7_IN_INITIAL