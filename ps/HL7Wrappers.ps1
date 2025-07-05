function Invoke-SqlQuery {
    param(
        [string]$Query
    )
    Write-Host "[SQL] $Query"
    # Placeholder: return sample data based on query
    if ($Query -match 'HL7_IN_FILE_PATH') {
        return @(
            @{ HL7_IN_FILE_PATH = 'samples/'; HL7_PROCESSED_FILE_PATH='samples/processed/'; HL7_ERROR_FILE_PATH='samples/error/'; HL7_MSG_TEMP_IN='DEFAULT' }
        )
    } elseif ($Query -match 'T_HL7_MESSAGE_IN') {
        return @()
    } else {
        return @()
    }
}

function Get-DirectoryFiles {
    param(
        [string]$Path,
        [string]$Filter = '*'
    )
    if (-Not (Test-Path $Path)) { return @() }
    Get-ChildItem -Path $Path -Filter $Filter -File
}

function Get-FileContents {
    param(
        [string]$Path
    )
    Get-Content -Path $Path -Raw
}

function Rename-File {
    param(
        [string]$Old,
        [string]$New
    )
    $dir = Split-Path $New
    if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir | Out-Null }
    Move-Item -Path $Old -Destination $New
}

function Create-LIMSLog {
    param(
        [string]$Message
    )
    Write-Host "[LOG] $Message"
}

function HL7-Parse {
    param(
        [string]$Message
    )
    return @{ ORC=$Message; MSH=$Message }
}

function HL7-FieldFromSegment {
    param(
        $Handle,
        [string]$Segment,
        [int]$Rep,
        [string]$Field,
        [string]$Component
    )
    if ($Segment -eq 'ORC') { return '10001' }
    if ($Segment -eq 'MSH') { return 'SendingApp' }
    return ''
}

function HL7-DiscardMessage { param($Handle) return $true }

function HL7-FormatDate {
    param([datetime]$Date)
    return $Date.ToString('yyyyMMddHHmmss')
}

# ---------------------------------------------------------------------------
# In-memory table helpers for HL7_MESSAGE_IN
# ---------------------------------------------------------------------------

if (-not $Global:HL7MessageTable) { $Global:HL7MessageTable = @() }

function Insert-HL7MessageIn {
    param(
        [string]$MsgCategory,
        [string]$Status,
        [string]$HL7String,
        [string]$OrderNumber,
        [string]$SendingApplication
    )
    $entryCode = [string]($Global:HL7MessageTable.Count + 1)
    $record = [pscustomobject]@{
        ENTRY_CODE        = $entryCode
        MSG_CATEGORY      = $MsgCategory
        STATUS            = $Status
        HL7_STRING        = $HL7String
        ORDER_NUMBER      = $OrderNumber
        SENDING_APPLICATION = $SendingApplication
        SAMPLE_NUMBER     = ''
    }
    $Global:HL7MessageTable += $record
    return $record
}

function Update-HL7MessageString {
    param(
        [string]$EntryCode,
        [string]$HL7String
    )
    $rec = $Global:HL7MessageTable | Where-Object { $_.ENTRY_CODE -eq $EntryCode }
    if ($rec) { $rec.HL7_STRING = $HL7String }
}

function Get-HL7MessageByOrder {
    param(
        [string]$OrderNumber,
        [string]$SendingApplication
    )
    $Global:HL7MessageTable | Where-Object { $_.ORDER_NUMBER -eq $OrderNumber -and $_.SENDING_APPLICATION -eq $SendingApplication } | Select-Object -First 1
}

function Get-PendingHL7Messages {
    $Global:HL7MessageTable | Where-Object { $_.STATUS -eq 'N' }
}

function Get-QueuedOrErroredHL7Messages {
    $Global:HL7MessageTable | Where-Object { $_.STATUS -in @('P','E') }
}

function Update-HL7MessageStatus {
    param(
        [string]$EntryCode,
        [string]$Status
    )
    $rec = $Global:HL7MessageTable | Where-Object { $_.ENTRY_CODE -eq $EntryCode }
    if ($rec) { $rec.STATUS = $Status }
}
