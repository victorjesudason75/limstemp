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
