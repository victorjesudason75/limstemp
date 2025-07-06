function Invoke-SqlQuery {
    param(
        [string]$Query,
        [hashtable]$Parameters = @{},
        [hashtable]$Config,
        [switch]$NonQuery
    )
    
    $connection = $null
    try {
        $connection = New-Object System.Data.Odbc.OdbcConnection
        $settings = Get-Content $Config.ConfigPath -ErrorAction Stop | ConvertFrom-Json
        $connection.ConnectionString = "DSN=$($settings.DSN);UID=$($settings.UID);PWD=$($settings.PWD);"
        $connection.Open()

        $command = New-Object System.Data.Odbc.OdbcCommand
        $command.Connection = $connection
        $command.CommandText = $Query

        foreach ($key in $Parameters.Keys) {
            $param = $command.Parameters.AddWithValue($key, $Parameters[$key])
            if ($Parameters[$key] -eq $null) {
                $param.Value = [DBNull]::Value
            }
        }

        if ($NonQuery) {
            return $command.ExecuteNonQuery()
        }
        else {
            $adapter = New-Object System.Data.Odbc.OdbcDataAdapter $command
            $dataset = New-Object System.Data.DataSet
            $adapter.Fill($dataset) | Out-Null
            return $dataset.Tables[0]
        }
    }
    catch {
        Create-LIMSLog "Database error executing query [$Query]: $_"
        throw
    }
    finally {
        if ($connection -and $connection.State -eq 'Open') {
            $connection.Close()
        }
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
    $logMessage = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Message"
    Write-Host $logMessage
    $logMessage | Out-File -FilePath $config.LogPath -Append
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

function Update-HL7MessageStatus {
    param(
        [string]$EntryCode,
        [string]$Status,
        [hashtable]$Config
    )
    $query = "UPDATE T_HL7_MESSAGE_IN SET STATUS = ?, PROCESSED_DATE = ? WHERE ENTRY_CODE = ?"
    $params = @{
        Status = $Status
        ProcessedDate = (Get-Date)
        EntryCode = $EntryCode
    }
    Invoke-SqlQuery -Query $query -Parameters $params -Config $config -NonQuery
}