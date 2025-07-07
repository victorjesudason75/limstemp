function Invoke-SqlQuery {
    param(
        [string]$Query,
        [hashtable]$Parameters = @{},
        [hashtable]$Config,
        [switch]$NonQuery
    )
    
    $connection = $null
    try {
        Write-Verbose "Executing SQL query: $($Query -replace '\s+', ' ')"
        if ($Parameters.Count -gt 0) {
            Write-Verbose "Parameters: $($Parameters | ConvertTo-Json -Compress)"
        }

        $connection = New-Object System.Data.Odbc.OdbcConnection
        $settings = Get-Content $Config.ConfigPath -ErrorAction Stop | ConvertFrom-Json
        $connection.ConnectionString = "DSN=$($settings.DSN);UID=$($settings.UID);PWD=$($settings.PWD);"
        
        Write-Host "Connecting to database..." -ForegroundColor DarkGray
        $connection.Open()

        $command = New-Object System.Data.Odbc.OdbcCommand
        $command.Connection = $connection
        $command.CommandText = $Query

        foreach ($key in $Parameters.Keys) {
            $paramValue = $Parameters[$key]
            Write-Verbose "Binding parameter: @$key = '$paramValue'"
            $param = $command.Parameters.AddWithValue("@$key", $paramValue)
            if ($Parameters[$key] -eq $null) {
                $param.Value = [DBNull]::Value
            }
        }

        if ($NonQuery) {
            $result = $command.ExecuteNonQuery()
            Write-Host "Query executed. Rows affected: $result" -ForegroundColor DarkGray
            return $result
        }
        else {
            $adapter = New-Object System.Data.Odbc.OdbcDataAdapter $command
            $dataset = New-Object System.Data.DataSet
            $rowsReturned = $adapter.Fill($dataset)
            Write-Host "Query returned $rowsReturned rows" -ForegroundColor DarkGray
            return $dataset.Tables[0]
        }
    }
    catch {
        $msg = "DATABASE ERROR: $_"
        Write-Host $msg -ForegroundColor Red
        Create-LIMSLog -Message $msg -Config $Config
        throw
    }
    finally {
        if ($connection -and $connection.State -eq 'Open') {
            $connection.Close()
            Write-Verbose "Database connection closed"
        }
    }
}

function Get-DirectoryFiles {
    param(
        [string]$Path,
        [string]$Filter = '*'
    )
    if (-Not (Test-Path $Path)) { 
        Write-Host "Directory not found: $Path" -ForegroundColor Yellow
        return @() 
    }
    Write-Verbose "Scanning directory: $Path (Filter: $Filter)"
    Get-ChildItem -Path $Path -Filter $Filter -File
}

function Get-FileContents {
    param(
        [string]$Path
    )
    Write-Verbose "Reading file: $Path"
    Get-Content -Path $Path -Raw
}

function Rename-File {
    param(
        [string]$Old,
        [string]$New
    )

    Write-Verbose "Attempting to move file: $Old -> $New"

    $dir = Split-Path $New -Parent
    if (-not (Test-Path $dir)) {
        Write-Host "Creating directory: $dir" -ForegroundColor DarkGray
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }

    $target = $New
    $attempt = 0

    while ($true) {
        try {
            Move-Item -Path $Old -Destination $target -Force -ErrorAction Stop
            Write-Host "Successfully moved to: $target" -ForegroundColor DarkGray
            return $target
        }
        catch [System.IO.IOException] {
            if ($_.Exception.Message -match 'already exists') {
                $attempt++
                $baseName = [System.IO.Path]::GetFileNameWithoutExtension($New)
                $extension = [System.IO.Path]::GetExtension($New)
                $target = Join-Path $dir "${baseName}_$attempt$extension"
                Write-Host "Destination exists, retrying with new name: $(Split-Path $target -Leaf)" -ForegroundColor Yellow
                continue
            }
            else {
                $msg = "FILE MOVE ERROR: $_"
                Write-Host $msg -ForegroundColor Red
                Create-LIMSLog -Message $msg -Config $config
                throw
            }
        }
        catch {
            $msg = "FILE MOVE ERROR: $_"
            Write-Host $msg -ForegroundColor Red
            Create-LIMSLog -Message $msg -Config $config
            throw
        }
    }
}

function Create-LIMSLog {
    param(
        [string]$Message,
        [hashtable]$Config
    )
    $logMessage = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Message"
    
    # Auto-color based on message content
    switch -Regex ($Message) {
        "error|fail|exception|not found|critical" { 
            Write-Host $logMessage -ForegroundColor Red
        }
        "warn|missing|skipping" { 
            Write-Host $logMessage -ForegroundColor Yellow 
        }
        "success|complete|processed|created" { 
            Write-Host $logMessage -ForegroundColor Green 
        }
        default { 
            Write-Host $logMessage -ForegroundColor Gray 
        }
    }
    
    # Write to log file
    $logDir = Split-Path $Config.LogPath -Parent
    if (-not (Test-Path $logDir)) { 
        New-Item -ItemType Directory -Path $logDir -Force | Out-Null 
    }
    $logMessage | Out-File -FilePath $Config.LogPath -Append -Encoding utf8
}

function HL7-Parse {
    param([string]$Message)
    Write-Verbose "Parsing HL7 message"
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
    Write-Verbose "Extracting field: Segment=$Segment, Field=$Field, Component=$Component"
    if ($Segment -eq 'ORC') { return '10001' }
    if ($Segment -eq 'MSH') { return 'SendingApp' }
    return ''
}

function HL7-DiscardMessage { 
    param($Handle) 
    Write-Verbose "Discarding HL7 message handle"
    return $true 
}

function HL7-FormatDate {
    param([datetime]$Date)
    return $Date.ToString('yyyyMMddHHmmssfff')
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
    Invoke-SqlQuery -Query $query -Parameters $params -Config $Config -NonQuery
}