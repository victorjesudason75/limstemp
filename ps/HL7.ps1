. "$PSScriptRoot/HL7Wrappers.ps1"

function SCHED_HL7_IN_MSG_READ {
    param(
        [int]$MaxMessagesLoop = 100
    )

    $HL7FilesQry = @"
select HL7facilityDetails.HL7_IN_FILE_PATH,
       HL7facilityDetails.HL7_PROCESSED_FILE_PATH,
       HL7facilityDetails.HL7_ERROR_FILE_PATH,
       HL7facilityDetails.HL7_MSG_TEMP_IN
from HL7_FACILITY_DETAILS HL7facilityDetails
join FACILITY facility on facility.Z_HL7_FACILITY_DETAILS = HL7facilityDetails.ENTRY_NAME
where HL7facilityDetails.ACTIVE = 'T' and facility.Z_HL7 = 'T'
"@
    $facilities = Invoke-SqlQuery $HL7FilesQry

    foreach ($facility in $facilities) {
        $inputDirectory = $facility.HL7_IN_FILE_PATH
        $processedDirectory = $facility.HL7_PROCESSED_FILE_PATH
        $errorDirectory = $facility.HL7_ERROR_FILE_PATH
        $HL7Template = $facility.HL7_MSG_TEMP_IN

        $files = Get-DirectoryFiles $inputDirectory '*'
        foreach ($file in $files) {
            $filePath = $file.FullName
            $HL7String = Get-FileContents $filePath
            $handle = HL7-Parse $HL7String
            $orderNumber = HL7-FieldFromSegment $handle 'ORC' 1 'Placer Order Number' 'Entity Identifier'
            $sendingApplication = HL7-FieldFromSegment $handle 'MSH' 1 'Sending Application' 'Namespace ID'
            HL7-DiscardMessage $handle | Out-Null

            $dateStr = HL7-FormatDate (Get-Date)

            if (-not $orderNumber -or -not $sendingApplication) {
                Create-LIMSLog "Order Number OR Sending Application not Found"
                $errFile = Join-Path $errorDirectory "$dateStr-$($file.Name)"
                Rename-File $filePath $errFile
                continue
            }

            $existing = Get-HL7MessageByOrder -OrderNumber $orderNumber -SendingApplication $sendingApplication
            if (-not $existing) {
                $rec = Insert-HL7MessageIn -MsgCategory $HL7Template -Status 'N' -HL7String $HL7String -OrderNumber $orderNumber -SendingApplication $sendingApplication
                $entryCode = $rec.ENTRY_CODE
            } elseif (-not $existing.SAMPLE_NUMBER) {
                Update-HL7MessageString -EntryCode $existing.ENTRY_CODE -HL7String $HL7String
                $entryCode = $existing.ENTRY_CODE
            } else {
                Create-LIMSLog "Duplicate Order Sample# $($existing.SAMPLE_NUMBER) already logged"
                $entryCode = $existing.ENTRY_CODE
            }

            $newFileName = Join-Path $processedDirectory "$dateStr-$orderNumber-$sendingApplication.txt"
            Rename-File $filePath $newFileName
        }
    }

    $moreMessagesToProcess = $true
    while ($moreMessagesToProcess) {
        $messages = Get-PendingHL7Messages
        $numMessages = $messages.Count
        if ($numMessages -lt $MaxMessagesLoop) { $moreMessagesToProcess = $false }
        foreach ($msg in $messages) {
            HL7_IN_INITIAL $msg.ENTRY_CODE $msg.MSG_CATEGORY $msg.HL7_STRING
            Update-HL7MessageStatus -EntryCode $msg.ENTRY_CODE -Status 'P'
        }
    }

    $records = Get-QueuedOrErroredHL7Messages
    foreach ($rec in $records) {
        HL7_CREATE_MESSAGE -HL7MessageEntryCode $rec.ENTRY_CODE -HL7InInitialStatus $rec.STATUS
    }
}

function HL7_CREATE_MESSAGE {
    param(
        [string]$HL7MessageEntryCode,
        [string]$HL7InInitialStatus
    )
    $now = Get-Date
    $dateTimeHL7Out = HL7-FormatDate $now
    $record = ($Global:HL7MessageTable | Where-Object { $_.ENTRY_CODE -eq $HL7MessageEntryCode })[0]
    if (-not $record) { return }

    $messageType = 'ORU^R01'
    $messageControlID = "$dateTimeHL7Out-$HL7MessageEntryCode"
    $HL7String = $record.HL7_STRING
    if ($HL7String) {
        $fileName = "samples/out_${HL7MessageEntryCode}.hl7"
        Rename-File -Old (New-TemporaryFile) -New $fileName
        Set-Content -Path $fileName -Value $HL7String
        Update-HL7MessageStatus -EntryCode $HL7MessageEntryCode -Status 'A'
    }
}

function HL7_IN_INITIAL { param($EntryCode,$Template,$String) }

Export-ModuleMember -Function SCHED_HL7_IN_MSG_READ,HL7_CREATE_MESSAGE,HL7_IN_INITIAL
