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
            $HL7String = Get-FileContents $file.FullName
            $orderNumber = ''
            $sendingApplication = ''
            $handle = HL7-Parse $HL7String
            $orderNumber = HL7-FieldFromSegment $handle 'ORC' 1 'Placer Order Number' 'Entity Identifier'
            $sendingApplication = HL7-FieldFromSegment $handle 'MSH' 1 'Sending Application' 'Namespace ID'
            HL7-DiscardMessage $handle | Out-Null

            if (-not $orderNumber -or -not $sendingApplication) {
                Create-LIMSLog "Order Number OR Sending Application not Found"
                continue
            }
            $dateTimeHL7Out = HL7-FormatDate (Get-Date)
            $newFileName = "$processedDirectory$($dateTimeHL7Out)-$orderNumber-$sendingApplication.txt"
            Rename-File $file.FullName $newFileName
        }
    }

    $moreMessagesToProcess = $true
    while ($moreMessagesToProcess) {
        $query = "select ENTRY_CODE, MSG_CATEGORY, HL7_STRING from T_HL7_MESSAGE_IN where STATUS = 'N' order by ENTRY_CODE"
        $messages = Invoke-SqlQuery $query
        $numMessages = $messages.Count
        if ($numMessages -lt $MaxMessagesLoop) { $moreMessagesToProcess = $false }
        foreach ($msg in $messages) {
            HL7_IN_INITIAL $msg.ENTRY_CODE $msg.MSG_CATEGORY $msg.HL7_STRING
        }
    }

    $qryHL7In = "select ENTRY_CODE, STATUS from T_HL7_MESSAGE_IN where STATUS in ('P','E')"
    $records = Invoke-SqlQuery $qryHL7In
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
    $query = "select MSG_CATEGORY, HL7_STRING from T_HL7_MESSAGE_IN where ENTRY_CODE = '$HL7MessageEntryCode'"
    $record = (Invoke-SqlQuery $query)[0]
    $messageType = 'ORU^R01'
    $messageControlID = "$dateTimeHL7Out-$HL7MessageEntryCode"
    $HL7String = $record.HL7_STRING
    if ($HL7String) {
        $fileName = "samples/out_${HL7MessageEntryCode}.hl7"
        Rename-File -Old (New-TemporaryFile) -New $fileName
        Set-Content -Path $fileName -Value $HL7String
    }
}

function HL7_IN_INITIAL { param($EntryCode,$Template,$String) }

Export-ModuleMember -Function SCHED_HL7_IN_MSG_READ,HL7_CREATE_MESSAGE,HL7_IN_INITIAL
