Import-Module "$PSScriptRoot/../ps/HL7.ps1"

Describe 'SCHED_HL7_IN_MSG_READ' {
    It 'Processes sample HL7 file without error' {
        { SCHED_HL7_IN_MSG_READ -MaxMessagesLoop 0 } | Should -Not -Throw
        ($Global:HL7MessageTable.Count) | Should -BeGreaterThan 0
    }
}

Describe 'HL7_CREATE_MESSAGE' {
    It 'Creates HL7 output file' {
        $rec = $Global:HL7MessageTable | Select-Object -First 1
        { HL7_CREATE_MESSAGE -HL7MessageEntryCode $rec.ENTRY_CODE -HL7InInitialStatus 'P' } | Should -Not -Throw
        Test-Path "samples/out_${($rec.ENTRY_CODE)}.hl7" | Should -BeTrue
    }
}
