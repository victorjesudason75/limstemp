Import-Module "$PSScriptRoot/../ps/HL7.ps1"

Describe 'SCHED_HL7_IN_MSG_READ' {
    It 'Processes sample HL7 file without error' {
        { SCHED_HL7_IN_MSG_READ -MaxMessagesLoop 0 } | Should -Not -Throw
    }
}

Describe 'HL7_CREATE_MESSAGE' {
    It 'Creates HL7 output file' {
        { HL7_CREATE_MESSAGE -HL7MessageEntryCode '1' -HL7InInitialStatus 'P' } | Should -Not -Throw
    }
}
