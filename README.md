# LIMS HL7 Processor

This repository contains PowerShell scripts for processing HL7 messages.

## Configuration

`ps/HL7.ps1` requires a JSON file with ODBC connection details. By default it
looks for `config/Settings.json`. Set the `HL7ConfigPath` environment variable
to point to a different configuration file if needed.

A sample configuration file is provided at `samples/Settings.json`. Copy it to
the `config` directory and edit the DSN, UID and PWD values for your
environment.

Log files are written to `logs/HL7Processor_<date>.log` by default. You can
change the log directory by setting the `HL7LogPath` environment variable.

## Usage

`ps/HL7.ps1` acts as a PowerShell script module. Load it with
`Import-Module` so its exported functions are available:

```powershell
Import-Module ./ps/HL7.ps1
```

Executing the file directly may raise an error from
`Export-ModuleMember` because that cmdlet only runs inside a module
scope.

### Processed filenames

When HL7 files are processed they are renamed using an HL7 timestamp with
millisecond precision followed by the order number and sending application.
This timestamp is generated by the `HL7-FormatDate` helper.  Using
milliseconds ensures that filenames are unique even when multiple files are
handled within the same second.  If a file with the generated name already
exists the `Rename-File` function appends an incremental suffix.

## Tests

Execute the tests using `run-tests.sh`. The script checks for `pwsh` and skips
tests with a message if it is not installed.
