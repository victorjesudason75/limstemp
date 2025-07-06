# LIMS HL7 Processor

This repository contains PowerShell scripts for processing HL7 messages.

## Configuration

`ps/HL7.ps1` requires a JSON file with ODBC connection details. By default it
looks for `config/Settings.json`. Override this path with the `HL7ConfigPath`
environment variable.

A sample configuration file is provided at `samples/Settings.json`. Copy it to
the `config` directory and edit the DSN, UID and PWD values for your
environment.

Log files are written to `logs/HL7Processor_<date>.log` by default. You can
change the log directory by setting the `HL7LogPath` environment variable.

## Tests

Execute the tests using `run-tests.sh`. The script checks for `pwsh` and skips
tests with a message if it is not installed.
