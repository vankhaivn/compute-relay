$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$ProjectDir = Join-Path $RepoRoot "tools/kaggle-client"

if ($args.Count -eq 0) {
    Write-Error "usage: scripts/kaggle-client.ps1 <sync|lock-check|test|inventory|dependencies|check> [args...]"
    exit 2
}

$Task = $args[0]
$TaskArgs = @($args | Select-Object -Skip 1)
Set-Location $ProjectDir

function Invoke-Uv {
    param([string[]]$Arguments)
    & uv @Arguments
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

switch ($Task) {
    "sync" { Invoke-Uv (@("sync", "--locked", "--no-dev") + $TaskArgs) }
    "lock-check" { Invoke-Uv (@("lock", "--check") + $TaskArgs) }
    "test" {
        Invoke-Uv @("run", "--locked", "python", "-m", "compileall", "-q", "src", "tests")
        Invoke-Uv (@("run", "--locked", "python", "-m", "unittest", "discover", "-s", "tests", "-v") + $TaskArgs)
    }
    "inventory" { Invoke-Uv (@("run", "--locked", "compute-relay-kaggle-probe", "inventory") + $TaskArgs) }
    "dependencies" { Invoke-Uv (@("run", "--locked", "compute-relay-kaggle-probe", "dependencies") + $TaskArgs) }
    "check" {
        Invoke-Uv @("lock", "--check")
        Invoke-Uv @("run", "--locked", "python", "-m", "compileall", "-q", "src", "tests")
        Invoke-Uv @("run", "--locked", "python", "-m", "unittest", "discover", "-s", "tests", "-v")
        $TempDir = Join-Path ([System.IO.Path]::GetTempPath()) ("compute-relay-kaggle-" + [Guid]::NewGuid())
        New-Item -ItemType Directory -Path $TempDir | Out-Null
        try {
            $Inventory = Join-Path $TempDir "inventory.json"
            $Dependencies = Join-Path $TempDir "dependencies.json"
            Invoke-Uv @("run", "--locked", "compute-relay-kaggle-probe", "inventory", "--output", $Inventory)
            Invoke-Uv @("run", "--locked", "compute-relay-kaggle-probe", "dependencies", "--output", $Dependencies)
            # The committed full metadata record is generated on Linux. Windows can add
            # platform-specific distributions, so validate generation here and compare the
            # canonical record in Linux CI.
            if (-not (Test-Path $Dependencies)) {
                throw "dependency inventory was not generated"
            }
        }
        finally {
            Remove-Item -Recurse -Force $TempDir -ErrorAction SilentlyContinue
        }
    }
    default {
        Write-Error "unknown Kaggle client task: $Task"
        exit 2
    }
}
