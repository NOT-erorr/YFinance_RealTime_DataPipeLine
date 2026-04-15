param(
    [string]$ComposeFile = "docker-compose.yaml",
    [switch]$ShowSampleRows,
    [int]$SampleLimit = 5,
    [switch]$SkipDuckDbLockHandling
)

$ErrorActionPreference = "Stop"

function Invoke-Compose {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Args
    )

    & docker compose -f $ComposeFile @Args
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed: docker compose -f $ComposeFile $($Args -join ' ')"
    }
}

function Ensure-Service-Running {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ServiceName
    )

    $containerId = Invoke-Compose -Args @("ps", "-q", $ServiceName)
    if (-not $containerId) {
        throw "Service '$ServiceName' is not running. Start stack first: docker compose -f $ComposeFile up -d --build"
    }
}

Write-Host "=== Pipeline Data Check ===" -ForegroundColor Cyan
Write-Host "Compose file: $ComposeFile"
Write-Host ""

Write-Host "[1] Service status" -ForegroundColor Yellow
Invoke-Compose -Args @("ps")
Write-Host ""

Ensure-Service-Running -ServiceName "postgres"
Ensure-Service-Running -ServiceName "datapipeline"

Write-Host "[2] PostgreSQL row count" -ForegroundColor Yellow
Invoke-Compose -Args @(
    "exec", "-T", "postgres",
    "psql", "-U", "admin", "-d", "kraf_db",
    "-c", "SELECT COUNT(*) AS total_rows FROM stock_prices;"
)
Write-Host ""

if ($ShowSampleRows) {
    Write-Host "[3] PostgreSQL sample rows" -ForegroundColor Yellow
    Invoke-Compose -Args @(
        "exec", "-T", "postgres",
        "psql", "-U", "admin", "-d", "kraf_db",
        "-c", "SELECT symbol, price, datetime FROM stock_prices ORDER BY datetime DESC LIMIT $SampleLimit;"
    )
    Write-Host ""
}

Write-Host "[4] DuckDB row count" -ForegroundColor Yellow
$duckDbReadOnlyCmd = "import duckdb; con=duckdb.connect('data/yf_analytics.duckdb', read_only=True); print(con.execute('SELECT COUNT(*) AS total_rows FROM stock_prices').fetchone()[0])"

$duckDbOutput = $null
try {
    $duckDbOutput = Invoke-Compose -Args @(
        "run", "--rm", "--no-deps", "duckdb-check",
        "python", "-c", $duckDbReadOnlyCmd
    )
    Write-Host "DuckDB total_rows: $duckDbOutput"
}
catch {
    if ($SkipDuckDbLockHandling) {
        throw "DuckDB check failed and lock handling is disabled."
    }

    Write-Host "DuckDB appears locked by writer. Running safe fallback (stop datapipeline -> query -> start datapipeline)." -ForegroundColor DarkYellow
    Invoke-Compose -Args @("stop", "datapipeline") | Out-Null

    try {
        $duckDbOutput = Invoke-Compose -Args @(
            "run", "--rm", "--no-deps", "duckdb-check",
            "python", "-c",
            "import duckdb; con=duckdb.connect('data/yf_analytics.duckdb'); print(con.execute('SELECT COUNT(*) AS total_rows FROM stock_prices').fetchone()[0])"
        )
        Write-Host "DuckDB total_rows: $duckDbOutput"
    }
    finally {
        Invoke-Compose -Args @("start", "datapipeline") | Out-Null
    }
}

Write-Host ""
Write-Host "Data check completed." -ForegroundColor Green
