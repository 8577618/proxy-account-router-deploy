param(
  [Parameter(Mandatory=$true)]
  [ValidateSet("summary", "status", "account", "lock-region", "unlock-region", "set-fixed-proxy", "clear-fixed-proxy", "switch", "health-check", "reload")]
  [string]$Action,

  [string]$Account,
  [int]$Port,
  [string]$Region,
  [switch]$Strict,
  [string[]]$FallbackRegions = @(),
  [string]$ProxyType = "socks5",
  [string]$ProxyUrl,
  [string]$ProxyHost,
  [int]$ProxyPort,
  [string]$ProxyUsername,
  [string]$ProxyPassword,
  [string]$BaseUrl = "http://127.0.0.1:19080"
)

$ErrorActionPreference = "Stop"

function Invoke-JsonPost($Path, $Body = $null) {
  $uri = "$BaseUrl$Path"
  if ($null -eq $Body) {
    Invoke-WebRequest -UseBasicParsing -Method Post -Uri $uri | Select-Object -ExpandProperty Content
    return
  }
  Invoke-WebRequest -UseBasicParsing -Method Post -Uri $uri -ContentType "application/json" -Body ($Body | ConvertTo-Json -Depth 8) |
    Select-Object -ExpandProperty Content
}

function Get-Status {
  (Invoke-WebRequest -UseBasicParsing -Uri "$BaseUrl/status").Content | ConvertFrom-Json
}

function Resolve-AccountName {
  param([string]$Name, [int]$ListenPort)
  if ($Name) {
    return $Name
  }
  if ($ListenPort -le 0) {
    throw "Provide -Account or -Port."
  }
  $status = Get-Status
  $acct = $status.accounts.PSObject.Properties.Value | Where-Object { $_.listen_port -eq $ListenPort } | Select-Object -First 1
  if ($null -eq $acct) {
    throw "No account is listening on port $ListenPort."
  }
  return $acct.name
}

switch ($Action) {
  "summary" {
    (Invoke-WebRequest -UseBasicParsing -Uri "$BaseUrl/summary").Content
  }
  "status" {
    (Invoke-WebRequest -UseBasicParsing -Uri "$BaseUrl/status").Content
  }
  "account" {
    $name = Resolve-AccountName -Name $Account -ListenPort $Port
    $encoded = [System.Uri]::EscapeDataString($name)
    (Invoke-WebRequest -UseBasicParsing -Uri "$BaseUrl/admin/accounts/$encoded").Content
  }
  "lock-region" {
    $name = Resolve-AccountName -Name $Account -ListenPort $Port
    if (-not $Region) {
      throw "Provide -Region."
    }
    $encoded = [System.Uri]::EscapeDataString($name)
    Invoke-JsonPost "/admin/accounts/$encoded/lock-region" @{
      region = $Region
      strict = [bool]$Strict
      fallback_regions = $FallbackRegions
    }
  }
  "unlock-region" {
    $name = Resolve-AccountName -Name $Account -ListenPort $Port
    $encoded = [System.Uri]::EscapeDataString($name)
    Invoke-JsonPost "/admin/accounts/$encoded/unlock-region"
  }
  "set-fixed-proxy" {
    $name = Resolve-AccountName -Name $Account -ListenPort $Port
    if (-not $ProxyUrl -and (-not $ProxyHost -or $ProxyPort -le 0)) {
      throw "Provide -ProxyUrl or -ProxyHost and -ProxyPort."
    }
    $encoded = [System.Uri]::EscapeDataString($name)
    Invoke-JsonPost "/admin/accounts/$encoded/set-fixed-proxy" @{
      type = $ProxyType
      url = $ProxyUrl
      host = $ProxyHost
      port = $ProxyPort
      username = $ProxyUsername
      password = $ProxyPassword
    }
  }
  "clear-fixed-proxy" {
    $name = Resolve-AccountName -Name $Account -ListenPort $Port
    $encoded = [System.Uri]::EscapeDataString($name)
    Invoke-JsonPost "/admin/accounts/$encoded/clear-fixed-proxy"
  }
  "switch" {
    $name = Resolve-AccountName -Name $Account -ListenPort $Port
    $encoded = [System.Uri]::EscapeDataString($name)
    Invoke-JsonPost "/admin/accounts/$encoded/switch"
  }
  "health-check" {
    Invoke-JsonPost "/admin/health-check"
  }
  "reload" {
    Invoke-JsonPost "/admin/reload"
  }
}
