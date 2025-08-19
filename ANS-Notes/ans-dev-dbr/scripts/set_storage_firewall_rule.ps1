[CmdletBinding()]
Param
(
    [String] [Parameter(Mandatory = $true)] $AccountName,
    [String] [Parameter(Mandatory = $true)] $ResourceGroupName
)

Import-Module 'Az.Storage'

$attempts = 10
foreach($count in 1..$attempts) {
    try {
        $AgentIPAddress = (New-Object net.webclient).downloadstring("http://checkip.dyndns.com") -replace "[^\d\.]"
        break
    }
    catch{
        if($count -eq ($attempts - 1)) {
            throw $Error[0]
        }
        Start-Sleep -Seconds 10
        continue
    }
}

Add-AzStorageAccountNetworkRule `
    -ResourceGroupName $ResourceGroupName `
    -Name $AccountName `
    -IPAddressOrRange $AgentIPAddress

Write-Host "##vso[task.setvariable variable=AgentIPAddress;isOutput=true]$AgentIPAddress"