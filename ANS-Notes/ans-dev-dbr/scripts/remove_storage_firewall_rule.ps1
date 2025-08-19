[CmdletBinding()]
param
(
    [String] [Parameter(Mandatory = $true)] $AccountName,
    [String] [Parameter(Mandatory = $true)] $ResourceGroupName,
    [String] [Parameter(Mandatory = $true)] $AgentIPAddress

)

Import-Module 'Az.Storage'

Remove-AzStorageAccountNetworkRule `
    -ResourceGroupName $ResourceGroupName `
    -AccountName $AccountName `
    -IPAddressOrRange $AgentIPAddress