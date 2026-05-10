echo "=== Phase 0 Resource Check ==="

echo "Resource Group:"
az group show --name rg-migration-dbx --query "properties.provisioningState" -o tsv

echo "ADLS Gen2:"
az storage account show --name migrationadlsuc --resource-group rg-migration-dbx --query "isHnsEnabled" -o tsv

echo "Key Vault:"
az keyvault show --name migration-kv-uc --query "properties.vaultUri" -o tsv

echo "Azure SQL:"
az sql server show --name migration-sql-uc --resource-group rg-migration-dbx --query "fullyQualifiedDomainName" -o tsv

echo "ADF:"
az datafactory show --factory-name migration-adf-uc --resource-group rg-migration-dbx --query "provisioningState" -o tsv

echo "Access Connector:"
az databricks access-connector show --name migration-access-connector --resource-group rg-migration-dbx --query "provisioningState" -o tsv

echo "Databricks Workspace:"
az databricks workspace show --name migration-dbx-ws --resource-group rg-migration-dbx --query "provisioningState" -o tsv

echo "=== All should say: Succeeded ==="