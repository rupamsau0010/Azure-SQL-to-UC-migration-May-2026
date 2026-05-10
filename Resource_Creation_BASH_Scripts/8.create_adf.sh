az datafactory create \
  --factory-name migration-adf-uc \
  --resource-group rg-migration-dbx \
  --location centralindia 

# Get ADF Managed Identity principal ID (need for KV access)
ADF_PRINCIPAL_ID=$(az datafactory show \
  --factory-name migration-adf-uc \
  --resource-group rg-migration-dbx \
  --query identity.principalId -o tsv)

echo "ADF MSI Principal ID: $ADF_PRINCIPAL_ID"

# Grant ADF MSI → Key Vault (Get + List secrets)
az keyvault set-policy \
  --name migration-kv-uc \
  --object-id $ADF_PRINCIPAL_ID \
  --secret-permissions get list