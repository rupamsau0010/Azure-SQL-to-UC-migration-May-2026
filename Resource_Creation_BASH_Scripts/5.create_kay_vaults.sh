az keyvault create \
  --name migration-kv-uc \
  --resource-group rg-migration-dbx \
  --location centralindia \
  --sku standard \
  --enable-rbac-authorization false

# Verify
az keyvault show \
  --name migration-kv-uc \
  --query "{name:name, vaultUri:properties.vaultUri}" -o table