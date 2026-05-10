# Storage account — globally unique name — lowercase only, no dashes
az storage account create \
  --name migrationadlsuc \
  --resource-group rg-migration-dbx \
  --location centralindia \
  --sku Standard_LRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true \
  --access-tier Hot \
  --min-tls-version TLS1_2

# Verify HNS is on (must see "isHnsEnabled": true)
az storage account show \
  --name migrationadlsuc \
  --resource-group rg-migration-dbx \
  --query "isHnsEnabled"