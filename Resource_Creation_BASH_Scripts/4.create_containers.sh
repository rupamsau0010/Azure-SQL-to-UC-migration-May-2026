# Get storage key for container creation
STORAGE_KEY=$(az storage account keys list \
  --account-name migrationadlsuc \
  --resource-group rg-migration-dbx \
  --query "[0].value" -o tsv)

# Create containers
for container in migration-raw migration-processed migration-quarantine unity-catalog-metastore; do
  az storage container create \
    --name $container \
    --account-name migrationadlsuc \
    --account-key $STORAGE_KEY
  echo "Created: $container"
done

# Verify
az storage container list \
  --account-name migrationadlsuc \
  --account-key $STORAGE_KEY \
  --query "[].name" -o table