AC_PRINCIPAL_ID=$(az databricks access-connector show \
  --name migration-access-connector \
  --resource-group rg-migration-dbx \
  --query identity.principalId -o tsv)

echo "Access Connector MSI: $AC_PRINCIPAL_ID"

# Get ADLS resource ID
ADLS_RESOURCE_ID=$(az storage account show \
  --name migrationadlsuc \
  --resource-group rg-migration-dbx \
  --query id -o tsv)

# Assign role at storage account level (covers all containers)
az role assignment create \
  --assignee $AC_PRINCIPAL_ID \
  --role "Storage Blob Data Contributor" \
  --scope $ADLS_RESOURCE_ID

echo "Role assigned. Propagation takes ~2 min."