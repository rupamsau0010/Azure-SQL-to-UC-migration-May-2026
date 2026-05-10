# Install Databricks extension for Azure CLI if not present
az extension add --name databricks --upgrade

# Create Access Connector
az databricks access-connector create \
  --name migration-access-connector \
  --resource-group rg-migration-dbx \
  --location centralindia \
  --identity-type SystemAssigned

# Get the Access Connector's MSI principal ID
AC_PRINCIPAL_ID=$(az databricks access-connector show \
  --name migration-access-connector \
  --resource-group rg-migration-dbx \
  --query identity.principalId -o tsv)

echo "Access Connector MSI: $AC_PRINCIPAL_ID"