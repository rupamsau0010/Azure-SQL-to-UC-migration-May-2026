az databricks workspace create \
  --name migration-dbx-ws \
  --resource-group rg-migration-dbx \
  --location centralindia \
  --sku premium

# This takes 3–5 minutes. When complete:
az databricks workspace show \
  --name migration-dbx-ws \
  --resource-group rg-migration-dbx \
  --query "{name:name, url:workspaceUrl, sku:sku.name}" -o table