az group create \
  --name rg-migration-dbx \
  --location eastus

# Verify
az group show --name rg-migration-dbx --query "{name:name, location:location}" -o table