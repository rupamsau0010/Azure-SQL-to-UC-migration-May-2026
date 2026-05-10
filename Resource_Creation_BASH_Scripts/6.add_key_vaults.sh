# Store ADLS account name
az keyvault secret set \
  --vault-name migration-kv-uc \
  --name adls-account-name \
  --value "migrationadlsuc"

# Store ADLS account key
az keyvault secret set \
  --vault-name migration-kv-uc \
  --name adls-account-key \
  --value "$STORAGE_KEY"

# Verify
az keyvault secret list \
  --vault-name migration-kv-uc \
  --query "[].name" -o table

# Build JDBC connection string
SQL_CONN="jdbc:sqlserver://migration-sql-uc.database.windows.net:1433;database=migration-db;user=XXXXXXXXXXXXXX;password=XXXXXXXXXXXXXXX;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

az keyvault secret set \
  --vault-name migration-kv-uc \
  --name sql-connection-string \
  --value "$SQL_CONN"

# Also store password separately (ADF linked service needs it)
az keyvault secret set \
  --vault-name migration-kv-uc \
  --name sql-admin-password \
  --value "XXXXXXXXXXXXXXXX"