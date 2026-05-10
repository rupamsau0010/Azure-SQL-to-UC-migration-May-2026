# Create SQL Server (logical server — not a VM)
az sql server create \
  --name migration-sql-uc \
  --resource-group rg-migration-dbx \
  --location centralindia \
  --admin-user XXXXXXXXXXX \
  --admin-password "XXXXXXXXXXXXXXXX"

# Create database on Basic tier (cheapest: $4.99/month, 5 DTU, 2GB)
az sql db create \
  --resource-group rg-migration-dbx \
  --server migration-sql-uc \
  --name migration-db \
  --service-objective Basic \

# Allow Azure services (ADF needs this)
az sql server firewall-rule create \
  --resource-group rg-migration-dbx \
  --server migration-sql-uc \
  --name AllowAzureServices \
  --start-ip-address 0.0.0.0 \
  --end-ip-address 0.0.0.0

# Allow YOUR current IP (for loading data from local machine)
MY_IP=$(curl -s https://api.ipify.org)
echo "Your IP: $MY_IP"

az sql server firewall-rule create \
  --resource-group rg-migration-dbx \
  --server migration-sql-uc \
  --name AllowMyIP \
  --start-ip-address $MY_IP \
  --end-ip-address $MY_IP