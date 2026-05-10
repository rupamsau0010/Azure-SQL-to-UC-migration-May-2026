az login
# Browser opens → sign in with Azure Student account

# List subscriptions (find your student sub name)
az account list --output table

# Set it
az account set --subscription "Azure for Students"

# Confirm
az account show --query "{name:name, id:id}" -o table