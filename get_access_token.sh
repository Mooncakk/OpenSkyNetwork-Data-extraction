export CLIENT_ID="ada_airflow-api-client" && \
export CLIENT_SECRET="JOa7qkZ6Io7jjtpgxs3ZUVpA3ZBW0Bph" && \

export TOKEN=$(curl -X POST "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=$CLIENT_ID" \
  -d "client_secret=$CLIENT_SECRET" | jq -r .access_token) && \

echo '{"token": "'$TOKEN'"}' > dags/token.json