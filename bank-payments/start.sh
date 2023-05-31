#!/bin/bash

echo -e "\033[32mCreating container..."
localstack start -d 
echo -e "\033[32m[Done!]"

echo -e "\033[32mCreating table template...\033[0m"
cd bank-payments-project/.chalice/ &&
awslocal cloudformation deploy --template-file dynamodb_cf_template.yaml --stack-name "my-stack"
echo -e "\033[32m[Done!]"
echo -e "\033[32mDeploying application...\033[0m"
cd ..
deploy_output=$(chalice-local deploy)

api_url=$(echo "$deploy_output" | grep -oP '(?<=Rest API URL: ).*')
api_id=$(echo "$api_url" | sed 's/.*\/\(.*\)\.execute.*/\1/')

echo "Rest API URL: $api_url"
echo "API ID: $api_id"

echo -e "\033[32m[Done!]\033[0m"
