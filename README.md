# DynamoDB Lambda Monitor

Monitor DynamoDB with Lambda to increase and decrease throughput as needed

## Installation
- Create a Lambda function in your AWS account
- Set it to use the minimum memory (128MB)
- Select a role that has dynamodb access
- Enter the code in from /index.js
- Set the table name to match your DynamoDB table
- Set a schedule in "Event Sources" for how often you want it to run

## Tips
- You can also set it to run on a -ReadThrottles event on your index
- Play with the settings at your own risk and please keep an eye on the changes until you are comfortable
- Actions will be logged to Cloudwatch

## Notes
- A DynamoDB table can be scaled down only 4 times per 24 hours (resetting at midnight UTC), so this script will only scale down a maximum of once per 6 hours so that your 'credits' don't get used up all at once.
