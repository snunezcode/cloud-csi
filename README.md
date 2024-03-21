

# Cloud - Current State Investigator - Cloud-CSI

> **Disclaimer:** The sample code; software libraries; command line tools; proofs of concept; templates; or other related technology (including any of the foregoing that are provided by our personnel) is provided to you as AWS Content under the AWS Customer Agreement, or the relevant written agreement between you and AWS (whichever applies). You are responsible for testing, securing, and optimizing the AWS Content, such as sample code, as appropriate for production grade use based on your specific quality control practices and standards. Deploying AWS Content may incur AWS charges for creating or using AWS chargeable resources, such as running Amazon EC2 instances, using Amazon CloudWatch or Amazon Cognito.



## What is Cloud - Current State Investigator ?

Cloud-CSI is a tool to help you and AWS collaboratively look at the performance metrics and configurations of your current AWS databases.


## How install Cloud-CSI ?

pip3 install -r requirements.txt 



## How execute Cloud-CSI ?


> python3 exporter.py -t <service_type> -r <region> -l <list_of_resources> -p <period_in_minutes> -i <interval_in_days>

Example

> python3 exporter.py -t rds -r us-east-1 -l rds-pgs-01 -p 300 -i 30
  
  