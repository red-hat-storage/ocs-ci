import boto3

"""
this script prints all running instances across all regions in aws account
"""
client = boto3.client('ec2', region_name='us-east-1')

ec2_regions = [region['RegionName'] for region in client.describe_regions()['Regions']]

for region in ec2_regions:
    conn = boto3.resource('ec2', region_name=region)
    instances = conn.instances.filter()
    for instance in instances:
        if instance.state["Name"] == "running":
            print(instance.id, instance.instance_type, region)
