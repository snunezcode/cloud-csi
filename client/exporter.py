
############################|-       Import Section     -|####################################

import numpy as np
import json
import os
import subprocess
import time
import calendar
import logging
import hashlib
import pytz
from pytz import timezone


from datetime import date, datetime, timedelta
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import uuid
import sys, getopt


############################|-
############################|-       Class : classFile     -|####################################
############################|-


class classFile():
    
    def __init__(self):
        # store the event
        pass
        
    def write_all(self,params):
        file_object = open(params["file_name"], params["open_mode"])
        file_object.write(params["content"])
        file_object.close()
        
    
    def write_json(self,params):
        file_object = open(params["file_name"], params["open_mode"])
        json.dump(
                    params["content"],
                    file_object,
                )
        file_object.close()
    
    
    def create_folder(self,path):
        if not os.path.exists(path):
            os.mkdir(path)
        else:
            pass

    def read_all(self, params):
        file = open(params['file_name'])
        content = file.read()
        file.close()
        return content
    
    def clean_directory(self,directory):
        # Clean-up output folder
        subprocess.call(
            ["rm", "-rf", directory],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        
    def compress_directory(self,directory, archive_uid):
        archive_file = archive_uid + ".tar.gz"
        logging.info(f'Creating archive file : {archive_file}')
        subprocess.call(
                        [
                            "tar",
                            "-zcvf",
                            f"./{archive_file}",
                            directory,
                        ],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.STDOUT,
        )
        
        return archive_file
    
    
    def default(self,o):
        if isinstance(o, (date, datetime)):
            return o.isoformat()

        


############################|-
############################|-       Class : classCloudwatchExporter     -|####################################
############################|-


class classCloudwatchExporter():
    
    # constructor
    def __init__(self, params):
        self.session = boto3.Session(region_name=params["region"])
        self.cw_client = self.session.client("cloudwatch")
        self.pi_client = self.session.client("pi")
        self.file = classFile()
        self.output_path = "output/"
        self.file.create_folder(self.output_path)
        
        
    def format_dimension(self, dimensions: list ) -> list:
        dimension_result = []
        for dimension in dimensions:
            dimension_result.append({"Name": dimension['name'], "Value": f"{dimension['value']}"})
        return dimension_result
    

    def dataframe_name(self, dimensions: dict) -> str:
        name = ""
        for dimension in dimensions:
            name = name +  dimension["Value"] + "."
        return name[:-1]
        
    
    def format_metric_query(  
        self, 
        namespace : str,
        metrics: dict,  
        dimension: dict,
        periods: list = [60, 300],  
    ) -> dict:
        metric_data_query = []
        for period in periods:
            for metric in metrics:
                metric_data_query.append(
                    {
                        "Id": metric["metric_name"].lower(),
                        "MetricStat": {
                            "Metric": {
                                "Namespace": namespace,
                                "MetricName": metric["metric_name"],
                                "Dimensions": dimension,
                            },
                            "Period": period,
                            "Stat": metric["stat"],
                        },
                        "Label": metric["metric_name"],
                        "ReturnData": True,
                    }
                )
        return metric_data_query
    
    
    def get_metric_data(  
        self, 
        namespace : str,
        metrics: dict,  
        dimension: dict,
        period: int,
        interval : int,
        metadata : dict,
    ) -> dict:  
        try:
            start_date, end_date = self.get_start_end_date(interval)
            results = {}
            metric_data_query = self.format_metric_query(namespace,metrics, dimension, [period])
            response = self.cw_client.get_metric_data(
                MetricDataQueries=metric_data_query,
                StartTime=start_date,
                EndTime=end_date
            )
    
            for metric in response["MetricDataResults"]:
                results[metric["Label"]] = {"Timestamps": [], "Values": []}
                results[metric["Label"]]["Values"] += metric["Values"]
                results[metric["Label"]]["Timestamps"] += metric["Timestamps"]
            while "NextToken" in response:
                response = self.cw_client.get_metric_data(
                    MetricDataQueries=metric_data_query,
                    StartTime=start_date,
                    EndTime=end_date,
                    NextToken=response["NextToken"],
                )
                for metric in response["MetricDataResults"]:
                    results[metric["Label"]]["Values"] += metric["Values"]
                    results[metric["Label"]]["Timestamps"] += metric["Timestamps"]
    
            time_series_pd = []
            for res in results:
                time_series_pd.append(
                    pd.Series(
                        results[res]["Values"],
                        name=res,
                        dtype="float64",
                        index=results[res]["Timestamps"],
                    )
                )
    
            result = pd.concat([i for i in time_series_pd], axis=1)
            
            if result.empty:
                return_value = {
                    "name": self.dataframe_name(dimension), 
                    "dimensions" : dimension, 
                    "metadata": metadata, 
                    "df": None, 
                    "is_null": True
                }
            else:
                # result.index = pd.to_datetime(result.index)
                # https://github.com/pandas-dev/pandas/issues/39537
                result.index = pd.to_datetime(result.index).tz_convert("UTC")
                result = result.fillna(0)
                return_value = {
                    "name": self.dataframe_name(dimension),
                    "dimensions" : dimension,
                    "metadata": metadata,
                    "df": result.to_json(orient="table"),
                    "is_null": False
                }
            return return_value
            
        except self.cw_client.exceptions.InvalidParameterValueException as e:
            print(e)
            print(metrics)
            pass
        except self.cw_client.exceptions.InternalServiceFault as e:
            print(e)
            print(metrics)
            pass
        
        except Exception as error:
            print("An exception occurred:", error)
            pass
        
    
    def get_pi_metrics(self, resource_id, service_type, query, interval, period, properties):
        
        utc=pytz.UTC
        
        current_date = datetime.now()
        current_date = current_date.replace(tzinfo=utc)
        
        start_date = datetime.now()
        start_date = start_date.replace(tzinfo=utc)
        
        start_date = start_date - timedelta(days=interval) 
        end_date = start_date + timedelta(hours=5)
        
        metrics = []
        results = {}
        
        while end_date <= current_date:
                
            response = self.pi_client.get_resource_metrics(
                                                        ServiceType=service_type,
                                                        Identifier=resource_id,
                                                        MetricQueries=query,
                                                        StartTime=start_date,
                                                        EndTime=end_date,
                                                        PeriodInSeconds=period
            )
            
            for record in response['MetricList']:
                hash_object = hashlib.sha1(str(record["Key"]).encode())
                hash_key = hash_object.hexdigest()
                if hash_key not in results:
                    results[hash_key] = { "Measure" : { "measure" : record['Key']['Metric'] }, "Tags" : { "tags" : self.get_metric_key_dimension(record['Key']) }, "DataPoints": [] }
                results[hash_key]["DataPoints"] += record['DataPoints']
            
            end_date = response["AlignedEndTime"]
            start_date = end_date
            end_date = end_date + timedelta(hours=5)

        
        for key in results.keys():
            timestamps = np.array([ x['Timestamp'] for x in results[key]['DataPoints']])
            values = np.array([ x['Value'] if x.get('Value')!=None else 0 for x in results[key]['DataPoints']])
            data_points = pd.Series(
                        values,
                        name="value",
                        dtype="float64",
                        index=timestamps,
                    )
            data_points.index = pd.to_datetime(data_points.index).tz_convert("UTC")
            data_points = data_points.fillna(0)
            data_points = data_points.to_json(orient="table")
            metrics.append({ **results[key]['Measure'],**results[key]['Tags'], "DataPoints" : data_points })
        
        
        return_value = {
                    "metadata": { "properties" :  properties },
                    "metrics" : metrics
        }
        self.file.write_json({ "file_name" : self.output_path + self.get_uid(23) + ".rpi", "open_mode" : "w", "content" : return_value })
    
    
    def get_metric_key_dimension(self,response):
        dimensions = []
        if 'Dimensions' in response:
            for key in response['Dimensions'].keys():
                dimensions.append({ f'{key.replace(".","_")}' : response['Dimensions'][key] })
        else:
            dimensions.append({ "level" : "base" })
        return dimensions
    
    
            
        
        
        
    def get_start_end_date(self, interval: int) -> str:
        end_date = datetime.now() - timedelta(minutes=5)
        time_delta = timedelta(days=interval)
        start_date = end_date - time_delta
        return start_date.isoformat(), end_date.isoformat()
        
    
    def replace_template_parameters(self, parameters, template) -> str:
        for parameter in parameters:
            template = template.replace(parameter['name'], parameter['value'])
        return template
    
    
    def get_metrics(self, template, properties ):
        metric_list = json.loads(template)
        for metric in metric_list['metrics']:
            result = self.get_metric_data(
                            metric['namespace'], 
                            metric['metrics'], 
                            metric['dimensions'], 
                            metric['period'], 
                            metric['interval'],
                            { "properties" :  properties }
            )
            if (result['is_null']==False):
                self.file.write_json({ "file_name" : self.output_path + self.get_uid(23) + ".mtr", "open_mode" : "w", "content" : result })
            
    def get_uid(self,length):
        return str(uuid.uuid4())[:length]
        
    
    def default(self,o):
        if isinstance(o, (date, datetime)):
            return o.isoformat()    
    
    def json_serialize(self,obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return obj


############################|-
############################|-       Class : classDynamoDB     -|####################################
############################|-

class classDynamoDB():
    
    # constructor
    def __init__(self, params):
        self.tables = []
        self.region = params["region"]
        self.period = params["period"]
        self.interval = params["interval"]
        self.indexes = [] 
        self.client = boto3.Session().client("dynamodb",region_name=self.region)
        self.file = classFile()
        self.clw = classCloudwatchExporter({ "region" : self.region})
        self.output_path = "output/"
        self.object_class = "dynamodb"
        self.export_id = datetime.now().strftime("%Y%m%d%H%M%S")
        
        for table in params["tables"]:
            self.tables.append({ "name" : table, "uid" : self.clw.get_uid(23) })    
        
        
    def gather_metadata(self,table,uid):
        result = self.client.describe_table(TableName=table)
        for index in result['Table']['GlobalSecondaryIndexes']:
            self.indexes.append({ "table_name" : table, "index_name" : index["IndexName"], "uid" : uid })
        return result
        
    
    def export_metrics(self):
        logging.info(f'Exporting metrics ...' )
        self.file.clean_directory(self.output_path)
        self.file.create_folder(self.output_path)
        
        # Export Information
        start_date, end_date = self.clw.get_start_end_date(int(self.interval))
        export_parameters = { 
                            "exportId" : self.export_id, 
                            "exportClass" : self.object_class,
                            "objects" : self.tables,
                            "region" : self.region,
                            "interval" : self.interval, 
                            "period" : self.period, 
                            "startDate" : start_date, 
                            "endDate" : end_date
        }
        
        # Gather Metadata
        metadata = []
        for table in self.tables:
            metadata.append(
                                {
                                "resourceType" : self.object_class, 
                                "resourceName" : table['name'], 
                                "resourceId" : table['uid'], 
                                "resourceDefinition" : self.gather_metadata(table['name'],table['uid']) 
                                }
            )
        self.file.write_all({ "file_name" : self.output_path + "metadata.json", "open_mode" : "w", "content" : json.dumps({ "exportParameters" : export_parameters, "metadata" : metadata}, indent=4, default=self.default) })
        
        
        # Gather Metrics - Tables
        table_template = self.file.read_all({"file_name" : 'templates/dynamodb.table.json' })
        for table in self.tables:
            logging.info(f'Processing Object : Table : {table["name"]}' )
            template = self.clw.replace_template_parameters(
                                                            [
                                                                { "name" :  "<table_name>", "value" : table['name'] },
                                                                { "name" :  "<period>", "value" : self.period },
                                                                { "name" :  "<interval>", "value" : self.interval },
                                                            ],
                                                            table_template
            )
            properties = [
                            { "Name" : "ObjectType", "Value" : "DynamoDBTable" }, 
                            { "Name" : "LevelType", "Value" : "table"  },
                            { "Name" : "Uid", "Value" : table['uid']  },
                            { "Name" : "GlobalSecondaryIndexName", "Value" : '<base>'  },
                            
            ]
            self.clw.get_metrics(template, properties)
            
        
        # Gather Metrics - Indexes
        index_template = self.file.read_all({"file_name" : 'templates/dynamodb.index.json' })
        for index in self.indexes:
            
            logging.info(f'Processing Object : Index :  {index["table_name"]}.{index["index_name"]}' )
            template = self.clw.replace_template_parameters(
                                                            [
                                                                    { "name" :  "<table_name>", "value" : index['table_name'] }, 
                                                                    { "name" :  "<index_name>", "value" : index['index_name'] },
                                                                    { "name" :  "<period>", "value" : self.period },
                                                                    { "name" :  "<interval>", "value" : self.interval },
                                                            ],
                                                            index_template
                                                            )
            properties = [
                            { "Name" : "ObjectType", "Value" : "DynamoDBIndex" }, 
                            { "Name" : "LevelType", "Value" : "gsi"  },
                            { "Name" : "Uid", "Value" : table['uid']  }
            ]
            self.clw.get_metrics(template,properties)
            
        
        archive_file_id = self.file.compress_directory(self.output_path, self.export_id + "." + self.object_class)
        return archive_file_id
        
        
    def default(self,o):
        if isinstance(o, (date, datetime)):
            return o.isoformat()
        



    
############################|-
############################|-       Class : classEC2     -|####################################
############################|-

class classEC2():
    
    # constructor
    def __init__(self, params):
        self.client = boto3.client('ec2', region_name=params["region"])
        
        
    
    def gather_instance_type(self,instance_type):
        result = self.client.describe_instance_types(InstanceTypes=[instance_type])
        return result





############################|-
############################|-       Class : classRDS     -|####################################
############################|-

class classRds():
    
    # constructor
    def __init__(self, params):
        self.instances = []
        self.region = params["region"]
        self.period = params["period"]
        self.interval = params["interval"]
        self.client = boto3.Session().client("rds",region_name=params["region"])
        self.file = classFile()
        self.clw = classCloudwatchExporter({ "region" : self.region})
        self.ec2 = classEC2({ "region" : params["region"]})
        self.output_path = "output/"
        self.object_class = "rds"
        self.export_id = datetime.now().strftime("%Y%m%d%H%M%S")
        
        if params["instances"][0] == "all":
            for instance in self.list_all_instances():
                self.instances.append({ "name" : instance, "uid" : self.clw.get_uid(23) })    
        else:
            for instance in params["instances"]:
                self.instances.append({ "name" : instance, "uid" : self.clw.get_uid(23) })    
    

    def list_all_instances(self):
        instances = []
        next_token = None
        while True:
            params = {}
            if next_token:
                params['Marker'] = next_token
            params['Filters'] = [{'Name': 'engine','Values': ['mysql', 'postgres', 'sqlserver-ee', 'sqlserver-se', 'sqlserver-ex', 'sqlserver-web', 'aurora-mysql', 'aurora-postgresql']}]  
            
            response = self.client.describe_db_instances(**params)
            
            for instance in response['DBInstances']:
                instances.append(instance['DBInstanceIdentifier'])
            
            if 'Marker' in response:
                next_token = response['Marker']
            else:
                break
        return instances

    def gather_metadata(self,instance):
        instance_metadata = self.client.describe_db_instances(DBInstanceIdentifier=instance)
        if ( instance_metadata['DBInstances'][0]['DBInstanceClass'] != "db.serverless"):
            instance_type = self.ec2.gather_instance_type(instance_metadata['DBInstances'][0]['DBInstanceClass'].replace("db.",""))
        else:
            instance_type = {}
        result = {**instance_metadata, **instance_type}
        return result
        
        
    
    def export_metrics(self):
        logging.info('Exporting metrics...')
        self.file.clean_directory(self.output_path)
        self.file.create_folder(self.output_path)
        
        # Export Information
        start_date, end_date = self.clw.get_start_end_date(int(self.interval))
        export_parameters = { 
                            "exportId" : self.export_id, 
                            "exportClass" : self.object_class,
                            "objects" : self.instances,
                            "region" : self.region,
                            "interval" : self.interval, 
                            "period" : self.period, 
                            "startDate" : start_date, 
                            "endDate" : end_date
        }
        
        # Gather Metadata
        metadata = []
        for key, val in enumerate(self.instances):
            instance_metadata = self.gather_metadata(self.instances[key]['name']) 
            self.instances[key]={
                                    **self.instances[key], 
                                    **{
                                        "DbiResourceId" : instance_metadata['DBInstances'][0]['DbiResourceId'], 
                                        "PerformanceInsightsRetentionPeriod" : instance_metadata['DBInstances'][0]['PerformanceInsightsRetentionPeriod'] if "PerformanceInsightsRetentionPeriod" in instance_metadata['DBInstances'][0] else "0",
                                        "PerformanceInsightsEnabled" : str(instance_metadata['DBInstances'][0]['PerformanceInsightsEnabled']),
                                        "DBInstanceClass" : instance_metadata['DBInstances'][0]['DBInstanceClass'],
                                        "Engine" : instance_metadata['DBInstances'][0]['Engine'],
                                        "IsCluster" : "True" if "DBClusterIdentifier" in instance_metadata['DBInstances'][0] else "False",
                                        "DBClusterIdentifier" : instance_metadata['DBInstances'][0]['DBClusterIdentifier'] if "DBClusterIdentifier" in instance_metadata['DBInstances'][0] else instance_metadata['DBInstances'][0]['DBInstanceIdentifier'],
                                        "EngineVersion" : instance_metadata['DBInstances'][0]['EngineVersion'],
                                        "AvailabilityZone" : instance_metadata['DBInstances'][0]['AvailabilityZone'],
                                        "EngineVersion" : instance_metadata['DBInstances'][0]['EngineVersion'],
                                        "PubliclyAccessible" : str(instance_metadata['DBInstances'][0]['PubliclyAccessible']),
                                        "MultiAZ" : instance_metadata['DBInstances'][0]['MultiAZ'],
                                        "AllocatedStorage" : str(instance_metadata['DBInstances'][0]['AllocatedStorage']) if "AllocatedStorage" in instance_metadata['DBInstances'][0] else "0",
                                        "Iops" : str(instance_metadata['DBInstances'][0]['Iops']) if "Iops" in instance_metadata['DBInstances'][0] else "0",
                                        "StorageType" : instance_metadata['DBInstances'][0]['StorageType'] if "StorageType" in instance_metadata['DBInstances'][0] else "<undefined>",
                                        "StorageThroughput" : str(instance_metadata['DBInstances'][0]['StorageThroughput']) if "StorageThroughput" in instance_metadata['DBInstances'][0] else "0",
                                        "BackupRetentionPeriod" : str(instance_metadata['DBInstances'][0]['BackupRetentionPeriod']) if "BackupRetentionPeriod" in instance_metadata['DBInstances'][0] else "0",
                                        "CPUTotal" : str(instance_metadata['InstanceTypes'][0]['VCpuInfo']['DefaultVCpus']) if "DefaultVCpus" in instance_metadata['InstanceTypes'][0]['VCpuInfo'] else "0",
                                        "MemoryTotalMB" : str(instance_metadata['InstanceTypes'][0]['MemoryInfo']['SizeInMiB']) if "SizeInMiB" in instance_metadata['InstanceTypes'][0]['MemoryInfo'] else "0",
                                        "IOBaselineBandwidthInMbps" : str(instance_metadata['InstanceTypes'][0]['EbsInfo']['EbsOptimizedInfo']['BaselineBandwidthInMbps']) if "BaselineBandwidthInMbps" in instance_metadata['InstanceTypes'][0]['EbsInfo']['EbsOptimizedInfo'] else "0",
                                        "IOBaselineThroughputInMBps" : str(instance_metadata['InstanceTypes'][0]['EbsInfo']['EbsOptimizedInfo']['BaselineThroughputInMBps']) if "BaselineThroughputInMBps" in instance_metadata['InstanceTypes'][0]['EbsInfo']['EbsOptimizedInfo'] else "0",
                                        "IOBaselineIops" : str(instance_metadata['InstanceTypes'][0]['EbsInfo']['EbsOptimizedInfo']['BaselineIops']) if "BaselineIops" in instance_metadata['InstanceTypes'][0]['EbsInfo']['EbsOptimizedInfo'] else "0",
                                        "IOMaximumBandwidthInMbps" : str(instance_metadata['InstanceTypes'][0]['EbsInfo']['EbsOptimizedInfo']['MaximumBandwidthInMbps']) if "MaximumBandwidthInMbps" in instance_metadata['InstanceTypes'][0]['EbsInfo']['EbsOptimizedInfo'] else "0",
                                        "IOMaximumThroughputInMBps" : str(instance_metadata['InstanceTypes'][0]['EbsInfo']['EbsOptimizedInfo']['MaximumThroughputInMBps']) if "MaximumThroughputInMBps" in instance_metadata['InstanceTypes'][0]['EbsInfo']['EbsOptimizedInfo'] else "0",
                                        "IOMaximumIops" : str(instance_metadata['InstanceTypes'][0]['EbsInfo']['EbsOptimizedInfo']['MaximumIops']) if "MaximumIops" in instance_metadata['InstanceTypes'][0]['EbsInfo']['EbsOptimizedInfo'] else "0",
                                        "NetworkPerformance" : str(instance_metadata['InstanceTypes'][0]['NetworkInfo']['NetworkPerformance']) if "NetworkPerformance" in instance_metadata['InstanceTypes'][0]['NetworkInfo'] else "0",
                                        "NetworkCardPerformance" : str(instance_metadata['InstanceTypes'][0]['NetworkInfo']['NetworkCards'][0]['NetworkPerformance']) if "NetworkPerformance" in instance_metadata['InstanceTypes'][0]['NetworkInfo']['NetworkCards'][0] else "0",
                                        "NetworkCardMaximumInterfaces" : str(instance_metadata['InstanceTypes'][0]['NetworkInfo']['NetworkCards'][0]['MaximumNetworkInterfaces']) if "MaximumNetworkInterfaces" in instance_metadata['InstanceTypes'][0]['NetworkInfo']['NetworkCards'][0] else "0",
                                        "NetworkCardBaselineBandwidthInGbps" : str(instance_metadata['InstanceTypes'][0]['NetworkInfo']['NetworkCards'][0]['BaselineBandwidthInGbps']) if "BaselineBandwidthInGbps" in instance_metadata['InstanceTypes'][0]['NetworkInfo']['NetworkCards'][0] else "0",
                                        "NetworkCardPeakBandwidthInGbps" : str(instance_metadata['InstanceTypes'][0]['NetworkInfo']['NetworkCards'][0]['PeakBandwidthInGbps']) if "PeakBandwidthInGbps" in instance_metadata['InstanceTypes'][0]['NetworkInfo']['NetworkCards'][0] else "0",
                                    } 
            }
            metadata.append(
                                {
                                "resourceType" : self.object_class, 
                                "resourceName" : self.instances[key]['name'], 
                                "resourceId" : self.instances[key]['uid'], 
                                "resourceDefinition" : instance_metadata
                                }
            )
        self.file.write_all({ "file_name" : self.output_path + "metadata.json", "open_mode" : "w", "content" : json.dumps({ "exportParameters" : export_parameters, "metadata" : metadata}, indent=4, default=self.default) })
        
        
        # Gather Metrics - Instances
        instance_template = self.file.read_all({"file_name" : 'templates/rds.json' })
        for instance in self.instances:
            logging.info(f'Processing Object : Instance : {instance["name"]}')
            template = self.clw.replace_template_parameters(
                                                            [
                                                                { "name" :  "<instance>", "value" : instance['name'] },
                                                                { "name" :  "<period>", "value" : self.period },
                                                                { "name" :  "<interval>", "value" : self.interval },
                                                            ],
                                                            instance_template
            )
            properties = [
                            { "Name" : "ExporterId", "Value" : self.export_id  },
                            { "Name" : "ObjectType", "Value" : "rds" }, 
                            { "Name" : "LevelType", "Value" : "instance"  },
                            { "Name" : "Uid", "Value" : instance['uid']  },
                            { "Name" : "Identifier", "Value" : instance['name']  },
                            { "Name" : "ClusterIdentifier", "Value" : instance['DBClusterIdentifier'] },
                            { "Name" : "InstanceClass", "Value" : instance['DBInstanceClass']  },
                            { "Name" : "Engine", "Value" : instance['Engine']  }
            ]
            
            self.clw.get_metrics(template, properties)
            
            # Performance Insight Collection
            if instance['PerformanceInsightsEnabled'] == "True" :
                
                pi_files_catalog = ["rds.pi.os.json","rds.pi.sql.json"]
                
                if instance['Engine'] == "aurora-postgres":
                    pi_files_catalog.append("rds.pi.aurora.postgres.json")
                elif instance['Engine'] == "postgres":
                    pi_files_catalog.append("rds.pi.postgres.json")
                elif instance['Engine'] == "aurora-mysql":
                    pi_files_catalog.append("rds.pi.aurora.mysql.json")
                elif instance['Engine'] == "mysql":
                    pi_files_catalog.append("rds.pi.mysql.json")
                else: 
                    pass
                
                
                if int(self.interval) > int(instance['PerformanceInsightsRetentionPeriod']):
                    pi_interval = instance['PerformanceInsightsRetentionPeriod']
                else:
                    pi_interval = int(self.interval)
                    
                
                for pi_files in pi_files_catalog:
                    pi_template = json.loads(self.file.read_all({ "file_name" : f'templates/{pi_files}' }))
                    self.clw.get_pi_metrics(instance['DbiResourceId'], "RDS", pi_template, pi_interval, int(self.period), properties)
            
        
        archive_file_id = self.file.compress_directory(self.output_path, self.export_id + "." + self.object_class)
        return archive_file_id
        
        
    def default(self,o):
        if isinstance(o, (date, datetime)):
            return o.isoformat()
        

      
   

############################|-
############################|-       Class : classImporter     -|####################################
############################|-


class classImporter():

    #-----| Constructor
    def __init__(self, params):
        # store the event
        self.user_id = params["user_id"]
        self.region = params["region"]
        self.database_ts = params["database_ts"]
        self.table_ts = params["table_ts"]
        self.table_ddb = params["table_ddb"]
        self.client_ts = boto3.Session(region_name=self.region).client("timestream-write")
        self.client_ddb = boto3.Session().client("dynamodb",region_name= "us-east-1")
        self.import_id = datetime.now().strftime("%Y%m%d%H%M%S")
        self.file = classFile()
        self.output_path = "output/"
        self.file.create_folder(self.output_path)
        


    
    #-----| Prepare Record
    def prepare_record(self,measure_name, measure_value, measure_time):
        datetime_object = datetime.strptime(measure_time,"%Y-%m-%dT%H:%M:%S.%fZ" )
        record = {
                "MeasureName": measure_name,
                "MeasureValue":str(measure_value),
                "Time": str(calendar.timegm(datetime_object.timetuple()) * 1000),
        }
        return record



    #-----| Writer Records
    def write_records(self, table, records, common_attributes):
        try:
            result = self.client_ts.write_records(DatabaseName=self.database_ts,
                                                TableName=table,
                                                CommonAttributes=common_attributes,
                                                Records=records)
            status = result['ResponseMetadata']['HTTPStatusCode']
            #print("Processed %d records. WriteRecords HTTPStatusCode: %s" %
            #      (len(records), status))
        except self.client_ts.exceptions.RejectedRecordsException as err:
          self._print_rejected_records_exceptions(err)
        except Exception as err:
          print("Error:", err)


    
    #-----| Write Execeptions 
    @staticmethod
    def _print_rejected_records_exceptions(err):
        print("RejectedRecords: ", err)
        for rr in err.response["RejectedRecords"]:
          print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
          if "ExistingVersion" in rr:
            print("Rejected record existing version: ", rr["ExistingVersion"])
        
        
    
    
    
    #-----| Importer File
    def import_metric_file(self,file):
        try:
            logging.info(f'Importing file : {file}' )
            content = (self.file.read_all({"file_name" : file }))
            json_content = json.loads(content)
            
            if (json_content['is_null'] == False):
                
                dimensions = [{ "Name" : "ImporterId", "Value" : self.import_id }] + json_content['dimensions'] + json_content['metadata']['properties']
                common_attributes = {
                    'Dimensions': dimensions,
                    'MeasureValueType': 'DOUBLE'
                }
                
                df = json.loads(json_content['df'])
                records = []
                
                for data in df['data']:
                    for field in df['schema']['fields']:
                        if ( field['name'] != "index"):
                            records.append(self.prepare_record(field['name'], data[field['name']], data['index'] ))
                            if len(records) == 100:
                                self.write_records(self.table_ts, records, common_attributes)
                                records = []
      
        except Exception as err:
            print("Error:", err)
    
    
    
    #-----| Importer File
    def import_rpi_file(self,file):
        try:
            logging.info(f'Importing file : {file}' )
            content = (self.file.read_all({"file_name" : file }))
            json_content = json.loads(content)
            
            global_dimensions = [{ "Name" : "ImporterId", "Value" : self.import_id }] + json_content['metadata']['properties']
            
            for metric in json_content['metrics']:
                dimensions = []
                for tag in metric['tags']:
                    for tag_key in tag.keys():
                        dimensions.append({ "Name" : tag_key, "Value" : tag[tag_key] })    
                
                dimensions = global_dimensions + [{ "Name" : "MetricName", "Value" : metric['measure'] }] + dimensions
                
                common_attributes = {
                    'Dimensions': dimensions,
                    'MeasureValueType': 'DOUBLE'
                }
            
                df = json.loads(metric['DataPoints'])
                records = []
                for data in df['data']:
                    records.append(self.prepare_record(metric['measure'], data['value'], data['index'] ))
                    if len(records) == 100:
                        self.write_records(self.table_ts + "Pi", records, common_attributes)
                        records = []
                
      
        except Exception as err:
            print("Error:", err)
    
    
    #-----| Importer File
    def write_targets(self,metadata):
        try:
            
            for object in metadata['exportParameters']['objects']:
                
                dimensions = []
                dimensions.append({ "Name" : "ExporterId", "Value" : str(metadata['exportParameters']['exportId']) })
                dimensions.append({ "Name" : "ImporterId", "Value" : self.import_id }) 
                
                for tag in object.keys():
                    dimensions.append({ "Name" : tag, "Value" : str(object[tag]) })    
                
                common_attributes = {
                    'Dimensions': dimensions,
                    'MeasureValueType': 'DOUBLE'
                }
                
                self.write_records(self.table_ts + "Targets", [{"MeasureName": "target","MeasureValue": "1", "Time": str(int(round(time.time() * 1000 ))) }], common_attributes)
                
                
      
        except Exception as err:
            print("Error:", err)
    
    #-----| Extract File
    def extract_archive(self,archive_file):
        
         # Create archive file
        logging.info(f'Extracting archive file : {archive_file}' )
        subprocess.call(
                        [
                            "tar",
                            "xvfz",
                             archive_file,
                        ],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.STDOUT,
        )
        files = os.listdir("output")
        return files
    
    
    #-----| Write Metadata
    def write_metadata(self,user_id,metadata):
        try:
            records = metadata["metadata"]
            header = metadata["exportParameters"]
          
            for record in records:
                item = { 
                            'user_id': {}, 
                            'resource_id': {}, 
                            'exp_id': {}, 
                            'imp_id': {}, 
                            'resource_type': {}, 
                            'resource_name': {}, 
                            'region': {},
                            'interval': {}, 
                            'period': {}, 
                            'start_date': {},
                            'end_date': {}, 
                            'metadata': {} 
                }
                item["user_id"]["S"] = user_id
                item["resource_id"]["S"] = record["resourceId"]
                item["exp_id"]["S"] = header["exportId"]
                item["imp_id"]["S"] = self.import_id
                item["resource_type"]["S"] = record["resourceType"]
                item["resource_name"]["S"] = record["resourceName"]
                item["region"]["S"] = header["region"]
                item["interval"]["S"] = header["interval"]
                item["period"]["S"] = header["period"]
                item["start_date"]["S"] = header["startDate"]
                item["end_date"]["S"] = header["endDate"]
                item["metadata"]["S"] = json.dumps(record["resourceDefinition"])
                response = self.client_ddb.put_item(TableName = self.table_ddb, Item=item)
                
                
        except ClientError as e:
                print(f'{e.response["Error"]["Code"]}: {e.response["Error"]["Message"]}')
        

    #-----| Import Process
    def import_metrics(self,archive_file):
        
        files = self.extract_archive(archive_file)
        self.import_id = datetime.now().strftime("%Y%m%d%H%M%S")
        
        logging.info(f'Importing metadata ...')
        metadata = json.loads((self.file.read_all({"file_name" : self.output_path + "metadata.json" })))
        self.write_metadata(self.user_id, metadata)
        self.write_targets(metadata)
        
        
        logging.info(f'Importing metrics ...')
        logging.info(f'ImportedId : {self.import_id}')
        for file in files:
            if ( file != "metadata.json" and file != "template.json"):
                extension = file.split('.')[-1]
                if (extension == "mtr"):
                    self.import_metric_file(self.output_path + file)
                elif (extension == "rpi"):
                    self.import_rpi_file(self.output_path + file)
                else:
                    pass
                
            
        
        logging.info(f'Import process completed.')
            
        

'''
python3.9 exporter.py -t dynamodb -r us-east-1 -l table1 -p 60 -i 5
python3.9 exporter.py -t rds -r us-east-1 -l aur-pgs-01-instance-1 -p 60 -i 5 
'''



#--| main process
def main(argv):
    
    resource_object = {}
    resource_type = ''
    resource_list = ''
    region = ''
    target_table = ''
    period = 15
    interval = 60
   
    opts, args = getopt.getopt(argv,"ht:l:r:p:i:",["type=","list=","region=","period=","interval="])
    for opt, arg in opts:
        if opt == '-h':
            print ('exporter.py -t <resource_type> -l <resource_list> -r <region>')
            sys.exit()
        elif opt in ("-t", "--type"):
            resource_type = arg
        elif opt in ("-l", "--list"):
            resource_list = arg.split(",")
        elif opt in ("-r", "--region"):
            region = arg
        elif opt in ("-p", "--period"):
            period = arg
        elif opt in ("-i", "--interval"):
            interval = arg
    
    logging.basicConfig(format='%(levelname)s : %(message)s', level=logging.INFO)
    logging.info(f'ResourceType : {resource_type}' )
    logging.info(f'ResourceList : {resource_list}' )
    logging.info(f'Region : {region}' )
    
    if resource_type == 'dynamodb':
        resource_object = classDynamoDB({ "tables" : resource_list , "region" : region, "period" : period, "interval" : interval })
        target_table = 'tblDynamoDB'
  
    elif resource_type == 'elasticache':
        resource_object = classElasticache({ "clusters" : resource_list , "region" : region, "period" : period, "interval" : interval })
        target_table = 'tblElasticache'
  
    elif resource_type == 'rds':
        resource_object = classRds({ "instances" : resource_list , "region" : region, "period" : period, "interval" : interval })
        target_table = 'tblRds'
    
    archive_file_id = resource_object.export_metrics()
    
    '''
    
    # Used to import the archiver
    importer = classImporter({ "user_id" : "mail@example.com", "region" : "us-east-1", "database_ts" : "dbDPA360", "table_ts" : target_table, "table_ddb" : "tblDPA360" })
    importer.import_metrics(archive_file_id)
    
    '''
    
if __name__ == "__main__":

    main(sys.argv[1:])
    
    
    