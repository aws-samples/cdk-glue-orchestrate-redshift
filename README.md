
# Use Glue workflow to orchestrate Redshift TPC-DS queries

This CDK project will deploy a clean redshift cluster and a Glue workflow to perform the TPC-DS benchmark test including sequantially run 99 queries and concurrently run 99 queries(10 parallel level). It doesn't include tpcds data. Thus, you need to generate the data first and store it in S3. 


## Prerequisite

* Refer to [CDK website](https://aws.amazon.com/cn/cdk/) for how to install the environment.

* Use TPC-DS generate data and store it in S3 in '|' delimited CSV format

## Deploy Testing Environment

To list current cloudformation stack that could be deployed:

```
$ cdk ls
```

It returns below stacks
```
repository                          # S3 bucket to store assets
vpc-stack                           # A vpc to put Redshift
redshift-stack                      # Redshift cluster
benchmark-workflow                  # A Glue workflow to perform the benchmark test
```

Use CDK CLI deploy the cloudformations with default parameters
```
$ cdk deploy benchmark-workflow
```

You could also pass in custom parameters to make your own configuration like

```
$ cdk deploy benchmark-workflow \
    --parameters benchmark-workflow:numRuns=2 \
    --parameters benchmark-workflow:parallel=100
```
All possible parameters you can tune are

* `redshift-stack:rsInstanceType`       Redshift instance type default to RA3.xlplus
* `redshift-stack:rsSize`               Redshift cluster size default to 2
* `redshift-stack:username`             Redshift super username default to admin
* `redshift-stack:password`             Redshift super user password default to Admin1234
* `benchmark-workflow:tpcdsDataPath`    S3 path that stores TPCDS data in | delimited CSV default to s3://redshift-downloads/TPC-DS/2.13/3TB/
* `benchmark-workflow:parallel`         Concurrent thread count during concurrent query test default to 10
* `benchmark-workflow:numRuns`          Total number of runs default to 2
* `benchmark-workflow:numFiles`         Total number of sql query files to run under tpcds_queries default to 99

## Perform Benchmark Test

Go to Glue console -> Workflow, you will see a workflow named 'redshift-benchmark'. Start the workflow and it will perform below tasks:

 * `tpcds-benchmark-create-tables`          Create all tpcds tables
 * `tpcds-benchmark-load-tables`            Use copy command to load data into Redshift tables
 * `tpcds-benchmark-sequential-report`      Do sequential query submit and generate report
 * `tpcds-benchmark-concurrent-report`      Do concurrent query submit and generate report

Above glue jobs can be run individually and manually.

## Check Performance Report

When the Glue workflow completes and succeeds. There are two places you could check the query runtime.

* Job logs of  `tpcds-benchmark-sequential-report`, `tpcds-benchmark-concurrent-report`
* S3 directory of repository cloudformation stack eg. s3://repository-s3xxx/report/

## Customize for your own dataset
You could put your own DDL and queries in redshift_scripts/ and replace the TPCDS DDL and queries. And modify the *app.py* parameters `tpcds_data_path` and `num_files` to match your own dataset.
Enjoy!
