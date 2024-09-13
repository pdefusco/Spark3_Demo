## Iceberg Demo Setup Instructions

Clone this repository to your machine. Then run the deployment script with:

```
% ./setup/deploy_hol.sh <docker-user> <cdp-workload-user> <number-of-participants> <storage-location>
```

For example:

```
#AWS
% ./deploy_hol.sh pauldefusco pauldefusco 1 s3a://goes-se-sandbox01/data
```

```
#Azure
% ./deploy_hol.sh pauldefusco pauldefusco 1 abfs://logs@go01demoazure.dfs.core.windows.net/data
```
