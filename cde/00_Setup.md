## Iceberg Demo Setup Instructions

Clone this repository to your machine. Then run the deployment script with:

```
% ./setup/deploy_hol.sh <docker-user> <cdp-workload-user> <number-of-participants> <storage-location>
```

For example:

```
#AWS
% ./deploy_demo.sh pauldefusco pauldefusco s3a://paul-aug26-buk-a3c2b50a/data/pdefusco
```

```
#Azure
% ./deploy_demo.sh pauldefusco pauldefusco abfs://logs@go01demoazure.dfs.core.windows.net/data
```

## Iceberg Demo Teardown Instructions

```
% ./teardown.sh cdpworkloaduser
```
