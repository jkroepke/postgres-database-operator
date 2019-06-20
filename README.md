# postgres-database-operator

A Kubernetes operator to create databases inside a running postgresql instance. 

The combine the power of kubernetes with the production grade databases running outside kubernetes clusters.

Example: If you running on AWS with a RDS and you want to give your users the power the create databases without giving
them admin credentials

The operator based the [kopf operator framework](https://github.com/zalando-incubator/kopf) from zalando.

# Install

Be sure your are running a Kubernetes Cluster (tested with 1.11) with RBAC enabled.

There is a helm chart inside `helm/charts` directory to install the operator.

Prebuild images are available on docker hub. http://hub.docker.com/r/jkroepke/postgres-database-operator