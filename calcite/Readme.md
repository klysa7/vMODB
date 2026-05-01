###OLAP Gateway + Calcite Prototype — Success Path

Here I describe how to start the Calcite package, deploy the system with the latest changes, execute a transactional workload, 
and verify the OLAP query that joins data across two VMSes.
It is a working prototype that validates an OLAP execution path in vMODB. 
The focus is on architectural correctness and cross-VMS query integration rather than performance optimization.

1. Starting the calcite package (here you can see the logs)
``` 
   cd calcite
   mvn -DskipTests exec:java -Dexec.mainClass=dk.ku.di.dms.vms.calcite.Main
```

2. Starting the marketplace
```
cd marketplace
scripts/deploy/deploy_linux.sh cart customer order payment product seller shipment stock proxy
```

3. Running Transactions and Executing the OLAP Query
```
cd marketplace
scripts/transactions/checkout_delivery.sh 4
```

4. Execute the Olap Query
```
curl -X GET localhost:8095/olap/orders
```

###Expected Result
```
{
  "resultColumns": [
    "customer_id",
    "order_id",
    "sequential"
  ],
  "resultRowCount": 4,
  "result": [
    {
      "customer_id": 1,
      "order_id": 1,
      "sequential": 1
    },
    {
      "customer_id": 2,
      "order_id": 1,
      "sequential": 1
    },
    {
      "customer_id": 3,
      "order_id": 1,
      "sequential": 1
    },
    {
      "customer_id": 4,
      "order_id": 1,
      "sequential": 1
    }
  ]
}```