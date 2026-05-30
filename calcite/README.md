# vMODB HTAP Benchmark — Run Guide
## Prerequisites

- The JDK you built the project with (the run commands use `--enable-preview`, so the runtime JDK must match the build JDK).
- Maven.
- All commands are run from the repository root unless a `cd` is shown.

## Port reference

| Component            | Port  |
|----------------------|-------|
| warehouse VMS        | 8001  |
| inventory VMS        | 8002  |
| order VMS            | 8003  |
| replica VMS          | 8004  |
| replica OLAP (HTTP)  | 8096  |
| coordinator (HTTP)   | 8079  |
| coordinator (SSE)    | 8091  |
| Calcite gateway      | 8095  |

---

## 1. Find the host IP

```bash
hostname -I
```

Take the first address it prints. for example `10.36.94.228`. Substitute it everywhere `<HOST_IP>` appears below.

## 2. Point the config files at `<HOST_IP>`

Two files need the address.

### 2a. `calcite/src/main/resources/application.properties`

```properties
coordinator.http_url=http://<HOST_IP>:8079
coordinator.sse_url=http://<HOST_IP>:8091
```

### 2b. `tpcc/proxy-tpcc/src/main/resources/app.properties`

```properties
warehouse_host=<HOST_IP>
inventory_host=<HOST_IP>
order_host=<HOST_IP>
replica_host=<HOST_IP>
gateway_host=<HOST_IP>
```

## 3. Build everything

```bash
mvn clean install -DskipTests
```

## 4. Start the services

Open a separate terminal for each component and start them in the order below. Leave each one running.
Proxy needs to be the last one.
### 4a. Calcite OLAP gateway

```bash
cd calcite
mvn exec:exec -Dexec.executable="java" \
  -Dexec.args="--enable-preview \
  --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-opens java.base/jdk.internal.util=ALL-UNNAMED \
  -classpath %classpath dk.ku.di.dms.vms.calcite.Main"
```

### 4b. Warehouse VMS

```bash
cd tpcc/warehouse-tpcc
java -Xms8G -Xmx16G -XX:MaxDirectMemorySize=8G --enable-preview \
  --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-opens java.base/jdk.internal.util=ALL-UNNAMED \
  -jar target/warehouse-tpcc-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### 4c. Order VMS

```bash
cd tpcc/order-tpcc
java -Xms8G -Xmx16G -XX:MaxDirectMemorySize=8G --enable-preview \
  --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-opens java.base/jdk.internal.util=ALL-UNNAMED \
  -jar target/order-tpcc-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### 4d. Inventory VMS

```bash
cd tpcc/inventory-tpcc
java -Xms8G -Xmx16G -XX:MaxDirectMemorySize=8G --enable-preview \
  --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-opens java.base/jdk.internal.util=ALL-UNNAMED \
  -jar target/inventory-tpcc-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### 4e. Replica VMS (optional)

Only needed for the replica experiment (`use_replica=true` in `app.properties`).

```bash
cd tpcc/replica-tpcc
java -Xms8G -Xmx16G -XX:MaxDirectMemorySize=8G --enable-preview \
  --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-opens java.base/jdk.internal.util=ALL-UNNAMED \
  -jar target/replica-tpcc-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### 4f. Proxy (the experiment driver)

```bash
cd tpcc/proxy-tpcc
java --enable-preview \
  --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-opens java.base/jdk.internal.util=ALL-UNNAMED \
  -jar target/proxy-tpcc-1.0-SNAPSHOT.jar
```

## 5. Run the experiment

Once every VMS and the gateway are up, drive the run from the **proxy terminal** by entering these menu choices in order:

1. `1` — Distributed
2. `1` — Populate VMS states
3. `7` — HATtrick throughput frontier experiment
4. `3` — Full grid
5. Query: `chq6`

Then accept the grid prompts (T-client counts, A-client counts, warmup, measurement) or take the defaults, and confirm with `y`.

---

## Troubleshooting

- **A-qps = 0 for every grid cell.** Almost always a wrong or missing `gateway_host` in `tpcc/proxy-tpcc/src/main/resources/app.properties`, or the gateway not running on port 8095.
- **Coordinator never says "N VMSes connected".** A VMS is not up, or a `*_host` value is wrong. With `use_replica=true` it waits for 4 VMSes, including the replica.
- **Check which Java processes are running:** `jps -l`. It should be empty before a fresh start.
- **Check who holds a port:** `sudo lsof -nP -iTCP -sTCP:LISTEN | grep java`, or free one with `sudo kill -9 $(sudo lsof -ti :8095)`.
- **Rebuild gotcha.** After switching branches or editing shared modules, always rebuild from the root with `mvn clean install` so every module uses the same classes.
