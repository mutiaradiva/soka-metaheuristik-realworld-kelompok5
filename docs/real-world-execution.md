# Real-world execution

This project keeps the original CloudSim simulation and CSA/PHS implementations and adds a separate HTTP execution path. The controller optimizes against available worker slots, then sends every planned task to the assigned worker. Worker responses determine success; HTTP failures and timeouts are recorded as failed tasks.

## Build and local worker

Requires JDK 21. From `java`, run `gradlew.bat build`. Set the same secret on controller and worker as `SOKA_WORKER_TOKEN`. Start a worker with:

```powershell
$env:SOKA_WORKER_TOKEN = 'local-development-secret'
java -cp 'build\classes\java\main;build\resources\main;build\libs\*' id.ac.soka.scheduling.realworld.AwsWorker --host-id local --vm-id 0 --slots 2 --bind 127.0.0.1 --port 8080
```

Copy `deploy/aws-workers.example.json` to `deploy/aws-workers.json` and set its endpoint, host/VM identity, slot count and modeled hourly rate. Then:

```powershell
./gradlew.bat run --args="preflight --config deploy/aws-workers.json"
./gradlew.bat run --args="run --algorithm CSA --config deploy/aws-workers.json --dataset ../dataset/GoCJ_Dataset_1000.csv --output ../results/csa-run-01"
./gradlew.bat run --args="run --algorithm PHS --config deploy/aws-workers.json --dataset ../dataset/GoCJ_Dataset_1000.csv --output ../results/phs-run-01"
```

Optional flags: `--tasks N`, `--seed N`. CSA/PHS use the existing `AlgorithmOptions.defaults()` (CSA population 30 / selected 10 / generations 200; PHS memory 10 / iterations 200 / improvisations 3); tune these defaults in the existing options source.

## AWS deployment outline

Build the Java project, copy the project and JDK 21 to an EC2 instance, create a dedicated `soka` user, install the systemd unit, and create `/etc/soka-worker.env` containing `SOKA_WORKER_TOKEN=<shared-random-secret>` with mode 600. Set `%H` and VM ID/slots in the unit for each worker. Restrict inbound TCP/8080 in the security group to the controller's address, enable and start `soka-metaheuristik-worker`, and configure the controller with the worker's private/reachable URL. No AWS credentials are needed by this execution protocol.

## Measurements and limitations

Task execution and CPU seconds come from worker measurements. Queue/wait, turnaround and makespan use controller monotonic time. Cost uses configured hourly rates and measured run duration and is an estimate. `estimated_utilization` is CPU-seconds divided by slot-seconds; it is an estimate from worker CPU timing rather than host-wide CloudWatch utilization. The CPU payload is a deterministic bounded integer workload scaled by task MI; adjust the worker's iteration calibration for the intended instance sizes before experiments. This repository has not been deployed to AWS from this environment.
