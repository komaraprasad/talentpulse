# Deploying TalentPulse on AWS

TalentPulse is one web service: a Python 3.12 FastAPI app that serves the dashboard and the API.
All LinkedIn work happens in each user's browser through the TalentPulse extension. The server
never connects to LinkedIn and stores no LinkedIn logins.

## What you need

| Item | Value |
|---|---|
| Container image | built from `Dockerfile` in the repo root, pushed to ECR |
| Port | 8000 |
| Health check | `GET /api/status` → 200 |
| Size | 0.5 vCPU, 1 GB RAM |
| Replicas | **exactly 1** (see below) |
| Database | Any PostgreSQL 14+: Aiven (chosen for testing) or RDS (`db.t4g.micro` is enough). The app creates its tables on first start. |
| Secret | `DATABASE_URL`, e.g. `postgresql://user:pass@host:5432/talentpulse?sslmode=require` |
| Load balancer | ALB, **idle timeout ≥ 300 s**. Testing: HTTP (80) on the ALB's own address. Later: HTTPS with an ACM certificate and a domain. |
| Network | inbound 80 (testing) or 443 (with a domain) only; outbound to the database and the internet (HTTPS to `cloudflare-dns.com` for email domain checks) |

Environment variables (already set in the image, can be overridden):

| Name | Value | Meaning |
|---|---|---|
| `DATABASE_URL` | secret | where candidate data is stored |
| `TALENTPULSE_HOSTED` | `1` | turns off test-only features (dry runs, raw logs, `/docs`) |
| `TALENTPULSE_SMTP_CHECK` | `0` | set `1` only if outbound port 25 is allowed |
| `TZ` | `Asia/Kolkata` | timestamps in India time |
| `TALENTPULSE_DB_POOL_SIZE` | `8` in the ECS and EKS files (app default 15) | most database connections at once; `8` suits a database with a 15-connection limit (e.g. Aiven free) |

**Restrict access: TalentPulse has no login.** Anyone who can reach the address can read, export and change
candidate data. Allow only the office / VPN addresses on the load balancer (or make it internal), never `0.0.0.0/0`.
Plain HTTP on the load balancer address is for short tests only: use HTTPS with a domain before real use.

**Why exactly 1 replica:** running searches and the dashboard's live updates are kept in the
app's memory. With 2+ copies, a browser's requests land on different copies and searches fail.
Use `Recreate` (EKS) or minimum/maximum healthy 0/100 % (ECS) so two copies never overlap.
Restarts are safe: all saved data is in PostgreSQL.

**Why idle timeout ≥ 300 s:** the dashboard keeps one live connection open (`/api/events`,
Server-Sent Events). A shorter timeout makes it reconnect often.

## 1. Build and push the image

```bash
aws ecr create-repository --repository-name talentpulse --region <REGION>
aws ecr get-login-password --region <REGION> | docker login --username AWS --password-stdin <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com
docker build --platform linux/amd64 -t talentpulse:<TAG> .
docker tag talentpulse:<TAG> <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/talentpulse:<TAG>
docker push <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/talentpulse:<TAG>
```

## 2. Database and secret

**Aiven (chosen for testing):** copy the **Service URI** from the Aiven console (keep `?sslmode=require`). The cluster
must be allowed to connect out to the Aiven host and port shown there (not 5432). Candidate data then sits outside
the company's AWS account, so confirm that is allowed.

**RDS (alternative):** create a PostgreSQL instance in the same VPC as the cluster, allow port 5432 only from the
service's security group, and create a database (e.g. `talentpulse`).

Store the connection string as a secret (never in a file or in Git):

```bash
aws secretsmanager create-secret --name talentpulse/database-url --region <REGION> \
  --secret-string '<CONNECTION_STRING>'
```

## 3a. ECS Fargate

1. Fill in `<ACCOUNT_ID>`, `<REGION>`, `<TAG>` in `ecs-task-definition.json`. The execution role
   needs `secretsmanager:GetSecretValue` on the secret, plus the standard ECR/CloudWatch permissions.
2. Register it: `aws ecs register-task-definition --cli-input-json file://deploy/aws/ecs-task-definition.json`
3. Create an ALB target group: target type `ip`, port 8000, health check `/api/status`.
   Set the ALB idle timeout to 300 s and add an HTTP (80) listener for testing (later: HTTPS with an
   ACM certificate once there is a domain).
4. Create the service with desired count **1**, deployment minimum healthy **0 %**, maximum **100 %**:

```bash
aws ecs create-service --cluster <CLUSTER> --service-name talentpulse \
  --task-definition talentpulse --desired-count 1 --launch-type FARGATE \
  --deployment-configuration "minimumHealthyPercent=0,maximumPercent=100" \
  --network-configuration "awsvpcConfiguration={subnets=[<SUBNETS>],securityGroups=[<SG>],assignPublicIp=DISABLED}" \
  --load-balancers "targetGroupArn=<TARGET_GROUP_ARN>,containerName=talentpulse,containerPort=8000"
```

## 3b. EKS

1. Fill in the image in `eks-talentpulse.yaml`. It is set up for HTTP testing on the ALB address; the
   comments in the Ingress show the lines to switch to once there is a domain and certificate.
2. Create the secret from the command line, then apply the file:

```bash
kubectl create namespace talentpulse
kubectl -n talentpulse create secret generic talentpulse-db --from-literal=DATABASE_URL='postgresql://...'
kubectl apply -f deploy/aws/eks-talentpulse.yaml
```

## 4. Address (testing without a domain)

While testing, use the load balancer's own address, e.g.
`http://talentpulse-123456.ap-south-1.elb.amazonaws.com`. The TalentPulse extension (2.12.0+)
already recognises any `*.elb.amazonaws.com` address, over HTTP or HTTPS. No certificate is needed:
use an HTTP (80) listener only and skip the ACM certificate.

When a domain is ready later, point it at the ALB (Route 53 alias or CNAME), add an HTTPS listener with
an ACM certificate, and send the domain to the developer so it can be added to the extension.

## 5. Check it works

Below, `<ADDRESS>` is the ALB address (testing) or the domain.

1. `<ADDRESS>/api/status` returns JSON.
2. `<ADDRESS>/` shows the dashboard.
3. `<ADDRESS>/api/extension.zip` downloads the extension (version 2.12.0 or later).
4. Install the extension (banner on the dashboard), run a 25-lead search, then restart the service
   and confirm the candidates are still there.

## Single EC2 instance (Docker + Postgres container)

Used when the company gives one EC2 instance instead of a cluster. `docker-compose.ec2.yml` runs the app and a
Postgres container; the data sits in a Docker volume on the instance's disk (ask for EBS snapshots).

1. `git clone` the repo on the instance, then create `.env` with `DB_PASSWORD` (see the top of the compose file).
2. Build with plain Docker (Compose's own build needs buildx): `sudo DOCKER_BUILDKIT=0 docker build -t talentpulse-app:local .`
   On an ARM (t4g) instance never use `--platform linux/amd64`.
3. `cp deploy/aws/docker-compose.ec2.yml docker-compose.yml`, then `sudo docker-compose up -d db`, wait 15 s,
   `sudo docker-compose up -d app`.
4. The security group must allow inbound **8000 from the office / VPN range only** (there is no login).
5. The team opens `http://ec2-<IP-with-dashes>.<region>.compute.amazonaws.com:8000` (the instance's Public IPv4 DNS).
   Extension 2.14.0+ recognises this address. Use an **Elastic IP** so the address does not change when the instance restarts.

## Updating

Build and push a new tag, then update the task definition / Deployment image. With 1 replica the
service is unavailable for about 30 seconds during an update; saved data is not affected.
