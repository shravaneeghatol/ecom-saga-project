# Deployment Deliverables — `ecom-saga-project`

Read **`AWS-Deployment-Plan.md`** first — it's the full analysis and answers all 13 requested sections. Everything else here is the supporting implementation.

## Files in this delivery

| File | Purpose |
|---|---|
| `AWS-Deployment-Plan.md` | The full deployment plan (analysis, service suitability, rankings, cost, security, IaC explanation, scorecard) |
| `terraform/main.tf` | EC2 instance, security group, IAM role, CloudWatch log group + alarms |
| `terraform/variables.tf` | All configurable inputs (region, instance type, your IP, etc.) |
| `terraform/outputs.tf` | Public IP, SSH/SSM command, service URLs after `terraform apply` |
| `terraform/user_data.sh` | First-boot script: installs Docker, Docker Compose, CloudWatch Agent, adds swap |
| `docker-compose.prod.yml` | Production override — pulls Docker Hub images, caps JVM heap, adds restart policies |
| `.env.example` | Template for the currently-empty `.env` (see Section 6/finding #1 in the plan) |
| `ci-cd/deploy.yml` | GitHub Actions job — place at `.github/workflows/deploy.yml`; deploys via SSM after CI passes |

## Minimum path to a working deployment

1. **In the app repo**, apply the small code changes from Section 6 of the plan. The one that actually changes `.java`/`.yml` files is the H2 console flag — do this in all 4 `application.yml` files:
   ```yaml
   # before
   spring:
     h2:
       console:
         enabled: true
   # after
   spring:
     h2:
       console:
         enabled: ${H2_CONSOLE_ENABLED:true}
   ```
   Everything else in Section 6 is `.env`/`docker-compose.yml`/CI changes, not Java code.
2. Copy `.env.example` → `.env` and fill in real values.
3. Replace `<DOCKERHUB_USERNAME>` in `docker-compose.prod.yml` with the value already used in `.github/workflows/ci.yml`.
4. `cd terraform && terraform init && terraform apply` (set `my_ip_cidr` in a `terraform.tfvars` first — find your IP at https://checkip.amazonaws.com).
5. On the instance (via SSM: `aws ssm start-session --target <instance_id>`, or SSH if `enable_ssh = true`):
   ```bash
   git clone <your-repo-url> app && cd app
   cp .env.example .env   # then edit with real values
   docker compose -f docker-compose.yml -f docker-compose.prod.yml pull
   docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
   ```
6. Confirm: `curl http://<public_ip>:8081/api/orders` (from `terraform output service_urls`).
7. (Optional) add `deploy.yml` to `.github/workflows/` and the secrets listed at the top of that file to automate step 5 on every future push to `main`.

## What was intentionally left out, and why

- **No RDS/persistent database** — the app has no persistence layer today (H2 in-memory is the current design); adding one is an optional Section-2/10 upgrade, not a launch requirement.
- **No ALB / ACM / CloudFront** — none are free, and none are needed for a single-instance backend-only deployment.
- **No VPC/NAT Gateway** — the default VPC + public subnet is sufficient; a NAT Gateway alone would cost more per month than the entire rest of this deployment combined.
- **No WAF** — no browser-facing surface and no indication of abuse risk; would only add cost.
