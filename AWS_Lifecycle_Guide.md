# 🛑 AWS Infrastructure Lifecycle Guide

As this project runs on AWS with a **2-Instance Architecture**, you are billed for the compute hours of running EC2 instances. Since this is an automated Infrastructure-as-Code (IaC) deployment managed by Terraform, you do **not** need to keep the infrastructure running 24/7.

This guide explains how to safely tear down the environment to achieve **$0.00 cost**, and how to bring it back online whenever you want to work again.

---

## 🏗️ Architecture Overview

The infrastructure provisions **2 AWS EC2 Instances**:
1. **Instance #1 (`ecom-saga-app`)**: Runs Kafka (KRaft), Kafka UI, and the 4 Spring Boot Microservices + Node Exporter, cAdvisor, and Promtail.
2. **Instance #2 (`ecom-saga-monitoring`)**: Runs Prometheus, Loki, and Grafana for centralized metrics, logging, and performance dashboards.

---

## 🟥 How to STOP the Deployment (Zero Cost Mode)

When you finish working for the day or over the weekend, destroy the infrastructure so AWS stops billing you.

### 1. The Command
Open your terminal on your local machine:
```bash
cd Infrastructure
terraform destroy -auto-approve
```

### 2. What happens under the hood?
* **Termination:** Terraform talks to AWS API and terminates both `t3.small` EC2 instances (`ecom-saga-app` and `ecom-saga-monitoring`).
* **Cleanup:** It deletes Security Groups, IAM Roles/Policies, and CloudWatch Metric Alarms.
* **State Update:** Updates `terraform.tfstate` to reflect 0 active resources.
* **Billing Stops:** AWS compute billing drops to **$0.00** immediately.
* **Data Persistence Note:** Disk storage on the EC2 instances (such as Kafka message logs and in-memory H2 databases) is wiped. This is expected for transient demo environments.

---

## 🟩 How to START the Deployment

To bring the full production-like microservices & monitoring stack back online in ~3 minutes:

### 1. Provision the Infrastructure
Run in your local terminal:
```bash
cd Infrastructure
terraform apply -auto-approve
```

### 2. What happens under the hood? (Phase 1 — Provisioning)
* **Creation:** Terraform provisions 2 new `t3.small` EC2 instances, security groups, IAM roles with SSM permissions, and CloudWatch metric alarms.
* **User Data Bootstrapping:** 
  - `user_data.sh` initializes Instance #1 (installs Docker, SSM agent, CloudWatch agent, 2GB swap space).
  - `monitoring_user_data.sh` initializes Instance #2 (installs Docker, SSM agent, CloudWatch agent, 2GB swap space).

### 3. Deploy the Applications
Go to your GitHub repository in your browser:
1. Open the **Actions** tab.
2. Select **Deploy to EC2** on the left panel.
3. Click **Run workflow** (or simply push code to `main`).

### 4. What happens under the hood? (Phase 2 — Automated Deployment)
* **AWS OIDC Authentication:** GitHub Actions authenticates securely with AWS using IAM OIDC (no static access keys).
* **IP Resolution in Runner:** The runner resolves the public IPs of both instances using AWS EC2 API.
* **SSM Deployment:** Runner uses AWS Systems Manager (SSM) to execute deployment commands directly on EC2:
  - **Instance #1:** Pulls latest code/Docker images, injects Loki URL into `.env`, and starts microservices via `docker compose up -d`.
  - **Instance #2:** Injects Instance #1 IP into `config/prometheus.yml` and starts Prometheus, Loki, and Grafana via `docker compose -f docker-compose.monitoring.yml up -d`.
* **Health Checks:** The workflow verifies `/actuator/health` endpoints on all microservices before completing.

---

## 💡 Dynamic IP Address Reminder
Every time you run `terraform apply`, AWS assigns **new public IPs** to both instances. Run `./scripts/print-endpoints.sh` or check `terraform output` to retrieve the active live URLs!
