# 🛑 AWS Infrastructure Lifecycle Guide

As this project runs on AWS, you are charged for the time the EC2 instance is running. Because this is an automated Infrastructure-as-Code (IaC) deployment, you do not need to keep the server running 24/7. 

This guide explains exactly how to safely tear down the environment to save costs, and how to bring it back online when you are ready to work again.

---

## 🟥 How to STOP the Deployment (Zero Cost Mode)

When you are done working for the day or over the weekend, you should destroy the infrastructure so AWS stops billing you for the EC2 compute time.

### 1. The Command
Open your terminal on your local machine and run:
```bash
cd Infrastructure
terraform destroy -auto-approve
```

### 2. What happens under the hood?
* **Termination:** Terraform talks to AWS and permanently terminates your `t3.small` EC2 server.
* **Cleanup:** It deletes the Security Groups, IAM Roles, and CloudWatch Log groups associated with the project.
* **Billing Stops:** The moment this command finishes, AWS stops charging you. Your hourly compute bill drops to $0.00.
* **Data Loss:** Because the server is destroyed, anything saved *locally* on the EC2 disk (like Kafka message history or in-memory H2 database records) is wiped out. *(This is expected and completely fine for a microservice dev environment).*

---

## 🟩 How to START the Deployment

When you are ready to resume work, you can bring the entire production-like environment back online in about 5 minutes.

### 1. Provision the Server
Open your terminal and run:
```bash
cd Infrastructure
terraform apply -auto-approve
```

### 2. What happens under the hood? (Phase 1)
* **Creation:** Terraform creates a brand new `t3.small` EC2 instance.
* **Security:** It recreates your Security Group (opening ports `8081-8084`, `8090`) and IAM OIDC roles.
* **Bootstrapping (Wait 3 minutes):** As the server boots, a script called `user_data.sh` runs automatically in the background. It installs Docker, creates a 2GB memory swap file, and starts the AWS SSM Agent. **You must wait 3 minutes for this to finish.**

### 3. Deploy the Microservices
Go to your GitHub repository in your browser:
1. Click the **Actions** tab.
2. Select **Deploy to EC2** on the left menu.
3. Click **Run workflow**.

### 4. What happens under the hood? (Phase 2)
* **Authentication:** GitHub Actions securely authenticates with AWS using OIDC (no hardcoded passwords).
* **Discovery:** It automatically scans your AWS account to find the EC2 instance tagged with `Name=ecom-saga-app`.
* **Deployment:** It uses AWS Systems Manager (SSM) to inject a bash script directly into the server.
* **Pull & Run:** The server pulls the latest Docker images from DockerHub and runs `docker compose up -d`.
* **Health Check:** The pipeline waits and checks the `/actuator/health` endpoint of all 4 Spring Boot services, ensuring they are fully booted before giving you the green checkmark.

### 💡 Important Note: Dynamic IP Address
Every time you run `terraform apply`, AWS assigns your new server a **brand new Public IP address**. 
At the end of the `terraform apply` command, Terraform will print out the new IP address. You must use this new IP address in your browser to access the Kafka UI and H2 database!
