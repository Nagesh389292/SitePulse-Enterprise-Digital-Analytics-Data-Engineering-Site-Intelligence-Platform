# AWS ECS Fargate Deployment Runbook — SitePulse Platform

This guide documents the procedure for deploying SitePulse as a high-availability microservice on AWS ECS Fargate with persistent EFS storage and Application Load Balancer (ALB).

---

## 1. Prerequisites
- AWS CLI v2 configured with IAM credentials
- Docker CLI installed locally
- Amazon ECR repository: `sitepulse`
- Amazon EFS File System provisioned with mount targets in private subnets
- Application Load Balancer with HTTPS listener on port 443 routing to target group port 8000

---

## 2. Docker Image Build & ECR Push

```bash
# 1. Authenticate Docker with Amazon ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 123456789012.dkr.ecr.us-east-1.amazonaws.com

# 2. Build production container
docker build -t sitepulse:v2.0.0 -f Dockerfile .

# 3. Tag container for ECR registry
docker tag sitepulse:v2.0.0 123456789012.dkr.ecr.us-east-1.amazonaws.com/sitepulse:v2.0.0
docker tag sitepulse:v2.0.0 123456789012.dkr.ecr.us-east-1.amazonaws.com/sitepulse:latest

# 4. Push image to ECR
docker push 123456789012.dkr.ecr.us-east-1.amazonaws.com/sitepulse:v2.0.0
docker push 123456789012.dkr.ecr.us-east-1.amazonaws.com/sitepulse:latest
```

---

## 3. Register ECS Task Definition

```bash
aws ecs register-task-definition \
    --cli-input-json file://ecs/task-definition.json \
    --region us-east-1
```

---

## 4. Deploy or Update ECS Fargate Service

```bash
aws ecs update-service \
    --cluster sitepulse-production-cluster \
    --service sitepulse-enterprise-service \
    --task-definition sitepulse-enterprise-task \
    --force-new-deployment \
    --region us-east-1
```

---

## 5. Verification & Health Monitoring

```bash
# Check service rollout status
aws ecs describe-services \
    --cluster sitepulse-production-cluster \
    --services sitepulse-enterprise-service \
    --query "services[0].deployments" \
    --region us-east-1

# Query CloudWatch logs
aws logs tail /ecs/sitepulse-enterprise --follow --region us-east-1
```
