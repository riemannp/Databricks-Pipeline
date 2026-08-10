# CI/CD Setup Guide

Quick guide to set up and test the CI/CD pipeline for the Databricks Dynamic Pipeline project.

## Prerequisites

- GitHub repository access
- Databricks workspace access  
- Databricks personal access token

## 1. Configure GitHub Secrets

The CI/CD pipeline requires two secrets to connect to Databricks:

### Add Secrets:

1. Go to your repository on GitHub
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Add the following secrets:

| Secret Name | Value | Description |
|-------------|-------|-------------|
| `DATABRICKS_HOST` | `https://dbc-a9341f07-f0a9.cloud.databricks.com` | Your Databricks workspace URL |
| `DATABRICKS_TOKEN` | `<your-token>` | Personal access token from Databricks |

### Get Databricks Token:

1. Log into your Databricks workspace
2. Click your username → **Settings**
3. Go to **Developer** → **Access tokens**
4. Click **Generate new token**
5. Give it a name (e.g., "GitHub Actions CI/CD")
6. Set expiration (recommended: 90 days)
7. Click **Generate**
8. **Copy the token immediately** (you won't see it again!)

## 2. Test the Workflow

### Option A: Push to Feature Branch

Create a feature branch and push to trigger the workflow.

### Option B: Create a Pull Request

1. Go to your GitHub repository
2. Click **Pull requests** → **New pull request**
3. Select your branch
4. Create the pull request
5. The workflow will automatically run

## 3. Monitor Workflow Execution

### View Workflow Status:

1. Go to your GitHub repository
2. Click the **Actions** tab
3. Click on the latest workflow run
4. View progress of each job

### Expected Timeline:

- **Lint**: ~30 seconds
- **Test**: ~1-2 minutes
- **Validate**: ~30 seconds
- **Deploy**: ~1 minute
- **Security**: ~1 minute
- **Total**: ~3-4 minutes

## 4. Workflow Stages

### Stage 1: Lint (Ruff)
Checks code quality and style.

### Stage 2: Test (pytest)
Runs all unit tests and generates coverage report.

### Stage 3: Validate Bundle
Validates databricks.yml configuration.

### Stage 4: Deploy to Dev
Deploys to development workspace.

### Stage 5: Security Scan
Scans for vulnerabilities using Safety and Bandit.

## 5. Enable Branch Protection

1. Go to **Settings** → **Branches**
2. Click **Add rule** for `main`
3. Enable:
   - ✅ Require pull request before merging
   - ✅ Require status checks to pass
4. Select required checks: `lint`, `test`, `validate-bundle`
5. Save changes

## 6. Troubleshooting

### Deployment Fails

**Error:** `Error: invalid Databricks Host/Token`

**Solution:**
- Verify secrets are set correctly
- Confirm token hasn't expired
- Check token has workspace access

### Tests Fail

**Solution:**
- Ensure all dependencies in requirements.txt
- Check Python version matches (3.10+)
- Clear pytest cache

### Bundle Validation Fails

**Solution:**
- Check YAML syntax
- Verify all file paths exist
- Ensure resource names are unique

## 7. Best Practices

### Commit Messages

Use conventional commits:
- `feat:` New feature
- `fix:` Bug fix
- `test:` Add/update tests
- `docs:` Documentation
- `refactor:` Code refactoring
- `ci:` CI/CD changes

### Testing Before Push

Run tests locally before pushing:
- `./run_tests.sh all`
- Check linting with Ruff
- Validate bundle configuration

## Support

For issues:
- Check .github/workflows/README.md
- Review workflow logs in GitHub Actions
- Consult Databricks Asset Bundles documentation