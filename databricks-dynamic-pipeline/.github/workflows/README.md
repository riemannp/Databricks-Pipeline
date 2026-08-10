# CI/CD Pipeline Documentation

## Overview

This directory contains GitHub Actions workflows for automated testing, validation, and deployment of the Databricks pipeline.

## Workflow: `ci_cd.yml`

### Triggers
- **Push** to `main`, `dev`, or `feature/**` branches
- **Pull Requests** to `main` branch

### Jobs

#### 1. **lint-and-test** - Code Quality & Unit Tests
Runs on every push and PR.

**Steps:**
- Checkout code
- Setup Python 3.10
- Install dependencies from `requirements.txt`
- Run Ruff linter (ignoring E501 line length)
- Execute pytest with coverage reporting
- Upload coverage report as artifact (30-day retention)

**Coverage Report:**
- View in Actions tab → Workflow run → Artifacts → `coverage-report`
- Download and open `htmlcov/index.html` in browser

#### 2. **bundle-deploy** - Validate & Deploy
Runs after successful tests.

**Steps:**
- Validate bundle with `--strict` flag
- Generate bundle summary
- **Dev deployment**: On feature/dev branches
- **Production deployment**: On merge to `main`
- Create timestamped git tag for production deploys

**Deployment Flow:**
```
feature/my-feature → Deploy to dev
        ↓
    PR to main
        ↓
    Merge → Deploy to prod + Tag
```

#### 3. **integration-tests** - Post-Deploy Validation
Runs after dev deployments (not on main).

**Current State:** Placeholder for future integration tests

**Planned:**
- Run end-to-end test job in Databricks
- Validate data pipeline execution
- Check data quality metrics

#### 4. **security-scan** - Security Checks
Runs in parallel with other jobs.

**Checks:**
- **Safety**: Scans dependencies for known vulnerabilities
- **Bandit**: Analyzes Python code for security issues
- Uploads security reports as artifacts

---

## Setup Instructions

### 1. Configure GitHub Secrets

Go to: **Repository Settings → Secrets and variables → Actions**

Add these secrets:

```bash
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi123456789...
```

**To create a Databricks token:**
1. In Databricks workspace: User Settings → Developer → Access Tokens
2. Click "Generate new token"
3. Set comment: "GitHub Actions CI/CD"
4. Set lifetime: 90 days (recommended)
5. Copy token immediately (shown only once)

### 2. Branch Protection Rules (Recommended)

Go to: **Repository Settings → Branches → Add rule**

For `main` branch:
- ✅ Require pull request reviews before merging
- ✅ Require status checks to pass before merging
  - Select: `lint-and-test`
  - Select: `bundle-deploy`
  - Select: `security-scan`
- ✅ Require branches to be up to date before merging
- ✅ Do not allow bypassing the above settings

### 3. Verify Workflow

**Test on feature branch:**
```bash
git checkout -b feature/test-ci-cd
# Make a small change
echo "# Test" >> README.md
git add README.md
git commit -m "Test CI/CD pipeline"
git push origin feature/test-ci-cd
```

**Check Actions tab:**
- All jobs should run
- Dev deployment should complete
- Coverage report should be available

---

## Viewing Results

### Coverage Report
1. Go to Actions tab
2. Click on workflow run
3. Scroll to Artifacts section
4. Download `coverage-report.zip`
5. Extract and open `htmlcov/index.html`

### Bundle Summary
1. Go to Actions tab
2. Click on workflow run
3. Click on `bundle-deploy` job
4. Expand "Generate Bundle Summary" step
5. View deployed resources

### Security Reports
1. Go to Actions tab
2. Click on workflow run
3. Download `security-reports` artifact
4. Review `bandit_report.txt`

---

## Troubleshooting

### Workflow fails with "DATABRICKS_HOST not found"
**Solution:** Verify GitHub secrets are configured correctly

### Bundle validation fails
**Solution:** Run locally first:
```bash
databricks bundle validate --strict -t dev
```

### Tests pass locally but fail in CI
**Solution:** Check Python version matches (3.10)
```bash
python --version
```

### Deployment succeeds but job not updated
**Solution:** Check bundle target matches workspace
```bash
databricks bundle summary -t dev
```

---

## Best Practices

1. **Always create feature branches** from `dev`
2. **Run tests locally** before pushing:
   ```bash
   pytest tests/ -v --cov=src
   ruff check src/ tests/
   databricks bundle validate -t dev
   ```
3. **Review coverage report** - aim for 80%+ coverage
4. **Tag releases** are created automatically for production deploys
5. **Monitor workflow runs** - set up email notifications in GitHub

---

## Next Steps

- [ ] Add integration test job ID to workflow
- [ ] Configure Slack/email notifications
- [ ] Add performance benchmarking
- [ ] Implement blue/green deployments for zero-downtime
- [ ] Add rollback automation
