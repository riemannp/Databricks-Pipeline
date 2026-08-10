# CI/CD Setup Guide - Quick Start

## ⚡ 5-Minute Setup

### Step 1: Create Databricks Access Token

1. Go to your Databricks workspace
2. Click your profile → **User Settings**
3. Navigate to **Developer** → **Access Tokens**
4. Click **Generate new token**
5. Settings:
   - **Comment**: `GitHub Actions CI/CD`
   - **Lifetime**: 90 days
6. **IMPORTANT**: Copy the token immediately (shown only once)

### Step 2: Add GitHub Secrets

1. Go to your GitHub repository
2. Navigate to: **Settings → Secrets and variables → Actions**
3. Click **New repository secret**
4. Add these two secrets:

**Secret 1:**
```
Name: DATABRICKS_HOST
Value: https://your-workspace.cloud.databricks.com
```
(Replace with your actual workspace URL)

**Secret 2:**
```
Name: DATABRICKS_TOKEN
Value: dapi1234567890abcdef...
```
(Paste the token from Step 1)

### Step 3: Test the Workflow

```bash
# Create a test branch
git checkout -b feature/test-ci-cd

# Make a small change
echo "\n# CI/CD Testing" >> README.md

# Commit and push
git add README.md
git commit -m "test: Verify CI/CD pipeline"
git push origin feature/test-ci-cd
```

### Step 4: Watch It Run

1. Go to your GitHub repo → **Actions** tab
2. You should see a workflow run starting
3. Click on it to watch progress
4. Expected result: All jobs pass ✅

---

## ✅ What Runs on Each Push

### On Feature/Dev Branches:
```
1. lint-and-test
   └─ Ruff linting
   └─ Unit tests with coverage
   └─ Upload coverage report

2. bundle-deploy
   └─ Validate bundle (--strict)
   └─ Generate summary
   └─ Deploy to dev environment

3. integration-tests
   └─ Post-deploy validation (placeholder)

4. security-scan
   └─ Safety (dependency vulnerabilities)
   └─ Bandit (code security issues)
```

### On Merge to Main:
```
1. lint-and-test (same as above)

2. bundle-deploy
   └─ Validate bundle
   └─ Deploy to PRODUCTION
   └─ Create git tag (prod-20260806-120000)

3. security-scan (runs in parallel)
```

---

## 📑 Viewing Results

### Test Coverage Report
1. Actions tab → Click workflow run
2. Scroll to **Artifacts** section at bottom
3. Download `coverage-report.zip`
4. Extract and open `htmlcov/index.html`
5. **Goal**: Keep coverage above 80%

### Security Reports
1. Actions tab → Click workflow run
2. Download `security-reports` artifact
3. Review `bandit_report.txt` for security issues

### Bundle Summary
1. Actions tab → Click workflow run
2. Click `bundle-deploy` job
3. Expand "Generate Bundle Summary" step
4. See what resources will be deployed

---

## 🛑 Troubleshooting

### "DATABRICKS_HOST not found" Error
**Fix**: Check that secrets are spelled correctly (case-sensitive)

### "Bundle validation failed"
**Fix**: Test locally first:
```bash
databricks bundle validate --strict -t dev
```

### "Authentication failed"
**Fix**: Token may be expired. Generate a new one:
- Databricks → Settings → Developer → Access Tokens
- Generate new token
- Update GitHub secret `DATABRICKS_TOKEN`

### Tests pass locally but fail in CI
**Fix**: Ensure dependencies match:
```bash
# Install from requirements.txt
pip install -r requirements.txt

# Run tests
pytest tests/ -v --cov=src
```

---

## 🚀 Recommended: Branch Protection

Protect your `main` branch from accidental direct pushes:

1. GitHub repo → **Settings → Branches**
2. Click **Add branch protection rule**
3. Branch name pattern: `main`
4. Enable:
   - ☑ Require pull request reviews before merging
   - ☑ Require status checks to pass before merging
     - Select: `lint-and-test`
     - Select: `bundle-deploy`
     - Select: `security-scan`
   - ☑ Require branches to be up to date before merging
5. Click **Create**

**Result**: No one (including you) can push directly to main without a PR and passing tests!

---

## 📝 Development Workflow

### For New Features:
```bash
# 1. Create feature branch
git checkout -b feature/my-new-feature

# 2. Make changes
vim src/03_gold.py

# 3. Test locally
pytest tests/ -v
ruff check src/ tests/
databricks bundle validate -t dev

# 4. Commit and push
git add .
git commit -m "feat: Add new gold layer aggregation"
git push origin feature/my-new-feature

# 5. CI/CD runs automatically
# - Tests run
# - Bundle validates
# - Deploys to dev

# 6. Create Pull Request on GitHub
# - Use PR template (auto-filled)
# - Review changes
# - Wait for approvals

# 7. Merge to main
# - Automatically deploys to PRODUCTION
# - Git tag created automatically
```

### For Hotfixes:
```bash
# 1. Create hotfix branch from main
git checkout main
git pull
git checkout -b hotfix/fix-critical-bug

# 2. Make fix
vim src/02_silver.py

# 3. Test locally
pytest tests/test_transforms.py -v

# 4. Push and create PR
git add .
git commit -m "fix: Resolve null handling in silver layer"
git push origin hotfix/fix-critical-bug

# 5. Get quick review and merge
# - Production deploy happens automatically
```

---

## ✅ Success Indicators

You'll know CI/CD is working when:

1. ✅ Green checkmarks on all commits in GitHub
2. ✅ Coverage reports available for every push
3. ✅ Bundle deploys automatically to dev on feature branches
4. ✅ Production deploys automatically on main merge
5. ✅ Git tags created for every production release
6. ✅ Security scans run on every push

---

## 📚 Additional Resources

* **Full CI/CD Documentation**: `.github/workflows/README.md`
* **PR Template**: `.github/PULL_REQUEST_TEMPLATE.md`
* **Project Tracking**: `NOTES.md`
* **Databricks CLI**: [Official Docs](https://docs.databricks.com/dev-tools/cli/index.html)

---

## ❓ Need Help?

If something isn't working:

1. Check the **Actions** tab for error details
2. Review `.github/workflows/README.md` troubleshooting section
3. Verify secrets are configured correctly
4. Test commands locally before pushing

---

**You're all set!** 🎉

Your pipeline now has enterprise-grade CI/CD with:
* Automated testing
* Security scanning  
* Dev/prod deployment
* Coverage tracking
* Git tagging

Next step: [Expand Test Coverage](NOTES.md#2-test-coverage-expansion---todo)
