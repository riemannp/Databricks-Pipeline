## Description
<!-- Provide a brief description of the changes in this PR -->

## Type of Change
<!-- Mark the relevant option with an 'x' -->

- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update
- [ ] Configuration change
- [ ] Performance improvement
- [ ] Code refactoring

## Related Issues
<!-- Link to related issues using #issue_number -->

Closes #

## Changes Made
<!-- Provide a detailed list of changes -->

* 
* 
* 

## Testing

### Unit Tests
- [ ] New tests added for new functionality
- [ ] All existing tests pass
- [ ] Coverage maintained or improved (target: 80%+)

### Integration Tests
- [ ] Tested in dev environment
- [ ] Data pipeline runs successfully end-to-end
- [ ] Validated output data quality

### Manual Testing
<!-- Describe manual testing performed -->

**Test Steps:**
1. 
2. 
3. 

**Expected Behavior:**


**Actual Behavior:**


## Impact Analysis

### Data Impact
- [ ] No data impact
- [ ] Schema changes required
- [ ] New tables created
- [ ] Existing tables modified

### Performance Impact
- [ ] No performance impact
- [ ] Performance improvement
- [ ] Potential performance degradation (justified below)

### Deployment Impact
- [ ] No deployment changes needed
- [ ] Requires configuration updates
- [ ] Requires data migration
- [ ] Requires compute restart

## Checklist

### Code Quality
- [ ] Code follows project style guidelines
- [ ] Ruff linter passes with no warnings
- [ ] No hardcoded credentials or sensitive data
- [ ] Error handling implemented appropriately
- [ ] Logging added for debugging

### Documentation
- [ ] Code comments added for complex logic
- [ ] README updated if needed
- [ ] Configuration changes documented
- [ ] NOTES.md updated with lessons learned

### Security
- [ ] No security vulnerabilities introduced
- [ ] Dependencies checked for vulnerabilities (Safety scan passes)
- [ ] Secrets/tokens properly managed
- [ ] Input validation implemented

### Databricks-Specific
- [ ] Bundle validates successfully (`databricks bundle validate --strict`)
- [ ] Compatible with Databricks Runtime (DBR 14.3+)
- [ ] Unity Catalog permissions verified
- [ ] Checkpoint/state management handled correctly

## Deployment Plan

### Pre-Deployment
<!-- Actions needed before deployment -->

- [ ] Backup production data
- [ ] Notify stakeholders
- [ ] Schedule maintenance window (if needed)

### Deployment Steps
<!-- Automated via CI/CD, but note any manual steps -->

1. Merge to main triggers production deployment
2. 
3. 

### Post-Deployment
<!-- Verification steps after deployment -->

- [ ] Verify job runs successfully
- [ ] Check data quality metrics
- [ ] Monitor error logs
- [ ] Validate Gold layer aggregates

## Rollback Plan
<!-- How to revert if issues occur -->

**If issues occur:**
1. Revert commit: `git revert <commit-hash>`
2. Redeploy previous version
3. Restore from backup (if data impact)

## Screenshots/Logs
<!-- Add relevant screenshots, logs, or output -->

<!-- Example:
![Coverage Report](link-to-image)
```
Job execution logs...
```
-->

## Additional Notes
<!-- Any additional information reviewers should know -->


---

## Reviewer Checklist
<!-- For reviewers -->

- [ ] Code changes reviewed and approved
- [ ] Tests are comprehensive and pass
- [ ] Documentation is clear and complete
- [ ] No security concerns
- [ ] Bundle validation passes in CI/CD
- [ ] Ready for production deployment
