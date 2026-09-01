# Security Policy

## Internal Use Only - BioNews Proprietary

This is proprietary software for authorized BioNews personnel only.

## Reporting Security Issues

**INTERNAL SECURITY ISSUES**: Report immediately to BioNews security team.

### How to Report

1. **Email**: security@bionews.com
2. **Include**:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Affected systems

### Response Time

- **Acknowledgment**: Within 24 hours
- **Assessment**: Within 3 business days
- **Resolution**: Based on severity (Critical: 7 days, High: 14 days)

## Security Best Practices

### Credential Management

1. **Never commit credentials**:
   - `.env` file is git-ignored
   - Use GCP Secret Manager for production
   - Rotate credentials quarterly

2. **Service Accounts**:
   - Use minimal required permissions
   - Separate service accounts per environment (dev/prod)
   - Enable audit logging

### GCP Security

1. **BigQuery**:
   - Use dataset-level permissions
   - Enable query audit logs
   - Implement column-level security for PII

2. **GCS Buckets**:
   - Private access only
   - Enable versioning
   - Use signed URLs for temporary access
   - Set lifecycle policies

3. **IAM**:
   - Follow principle of least privilege
   - Use service accounts, not user accounts
   - Enable MFA for admin access

### API Security

1. **Facebook API**:
   - Use short-lived access tokens
   - Store tokens in secret manager
   - Implement rate limiting
   - Log API errors without exposing tokens

2. **WordPress**:
   - Use SSH keys, not passwords
   - Restrict SSH access by IP
   - Keep WordPress and plugins updated

### Data Protection

1. **In Transit**:
   - All connections use TLS 1.2+
   - Verify SSL certificates

2. **At Rest**:
   - GCS buckets use encryption (default)
   - BigQuery tables encrypted
   - Local caching disabled for sensitive data

3. **PII Handling**:
   - Identify PII fields in schemas
   - Implement column-level security
   - Audit PII access
   - Follow data retention policies

## Known Risks

### Environment Variables
`.env` file contains sensitive credentials:
- `FB_ACCESS_TOKEN`
- `GOOGLE_APPLICATION_CREDENTIALS`
- Database passwords

**Mitigation**: File is git-ignored, use secret manager in production

### Log Files
Logs may contain sensitive query results.

**Mitigation**:
- Logs are git-ignored
- Implement log rotation
- Sanitize logs before archiving

## Compliance

This software handles:
- Customer data (Facebook, WordPress)
- Analytics data
- Potentially PII depending on configuration

**Compliance Requirements**:
- GDPR (if EU data)
- CCPA (if California data)
- BioNews data governance policies

## Incident Response

In case of security breach:

1. **Immediate**:
   - Disable compromised credentials
   - Isolate affected systems
   - Notify security team

2. **Within 24 Hours**:
   - Assess impact
   - Document incident
   - Begin remediation

3. **Follow-up**:
   - Root cause analysis
   - Implement preventive measures
   - Update security policies

## Security Contacts

- **Security Team**: security@bionews.com
- **Data Team**: data-team@bionews.com

---

*Last updated: 2025-10-23*
*Internal use only - Do not distribute*
*Copyright © 2025 Bionews. All Rights Reserved.*
