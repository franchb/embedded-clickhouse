# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| latest  | :white_check_mark: |

## Reporting a Vulnerability

We take the security of embedded-clickhouse seriously. If you discover a security vulnerability, please report it using GitHub's **Private Vulnerability Reporting**:

1. Go to the [Security tab](https://github.com/franchb/embedded-clickhouse/security) of this repository.
2. Click **"Report a vulnerability"**.
3. Fill in the details of the vulnerability.

Please include the following in your report:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Any suggested fixes (if applicable)

We will acknowledge your report within 48 hours and provide a detailed response within 5 business days.

**Please do not** disclose the vulnerability publicly until we have had a chance to address it.

## Security Considerations

When using this library, please keep the following in mind:

1. **Network Access**: The library downloads ClickHouse binaries from official sources. Ensure your environment allows outbound HTTPS connections to ClickHouse download servers.

2. **Binary Verification**: Downloaded binaries are cached locally. Review the cache directory permissions (`~/.cache/embedded-clickhouse` by default).

3. **Test Environments Only**: This library is designed for testing. Do not use embedded ClickHouse instances in production.

4. **Dependency Management**: Regularly update your dependencies to pick up security fixes. Run `govulncheck ./...` to check for known vulnerabilities.
