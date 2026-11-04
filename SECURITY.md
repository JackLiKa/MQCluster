# Security Policy

## Supported Versions

MQCluster is a local-first learning tool and is under active development — the version in
`java/pom.xml` may be a `-SNAPSHOT` ahead of the latest release tag. Security fixes are applied to the
latest `main` branch and the most recent release.

| Version | Supported |
| --- | --- |
| latest `main` | ✅ |
| latest release tag | ✅ |
| older releases | ❌ |

---

## Reporting a Vulnerability

**Do NOT open a public GitHub issue for security vulnerabilities.**

If you discover a security vulnerability in MQCluster, please report it responsibly:

1. **Use GitHub Security Advisories** — go to
   [https://github.com/JackLiKa/MQCluster/security/advisories/new](https://github.com/JackLiKa/MQCluster/security/advisories/new)
   and submit a private vulnerability report.
2. Include the following in your report:
   - A description of the vulnerability and its potential impact.
   - Steps to reproduce (proof-of-concept if possible).
   - Affected versions / commits.
   - Any suggested mitigations or fixes.
3. You will receive an acknowledgment within **72 hours**.
4. We will work with you to validate the issue and coordinate a fix and disclosure timeline.

### Coordinated disclosure

We ask that you:

- Do not publicly disclose the vulnerability until a fix has been released.
- Give us a reasonable window (typically 90 days) to develop and ship a patch.
- Credit will be given in the release notes unless you prefer to remain anonymous.

---

## Scope

This policy applies to the **MQCluster** repository at
[https://github.com/JackLiKa/MQCluster](https://github.com/JackLiKa/MQCluster), including:

- The Java backend (`java/`)
- The Next.js frontend (`next/`)
- CI workflows and build configuration

### Out of scope

- Vulnerabilities in third-party dependencies — report these to the upstream project.
- Issues that require already-compromised access to the user's machine.
- Denial-of-service via intentionally resource-exhausting local inputs (the tool runs locally by design).

---

## Security best practices for users

MQCluster is designed to run **locally** on a single machine for learning purposes. Keep in mind:

- The backend listens on **port 8088** and the frontend on **port 3000** — both bind to `localhost` by default.
  Do **not** expose these ports to the public internet.
- The embedded runtimes store data under `java/run/` (`pseudo-cluster/`, `kafka-cluster/`,
  `rabbitmq-cluster/`) — these directories may contain message payloads. Avoid running MQCluster on shared
  or untrusted machines if you handle sensitive data.
- The **压力实验** tab deliberately saturates the embedded broker (up to 50,000 msg/s for 32 threads).
  Run it on a machine you own; it is a local load test, not a remote attack surface.
- Always run from source or official releases. Do not trust unverified binaries.

---

## Acknowledgements

We thank all security researchers and contributors who report vulnerabilities responsibly.
Your efforts keep the open-source community safe.
