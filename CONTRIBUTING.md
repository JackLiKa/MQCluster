# Contributing to MQCluster

First off — **thank you** for taking the time to contribute! 🎉

MQCluster is a community-driven learning platform, and every contribution — bug reports, feature ideas,
documentation improvements, or code — makes it better for students everywhere.

This document covers everything you need to start contributing effectively.

---

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Reporting Bugs](#reporting-bugs)
- [Requesting Features](#requesting-features)
- [Development Environment Setup](#development-environment-setup)
- [Code Style](#code-style)
- [Adding a New MQ (or a New Tab for All MQs)](#adding-a-new-mq-or-a-new-tab-for-all-mqs)
- [Commit Convention (Conventional Commits)](#commit-convention-conventional-commits)
- [Pull Request Process](#pull-request-process)

---

## Code of Conduct

Be kind, respectful, and constructive. We expect all contributors to maintain a welcoming environment
for everyone regardless of background or experience level. Harassment or discrimination of any kind
will not be tolerated.

---

## Reporting Bugs

Found a bug? Help us fix it!

1. **Search existing issues** — check [open issues](https://github.com/JackLiKa/MQCluster/issues) to avoid duplicates.
2. **Open a new issue** using the **Bug Report** template (if available) or include:
   - **Title** — a concise summary of the problem.
   - **Environment** — OS, Java version, Node version, browser.
   - **Steps to reproduce** — numbered, minimal, and specific.
   - **Expected behavior** — what you thought would happen.
   - **Actual behavior** — what actually happened.
   - **Logs / screenshots** — backend console output, browser console errors, or screenshots.
3. **Be responsive** — maintainers may ask follow-up questions; please reply promptly.

> **Security-related bugs**: do **not** open a public issue. See [SECURITY.md](SECURITY.md).

---

## Requesting Features

Have an idea that would make MQCluster better?

1. **Search existing issues** to see if someone already proposed it.
2. **Open a new issue** with the **Feature Request** label and include:
   - **Use case** — what problem does this solve? Who benefits?
   - **Proposed solution** — describe how you envision it working.
   - **Alternatives considered** — any workarounds or other approaches.
3. **Wait for discussion** — maintainers will triage and provide feedback before implementation begins.

---

## Development Environment Setup

### Prerequisites

| Tool | Version |
| --- | --- |
| JDK | 21+ |
| Maven | 3.9+ (or use the bundled wrapper) |
| Node.js | 20+ |
| npm | 10+ |
| Git | any recent version |

### Clone & run

```bash
git clone https://github.com/JackLiKa/MQCluster.git
cd MQCluster

# Backend
cd java
./mvnw spring-boot:run          # Windows: .\mvnw.cmd spring-boot:run
# → http://localhost:8088

# Frontend (new terminal)
cd next
npm install
npm run dev
# → http://localhost:3000
```

### Recommended IDE setup

- **Backend**: IntelliJ IDEA (with the Spring Boot plugin) or VS Code with Java extension pack.
- **Frontend**: VS Code with ESLint, Prettier, and Tailwind CSS IntelliSense extensions.

---

## Code Style

### Java (Backend)

- Follow the **Google Java Style Guide** as a baseline.
- Use **4-space indentation**.
- Package names are lowercase: `com.example.clustermanager.*`.
- Class names are `PascalCase`; methods and fields are `camelCase`.
- Constants are `UPPER_SNAKE_CASE`.
- Keep the hexagonal layering strict:
  - `core` must have **zero** external dependencies.
  - `application` depends only on `core`.
  - `infrastructure` implements `core` ports.
  - `api` is the only layer that talks to the outside world.
- Prefer constructor injection over field injection.
- Write tests for new domain logic and adapters.

### TypeScript / React (Frontend)

- **Strict mode** TypeScript — no `any` without a justification comment.
- Use **functional components** and hooks only.
- 2-space indentation.
- Prefer named exports; use default export only for page components.
- Keep components small and composable; extract shared logic into hooks.
- Use Tailwind CSS utility classes for styling; avoid custom CSS unless necessary.
- Run `npx tsc --noEmit` and `npm run lint` before committing — both must pass.

---

## Adding a New MQ (or a New Tab for All MQs)

The hexagonal layout means a new middleware touches each layer exactly once. Follow this order:

1. **Core** — add the `MiddlewareType` constant, the domain models and the port interfaces
   (`IClusterProvider` plus optional capability ports such as `IKafkaInspector`, `IStressLab`,
   `IInternalsInspector`). `core` must stay dependency-free.
2. **Infrastructure** — create `infrastructure/<middleware>` with the provider and its runtime; if the
   MQ should support load experiments, implement `IStressLab` with a `StressSender` and reuse
   `infrastructure/lab/StressRunner` instead of writing another engine.
3. **Capabilities** — declare them in the provider's `ProviderDescriptor` (`ProviderCapabilities`), the
   frontend uses them to decide which tabs appear.
4. **API** — add a controller only for MQ-specific routes; `/lab/stress` and `/lab/internals` are shared
   by `LabController`.
5. **Frontend** — register the MQ in `lib/mq-registry.ts` (name, accent, role → shape, tabs with
   `requires`), then add components under `components/<middleware>` or reuse `components/lab`.
6. **Tests** — a provider test for lifecycle behaviour and a black-box API test; then `./mvnw clean verify`.
7. **Docs** — update the README tables, `AGENTS.md`, and both `docs/ARCHITECTURE.md` and
   `docs/zh/ARCHITECTURE.zh.md` (the two files are kept structurally identical).

---

## Commit Convention (Conventional Commits)

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification.

### Format

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

### Types

| Type | Description |
| --- | --- |
| `feat` | A new feature |
| `fix` | A bug fix |
| `docs` | Documentation only changes |
| `style` | Code style changes (formatting, no logic change) |
| `refactor` | Code changes that neither fix a bug nor add a feature |
| `perf` | Performance improvements |
| `test` | Adding or correcting tests |
| `chore` | Build, tooling, or dependency changes |
| `ci` | CI pipeline changes |

### Scope

Use the module or layer name, e.g. `core`, `api`, `infra`, `frontend`, `docs`.

### Examples

```
feat(frontend): add real-time CPU sparkline to node cards
fix(infra): correct broker restart race condition
docs: update Quick Start prerequisites table
test(core): add unit tests for cluster state transitions
```

> The `#` issue number is optional in the subject but encouraged in the footer for PRs that close issues:
>
> ```
> fix(api): handle null cluster id in topology endpoint
>
> Closes #42
> ```

---

## Pull Request Process

1. **Fork** the repository and create a feature branch from `main`:
   ```bash
   git checkout -b feat/my-awesome-feature
   ```
2. **Make your changes** following the code style above.
3. **Write or update tests** as appropriate.
4. **Run checks locally**:
   ```bash
   # Backend
   cd java && ./mvnw clean verify

   # Frontend
   cd next && npx tsc --noEmit && npm run lint && npm run build
   ```
5. **Commit** using Conventional Commits (see above).
6. **Push** your branch and open a Pull Request against `main`.
7. **Fill in the PR template** — describe what changed, why, and how it was tested.
8. **Link related issues** (e.g., `Closes #42`).
9. **Address review feedback** — push additional commits to the same branch (do not squash until merge).
10. **A maintainer will review and merge** once CI passes and the review is approved.

### PR checklist

- [ ] Branch is up to date with `main`.
- [ ] Commits follow Conventional Commits.
- [ ] Backend tests pass (`./mvnw clean verify`).
- [ ] Frontend type-check, lint and build pass.
- [ ] Documentation updated if behavior changed — including `docs/zh/ARCHITECTURE.zh.md` when
      `docs/ARCHITECTURE.md` changes.
- [ ] Relative links still resolve (`python3 scripts/doc-lint.py`).
- [ ] No secrets or credentials committed.

---

## Questions?

Feel free to [open a discussion](https://github.com/JackLiKa/MQCluster/discussions) or an issue —
we're happy to help you get started.

Happy hacking! 🚀
