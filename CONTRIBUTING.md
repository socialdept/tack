# Contributing

Contributions are **welcome** and will be fully **credited**.

## Etiquette

This project is open source, and as such, the maintainers give their free time to build and maintain the source code held within. They make the code freely available in the hope that it will be of use to other developers. It would be extremely unfair for them to suffer abuse or anger for their hard work.

Please be considerate towards maintainers when raising issues or presenting pull requests. Let's show the world that developers are civilized and selfless people.

It's the duty of the maintainer to ensure that all submissions to the project are of sufficient quality to benefit the project. Many developers have different skillsets, strengths, and weaknesses. Respect the maintainer's decision, and do not be upset or abusive if your submission is not used.

## Viability

When requesting or submitting new features, first consider whether it might be useful to others. Open source projects are used by many developers, who may have entirely different needs to your own. Think about whether or not your feature is likely to be used by other users of the project.
## Procedure

### Before Filing an Issue

- Search existing issues to avoid duplicates
- Check the [documentation](docs/introduction.md) to ensure it's not a usage question
- Provide a clear title and description
- Include steps to reproduce the issue, with the labeler involved
- Specify your environment (binary or Docker image, Tack version, delivery mode)
- Include relevant log output and full error messages, with `TACK_ADMIN_PASSWORD` removed

### Before Submitting a Pull Request

- **Discuss non-trivial changes first** by opening an issue
- **Fork the repository** and create a feature branch from `main`
- **Follow all requirements** listed below
- **Write tests** for your changes
- **Update documentation** if behavior changes
- **Run code style checks** with `gofmt -w .` and `go vet ./...`
- **Ensure all tests pass** with `go test -race ./...`
- **Write clear commit messages** that explain what and why

## Requirements

- **Formatted with gofmt** - CI fails on any file `gofmt -l .` lists.
- **Add tests** - Your patch won't be accepted if it doesn't have tests. Tests must pass with the race detector on.
- **Keep delivery at least once** - An event is acknowledged only after the app has it. Changes to the outbox, retries or acks need a test that covers a dropped connection or a failed webhook.
- **Document any change in behavior** - Make sure the `README.md` and the pages in `docs/` are kept up-to-date.
- **Consider our release cycle** - We follow [SemVer v2.0.0](https://semver.org/). Event payloads, the webhook and websocket protocols, the admin API and `TACK_*` settings are public interfaces. Breaking them needs a major version.
- **One pull request per feature** - If you want to do more than one thing, send multiple pull requests.
- **Send coherent history** - Make sure each individual commit in your pull request is meaningful. If you had to make multiple intermediate commits while developing, please [squash them](https://www.git-scm.com/book/en/v2/Git-Tools-Rewriting-History#Changing-Multiple-Commit-Messages) before submitting.

## How Changes Land

`main` is protected. Every change arrives by pull request, and direct pushes are
blocked.

- **Merges are squash-only, and the PR title becomes the permanent commit
  message on `main`.** Write it as one descriptive line. Individual commit
  messages are preserved in the body.
- **Two checks must pass:** `Tests (Go 1.26)` and `gofmt`.
  Your branch must also be up to date with `main` before merging.
- No approval is required, but review threads must be resolved.

## Running Tests

```bash
go vet ./...
go test -race ./...
```

## Code Style

This project uses gofmt. Run it before submitting:

```bash
gofmt -w .
```
