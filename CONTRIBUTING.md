# Contributing to PostgrestParser

Thank you for your interest in contributing to PostgrestParser! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Making Changes](#making-changes)
- [Testing](#testing)
- [Submitting a Pull Request](#submitting-a-pull-request)
- [Style Guidelines](#style-guidelines)

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally
3. Set up the development environment (see below)
4. Create a branch for your changes
5. Make your changes with tests
6. Submit a pull request

## Development Setup

### Prerequisites

- Elixir 1.14 or later
- Erlang/OTP 25 or later
- PostgreSQL 15+ (for integration tests)
- Docker and Docker Compose (optional, for running test database)

### Installation

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/postgrest_parser.git
cd postgrest_parser

# Install dependencies
mix deps.get

# Run unit tests
mix test --exclude integration
```

### Running Integration Tests

Integration tests require a PostgreSQL database:

```bash
# Start the test database
docker-compose up -d

# Wait for it to be ready
docker-compose exec postgres pg_isready -U postgres -d postgrest_parser_test

# Run all tests including integration
mix test
```

## Making Changes

### Branch Naming

Use descriptive branch names:
- `feature/add-new-operator` - For new features
- `fix/handle-empty-filters` - For bug fixes
- `docs/improve-readme` - For documentation changes
- `refactor/simplify-parser` - For refactoring

### Commit Messages

Write clear, concise commit messages:
- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Fix bug" not "Fixes bug")
- Limit the first line to 72 characters
- Reference issues when applicable

Example:
```
Add support for the 'between' operator

Implements the 'between' range operator for numeric and date columns.
This allows queries like `age=between.18,65`.

Closes #123
```

## Testing

All changes must include appropriate tests:

- **Unit tests** for parser logic and SQL generation
- **Integration tests** for database interaction (when applicable)
- **Doctests** for public API functions

### Running Tests

```bash
# Run all tests
mix test

# Run only unit tests
mix test --exclude integration

# Run a specific test file
mix test test/postgrest_parser_test.exs

# Run with coverage
mix test --cover
```

### Test Requirements

- All existing tests must pass
- New features must include tests
- Bug fixes should include a regression test
- Doctests are encouraged for public functions

## Submitting a Pull Request

1. **Update your fork** with the latest upstream changes
2. **Run all tests** and ensure they pass
3. **Format your code** with `mix format`
4. **Push your branch** to your fork
5. **Open a pull request** against the `main` branch

### Pull Request Checklist

- [ ] Tests pass locally (`mix test`)
- [ ] Code is formatted (`mix format --check-formatted`)
- [ ] Documentation is updated (if applicable)
- [ ] CHANGELOG is updated (for notable changes)
- [ ] PR description explains the change

### Review Process

1. A maintainer will review your PR
2. Address any feedback or requested changes
3. Once approved, a maintainer will merge your PR

## Style Guidelines

### Code Style

- Follow standard Elixir conventions
- Use `mix format` to format all code
- Keep functions small and focused
- Use pattern matching over conditionals when possible
- Prefer `with` statements for chaining operations

### Documentation

- Add `@moduledoc` to all modules
- Add `@doc` to public functions
- Include examples in documentation (these become doctests)
- Keep documentation up-to-date with code changes

### Example

```elixir
defmodule PostgrestParser.Example do
  @moduledoc """
  Example module demonstrating documentation style.
  """

  @doc """
  Parses the given input string.

  ## Examples

      iex> PostgrestParser.Example.parse("test")
      {:ok, "test"}
  """
  @spec parse(String.t()) :: {:ok, String.t()} | {:error, String.t()}
  def parse(input) when is_binary(input) do
    {:ok, input}
  end
end
```

## Questions?

If you have questions, feel free to:
- Open an issue for discussion
- Ask in the pull request

Thank you for contributing!
