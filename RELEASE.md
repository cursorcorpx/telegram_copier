# Release Process

## Automatic Release From Tag

1. Push a semantic version tag:

```bash
git tag v1.0.0
git push origin v1.0.0
```

2. GitHub Actions runs:
   - test suite
   - GitHub release creation

## Manual Release From GitHub

1. Open `Actions` in GitHub.
2. Select `Release`.
3. Click `Run workflow`.
4. Enter a semantic version like `v1.0.0`.

The workflow will:
- validate the version
- run tests
- create and push the tag
- create the GitHub release with generated notes

## Notes

- Release workflow does not change runtime code.
- Docker publishing remains in `appwrite-ci-cd.yml`.
- Appwrite deploy approval remains controlled by the `production` environment.
