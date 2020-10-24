
# Contributing

Some of the features of avrow are feature gated.
While making changes it's a good idea to build and
test with `--all-features` flag.

## Building the project

```
cargo build --all-features
```

## Running test cases

```
cargo test --all-features
```

## Generating and opening documentation locally

```
BROWSER=firefox cargo doc --no-deps --open
```

When contributing to this repository, please discuss the change you wish to make via issue,
email, or any other method with the owners of this repository before making a change. 

Please note we have a [code of conduct](./CODE_OF_CONDUCT.md), please follow it in all your interactions with the project.

## Pull Request Process

Following is a cursory guideline on how to make the process of making changes more efficient for the contributer and the maintainer.

1. File an issue for the change you want to make. This way we can track the why of the change. 
   Get consensus from community for the change.
2. Clone the project and perform a fresh build. Create a branch with the naming "feature/issue-number.
3. Ensure that the PR only changes the parts of code which implements/solves the issue. This includes running
   the linter (cargo fmt) and removing any extra spaces and any formatting that accidentally were made by
   the code editor in use.
4. If your PR has changes that should also reflect in README.md, please update that as well.
5. Document non obvious changes and the `why` of your changes if it's unclear. 
6. If you are adding a public API, add the documentation as well.
7. Increase the version numbers in Cargo.toml files and the README.md to the new version that this
   Pull Request would represent. The versioning scheme we use is [SemVer](http://semver.org/).
8. Update the CHANGELOG.md to reflect the change if applicable.

More details: https://github.community/t/best-practices-for-pull-requests/10195