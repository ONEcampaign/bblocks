# Contributing

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given.

To contribute to the `bblocks` ecosystem please visit the relevant package's
GitHub repository. General issues, questions, and bug reports can be reported 
in the main `bblocks` repository.

## Types of Contributions

### Report Bugs

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

### Fix Bugs

Look through the GitHub issues for bugs. Anything tagged with "bug" and "help
wanted" is open to whoever wants to implement it.

### Implement Features

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

### Write Documentation

You can never have enough documentation! Please feel free to contribute to any
part of the documentation, such as the official docs, docstrings, or even
on the web in blog posts, articles, and such. Documentation is built using
[mkdocs] and documentation files are located in the `docs/` directory.

### Submit Feedback

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a public good project: the tools will always be 
open source, and contributions are welcome!

## Get Started!

Ready to contribute? Here are the steps to get set up for local development

1. Navigate to the package's GitHub repository. `bblocks` is a namespace package where each
package lives in its own repository. For example, the `bblocks-places` package lives in the `bblocks-places` repository
here: https://github.com/ONEcampaign/bblocks-places

2. Open an issue in the repository to discuss your proposed changes or enhancements. 
   This is a good way to get feedback and ensure your work aligns with the project's goals.

3. Fork the repository and clone your fork to your local machine.

4. Dependencies are managed using poetry. Install poetry in your virtual environment and 
install the package:

    ```console
    $ poetry install
    ```

5. Develop your changes and enhancements. Ensure tests are written for any new functionality
and that existing tests pass. Code coverage is important but not critical. We aim for at least
90% coverage, but we prioritize meaningful tests that cover all major functionality. Code should be formatted 
using `black` and adhere to the project's coding standards.

6. When you are done developing, open a pull request against the main repository, with a clear description of 
your changes and any relevant issue numbers. One of the maintainers will review your changes and provide feedback.

7. If your pull request is accepted, it will be merged into the main branch. Your changes will be included in the 
next release, and your contributions will be acknowledged in the project's changelog.


## Pull Request Guidelines

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include additional tests if appropriate.
2. If the pull request adds functionality, the docs should be updated.
3. The pull request should work for all currently supported operating systems and versions of Python.
