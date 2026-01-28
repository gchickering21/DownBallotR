# DownBallotR

`DownBallotR` is an R package that integrates R workflows with a managed
Python environment (via **reticulate**) for tasks that require
Python-based tooling.

To ensure reliability and reproducibility, **DownBallotR manages its own
Python virtual environment**. Users do not need to configure Python
manually, but a one-time setup step is required.

📘 **Full setup and troubleshooting instructions are provided in the
Python setup vignette:**

- 📄 [Python setup
  vignette](https://gchickering21.github.io/DownBallotR/articles/python-setup.html)

After installing the package, you can also view it in R:

`{r} vignette("python-setup", package = "DownBallotR")`

------------------------------------------------------------------------

## Design notes

- `DownBallotR` intentionally does **not** auto-install Python
  dependencies on
  [`library(DownBallotR)`](https://rdrr.io/r/base/library.html)
- This avoids unexpected downloads and ensures predictable behavior
- Python is only initialized when explicitly requested by the user
