# DownBallotR

<!-- badges: start -->
[![R CMD check](https://github.com/gchickering21/DownBallotR/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/gchickering21/DownBallotR/actions/workflows/R-CMD-check.yaml)
[![pkgdown](https://github.com/gchickering21/DownBallotR/actions/workflows/pkgdown.yaml/badge.svg)](https://github.com/gchickering21/DownBallotR/actions/workflows/pkgdown.yaml)
[![Lifecycle: experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html)

<!-- badges: end -->


`DownBallotR` is an R package that integrates R workflows with a managed Python
environment (via **reticulate**) for tasks that require Python-based tooling.

To ensure reliability and reproducibility, **DownBallotR manages its own Python
virtual environment**. Users do not need to configure Python manually, but a
one-time setup step is required.

📘 **Full setup and troubleshooting instructions are in the Python setup vignette:**

- 📄 Source (always available in the repo): [vignettes/python-setup.Rmd](vignettes/python-setup.Rmd)
- 🧭 In R (after installing): `vignette("python-setup", package = "DownBallotR")`
- 🌐 Rendered HTML (pkgdown): https://gchickering21.github.io/DownBallotR/articles/python-setup.html

---

## Design notes

- `DownBallotR` intentionally does **not** auto-install Python dependencies on
  `library(DownBallotR)`
- This avoids unexpected downloads and ensures predictable behavior
- Python is only initialized when explicitly requested by the user
