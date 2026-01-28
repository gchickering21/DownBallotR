# DownBallotR

<!-- badges: start -->
[![R-CMD-check](https://github.com/gchickering21/Downballot/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/gchickering21/Downballot/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

`DownBallotR` is an R package that integrates R workflows with a managed Python
environment (via **reticulate**) for tasks that require Python-based tooling.

To ensure reliability and reproducibility, **DownBallotR manages its own Python
virtual environment**. Users do not need to configure Python manually, but a
one-time setup step is required.

This README covers:

1. Installing the package
2. First-time Python setup (required once)
3. Normal day-to-day usage
4. Using the package across multiple R sessions
5. How to verify and troubleshoot the Python environment

---

## 1. Install the package

You can install the development version from GitHub:

```r
install.packages("pak")
pak::pak("gchickering21/Downballot")
```

Or using `remotes`:

```r
install.packages("remotes")
remotes::install_github("gchickering21/Downballot")
```

Then load the package:

```r
library(DownBallotR)
```

---

## 2. First-time setup (required once)

After installing the R package, you must set up the Python environment used by
`DownBallotR`.

### Step 1: Install Python dependencies

Run the following **once**:

```r
downballot_install_python()
```

This command:

- Creates a dedicated Python virtual environment for `DownBallotR`
- Installs required Python packages
- Installs Playwright and its Chromium browser  
  (this may download ~100–200MB the first time)

This step may take a few minutes.

---

### Step 2: Verify the setup

After installation completes, check the status:

```r
downballot_python_status()
```

You should see output indicating:

- The virtual environment exists
- Required Python packages are installed
- Playwright Chromium is available

At this point, Python is installed but **not yet active** in your R session.

---

## 3. Normal usage (each R session)

Each R session must explicitly activate the Python environment **once per
session**.

### Activate Python for the session

At the start of any R session where you plan to use `DownBallotR`, run:

```r
downballot_use_python()
```

This:

- Pins `reticulate` to the correct Python virtual environment
- Safely initializes Python for the current R session

You only need to run this once per session.

---

### Optional: check status

At any time, you can check the current state of the Python environment:

```r
downballot_python_status()
```

This reports:

- Whether Python is initialized
- Which Python interpreter is active
- Whether required packages are available

---

## 4. Using DownBallotR in future or concurrent sessions

### New R session?

If you restart R, open a new RStudio window, or start a separate R process,
simply run:

```r
downballot_use_python()
```

You **do not** need to reinstall Python.

---

### Multiple R sessions at the same time

If you have multiple R sessions open concurrently:

- Each session must call `downballot_use_python()` once
- All sessions can safely share the same virtual environment

---

## 5. Common issues and fixes

### Python is not initialized or packages appear missing

Run:

```r
downballot_use_python()
downballot_python_status()
```

If packages are still missing:

```r
downballot_install_python(reinstall = TRUE)
```

---

### “reticulate is already initialized to a different Python interpreter”

This occurs when Python is initialized **before** calling
`downballot_use_python()`.

Fix:

1. Restart your R session
2. Immediately run:
   ```r
   downballot_use_python()
   ```

Then continue using the package.

---

### Reinstall everything from scratch

If the environment becomes corrupted or inconsistent:

```r
downballot_install_python(reinstall = TRUE)
downballot_use_python()
downballot_python_status()
```

---

## 6. Recommended workflow summary

### First-time setup:
```r
downballot_install_python()
downballot_use_python()
```

### Every new R session:
```r
downballot_use_python()
```

### Check environment status:
```r
downballot_python_status()
```

---

## 7. Design notes

- `DownBallotR` intentionally does **not** auto-install Python dependencies on
  `library(DownBallotR)`
- This avoids unexpected downloads and ensures predictable behavior
- Python is only initialized when explicitly requested by the user
