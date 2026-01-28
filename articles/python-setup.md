# Python setup for DownBallotR

## 1. Install the package

You can install the development version from GitHub:

``` r
install.packages("pak")
#> Installing package into '/home/runner/work/_temp/Library'
#> (as 'lib' is unspecified)
pak::pak("gchickering21/Downballot")
#> ℹ Loading metadata database
#> ✔ Loading metadata database ... done
#> 
#> 
#> → Will update 1 package.
#> → The package (0 B) is cached.
#> + DownBallotR 0.0.0.9000 → 0.0.0.9000 [bld][cmp] (GitHub: 238bfd4)
#> ✔ All system requirements are already installed.
#> 
#> ℹ No downloads are needed, 1 pkg is cached
#> ✔ Got DownBallotR 0.0.0.9000 (source) (20.52 kB)
#> ℹ Installing system requirements
#> ℹ Executing `sudo sh -c apt-get -y update`
#> Get:1 file:/etc/apt/apt-mirrors.txt Mirrorlist [144 B]
#> Hit:2 http://azure.archive.ubuntu.com/ubuntu noble InRelease
#> Hit:6 https://packages.microsoft.com/repos/azure-cli noble InRelease
#> Hit:7 https://packages.microsoft.com/ubuntu/24.04/prod noble InRelease
#> Hit:3 http://azure.archive.ubuntu.com/ubuntu noble-updates InRelease
#> Hit:4 http://azure.archive.ubuntu.com/ubuntu noble-backports InRelease
#> Hit:5 http://azure.archive.ubuntu.com/ubuntu noble-security InRelease
#> Reading package lists...
#> ℹ Executing `sudo sh -c apt-get -y install libpng-dev python3`
#> Reading package lists...
#> Building dependency tree...
#> Reading state information...
#> libpng-dev is already the newest version (1.6.43-5ubuntu0.3).
#> python3 is already the newest version (3.12.3-0ubuntu2.1).
#> 0 upgraded, 0 newly installed, 0 to remove and 51 not upgraded.
#> ℹ Packaging DownBallotR 0.0.0.9000
#> ✔ Packaged DownBallotR 0.0.0.9000 (708ms)
#> ℹ Building DownBallotR 0.0.0.9000
#> ✔ Built DownBallotR 0.0.0.9000 (1.9s)
#> ✔ Installed DownBallotR 0.0.0.9000 (github::gchickering21/Downballot@238bfd4) (19ms)
#> ✔ 1 pkg + 12 deps: kept 12, upd 1, dld 1 (NA B) [11.3s]
```

Or using `remotes`:

``` r
install.packages("remotes")
#> Installing package into '/home/runner/work/_temp/Library'
#> (as 'lib' is unspecified)
remotes::install_github("gchickering21/Downballot")
#> Using github PAT from envvar GITHUB_PAT. Use `gitcreds::gitcreds_set()` and unset GITHUB_PAT in .Renviron (or elsewhere) if you want to use the more secure git credential store instead.
#> Skipping install of 'DownBallotR' from a github remote, the SHA1 (238bfd46) has not changed since last install.
#>   Use `force = TRUE` to force installation
```

Then load the package:

``` r
library(DownBallotR)
#> downballot: Python dependencies are not set up yet.
#> Run: downballot_install_python()
#> Then: downballot_use_python()
```

## 2. First-time setup (required once)

After installing the R package, you must set up the Python environment
used by `DownBallotR`.

### Step 1: Install Python dependencies

Run the following **once**:

``` r
downballot_install_python()
#> Creating virtualenv 'downballotR' ...
#> Using Python: /usr/bin/python3.12
#> Creating virtual environment 'downballotR' ...
#> + /usr/bin/python3.12 -m venv /home/runner/.virtualenvs/downballotR
#> Done!
#> Installing packages: pip, wheel, setuptools
#> + /home/runner/.virtualenvs/downballotR/bin/python -m pip install --upgrade pip wheel setuptools
#> Installing packages: numpy
#> + /home/runner/.virtualenvs/downballotR/bin/python -m pip install --upgrade --no-user numpy
#> Virtual environment 'downballotR' successfully created.
#> Installing Python packages into 'downballotR': pandas, requests, lxml, playwright
#> Using virtual environment 'downballotR' ...
#> + /home/runner/.virtualenvs/downballotR/bin/python -m pip install --upgrade --no-user pandas requests lxml playwright
#> Ensuring Playwright Chromium is installed (may download ~100-200MB)...
#> Python setup complete for env 'downballotR'.
```

This command:

- Creates a dedicated Python virtual environment for `DownBallotR`
- Installs required Python packages
- Installs Playwright and its Chromium browser  
  (this may download ~100–200MB the first time)

This step may take a few minutes.

------------------------------------------------------------------------

### Step 2: Verify the setup

After installation completes, check the status:

``` r
downballot_python_status()
#> 
#>  downballotR Python status
#> --------------------------------------
#> Virtualenv name:         downballotR
#> Virtualenv exists:       TRUE
#> Virtualenv python:       /home/runner/.virtualenvs/downballotR/bin/python
#> reticulate initialized:  TRUE
#> Active python:           /home/runner/.virtualenvs/downballotR/bin/python
#> Python packages:         all required packages correctly installed
#> Playwright Chromium:     correctly installed
#> 
#> - Python environment is ready for use.
```

You should see output indicating:

- The virtual environment exists
- Required Python packages are installed
- Playwright Chromium is available

At this point, Python is installed but **not yet active** in your R
session.

------------------------------------------------------------------------

## 3. Normal usage (each R session)

Each R session must explicitly activate the Python environment **once
per session**.

### Activate Python for the session

At the start of any R session where you plan to use `DownBallotR`, run:

``` r
downballot_use_python()
```

This:

- Pins `reticulate` to the correct Python virtual environment
- Safely initializes Python for the current R session

You only need to run this once per session.

------------------------------------------------------------------------

### Optional: check status

At any time, you can check the current state of the Python environment:

``` r
downballot_python_status()
#> 
#>  downballotR Python status
#> --------------------------------------
#> Virtualenv name:         downballotR
#> Virtualenv exists:       TRUE
#> Virtualenv python:       /home/runner/.virtualenvs/downballotR/bin/python
#> reticulate initialized:  TRUE
#> Active python:           /home/runner/.virtualenvs/downballotR/bin/python
#> Python packages:         all required packages correctly installed
#> Playwright Chromium:     correctly installed
#> 
#> - Python environment is ready for use.
```

This reports:

- Whether Python is initialized
- Which Python interpreter is active
- Whether required packages are available

------------------------------------------------------------------------

## 4. Using DownBallotR in future or concurrent sessions

### New R session?

If you restart R, open a new RStudio window, or start a separate R
process, simply run:

``` r
downballot_use_python()
```

You **do not** need to reinstall Python.

------------------------------------------------------------------------

### Multiple R sessions at the same time

If you have multiple R sessions open concurrently:

- Each session must call
  [`downballot_use_python()`](https://gchickering21.github.io/Downballot/reference/downballot_use_python.md)
  once
- All sessions can safely share the same virtual environment

------------------------------------------------------------------------

## 5. Common issues and fixes

### Python is not initialized or packages appear missing

Run:

``` r
downballot_use_python()
downballot_python_status()
#> 
#>  downballotR Python status
#> --------------------------------------
#> Virtualenv name:         downballotR
#> Virtualenv exists:       TRUE
#> Virtualenv python:       /home/runner/.virtualenvs/downballotR/bin/python
#> reticulate initialized:  TRUE
#> Active python:           /home/runner/.virtualenvs/downballotR/bin/python
#> Python packages:         all required packages correctly installed
#> Playwright Chromium:     correctly installed
#> 
#> - Python environment is ready for use.
```

If packages are still missing:

``` r
downballot_install_python(reinstall = TRUE)
#> Installing Python packages into 'downballotR': pandas, requests, lxml, playwright
#> Using virtual environment 'downballotR' ...
#> + /home/runner/.virtualenvs/downballotR/bin/python -m pip install --upgrade --no-user --ignore-installed pandas requests lxml playwright
#> Ensuring Playwright Chromium is installed (may download ~100-200MB)...
#> Python setup complete for env 'downballotR'.
```

------------------------------------------------------------------------

### “reticulate is already initialized to a different Python interpreter”

This occurs when Python is initialized **before** calling
[`downballot_use_python()`](https://gchickering21.github.io/Downballot/reference/downballot_use_python.md).

Fix:

1.  Restart your R session
2.  Immediately run:

``` r
downballot_use_python()
```

Then continue using the package.

### Reinstall everything from scratch

If the environment becomes corrupted or inconsistent:

``` r
downballot_install_python(reinstall = TRUE)
#> Installing Python packages into 'downballotR': pandas, requests, lxml, playwright
#> Using virtual environment 'downballotR' ...
#> + /home/runner/.virtualenvs/downballotR/bin/python -m pip install --upgrade --no-user --ignore-installed pandas requests lxml playwright
#> Ensuring Playwright Chromium is installed (may download ~100-200MB)...
#> Python setup complete for env 'downballotR'.
downballot_use_python()
downballot_python_status()
#> 
#>  downballotR Python status
#> --------------------------------------
#> Virtualenv name:         downballotR
#> Virtualenv exists:       TRUE
#> Virtualenv python:       /home/runner/.virtualenvs/downballotR/bin/python
#> reticulate initialized:  TRUE
#> Active python:           /home/runner/.virtualenvs/downballotR/bin/python
#> Python packages:         all required packages correctly installed
#> Playwright Chromium:     correctly installed
#> 
#> - Python environment is ready for use.
```

------------------------------------------------------------------------

## 6. Recommended workflow summary

### First-time setup:

``` r
downballot_install_python()
#> Python environment 'downballotR' already exists and is ready.
#> Packages present: pandas, requests, lxml, playwright
#> Playwright Chromium: Correctly installed
#> Nothing to do.
downballot_use_python()
```

### Every new R session:

``` r
downballot_use_python()
```

### Check environment status:

``` r
downballot_python_status()
#> 
#>  downballotR Python status
#> --------------------------------------
#> Virtualenv name:         downballotR
#> Virtualenv exists:       TRUE
#> Virtualenv python:       /home/runner/.virtualenvs/downballotR/bin/python
#> reticulate initialized:  TRUE
#> Active python:           /home/runner/.virtualenvs/downballotR/bin/python
#> Python packages:         all required packages correctly installed
#> Playwright Chromium:     correctly installed
#> 
#> - Python environment is ready for use.
```
