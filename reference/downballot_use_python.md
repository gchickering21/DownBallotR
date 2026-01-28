# Use the downballotR Python virtualenv in this R session

Pins reticulate to the package's virtualenv for the current R session.
If reticulate is already initialized to a different interpreter, this
will error with a clear message (because reticulate cannot switch
interpreters mid-session).

## Usage

``` r
downballot_use_python(envname = "downballotR")
```

## Arguments

- envname:

  Name of the virtualenv to use.

## Value

Invisibly TRUE on success.
