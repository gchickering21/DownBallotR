# DownBallotR Beta Testing Guide

Thank you for helping test `DownBallotR`! This guide walks you through installation and a set of test calls. We're looking for feedback on anything that's confusing, broken, or could work better, no particular experience level required.

Questions or issues? Email grahamchickering@gmail.com or file a GitHub issue at https://github.com/gchickering21/DownBallotR/issues

---

## What is DownBallotR?

`DownBallotR` is an R package that downloads and standardizes election results from 15 US states. It pulls data live from official state sources and returns clean data frames — no manual downloading required. All data retrieval goes through a single function: `scrape_elections()`. **Currently 15 states are available to pull data from but over future versions we plan to continue to add more to continue to expand the usefullness of this package.**

---

## Step 1: Make sure R is installed

**R version 4.1.0 or higher is required.** Check your version by running:

```r
R.version$version.string
```

If you don't have R (or need to update), download it from https://cran.r-project.org. We also recommend RStudio as a friendlier interface: https://posit.co/download/rstudio-desktop

---

## Step 2: Install the package and set up Python

Follow the installation instructions in the README:
https://github.com/gchickering21/DownBallotR

For a more detailed walkthrough of the Python setup step, see the Python setup vignette:
https://gchickering21.github.io/DownBallotR/articles/python-setup.html

---

## Step 3: Review the documentation

Before running any code, we'd love feedback on whether the documentation is clear and easy to follow. Please take a look at any of the following and note anything confusing, incomplete, or missing:

**README** — the main package overview, installation instructions, and quick-start examples:
https://github.com/gchickering21/DownBallotR

**Vignettes** — detailed guides for each state and topic (available at the links below or by running `vignette("<name>", package = "DownBallotR")` after installing):

- [Python setup](https://gchickering21.github.io/DownBallotR/articles/python-setup.html) — environment installation and troubleshooting
- [Datasheet](https://gchickering21.github.io/DownBallotR/articles/datasheet.html) — data composition, limitations, and responsible use
- [Data dictionary](https://gchickering21.github.io/DownBallotR/articles/data-dictionary.html) — all columns returned across all states
- [Scraping data](https://gchickering21.github.io/DownBallotR/articles/scraping-data.html) — overview of `scrape_elections()` and how routing works
- [ElectionStats states](https://gchickering21.github.io/DownBallotR/articles/election-stats.html) — VA, MA, CO, NH, ID, SC, NM, NY, VT
- [North Carolina](https://gchickering21.github.io/DownBallotR/articles/north-carolina.html)
- [Connecticut](https://gchickering21.github.io/DownBallotR/articles/connecticut.html)
- [Georgia](https://gchickering21.github.io/DownBallotR/articles/georgia.html)
- [Indiana](https://gchickering21.github.io/DownBallotR/articles/indiana.html)
- [Louisiana](https://gchickering21.github.io/DownBallotR/articles/louisiana.html)
- [Utah](https://gchickering21.github.io/DownBallotR/articles/utah.html)

Some questions to keep in mind as you read:
- Is it clear what the package does and what states/years are covered?
- Are the installation steps easy to follow for your level of R experience?
- Is there anything you expected to find in the docs that wasn't there?
- Are the column descriptions in the data dictionary clear and useful?

---

## Step 4: Run the tests

Below are test calls grouped by state. **You don't need to run all of them** — pick a few states you're interested in, or work through them in order. Each call fetches a single recent year to keep things fast but **feel free to adjust the years, input variables, or any other areas of interest** (Note to keep year ranges minimal, separately we are working to create a downloadable dataset so these sites do not need to be constantly scraped). **Do your best to try to break things and report any issues.**

After each call, look at what comes back and note anything surprising, confusing, or broken.

Note: In your R Global Environment you should see different data frames appear related to state, county, and (if available) precinct data.

---

### Virginia (ElectionStats)

```r
va <- scrape_elections(state = "virginia", year_from = 2023, year_to = 2023)
```

---

### Colorado (includes precinct data)

```r
co <- scrape_elections(state = "colorado", year_from = 2022, year_to = 2022)
```

---

### Massachusetts

> **Note:** `level = "state"` returns only candidate-level results. Omit it to get county data as well.

```r
ma <- scrape_elections(state = "massachusetts", year_from = 2022, year_to = 2022, level = "state")
```

---

### New Hampshire

```r
nh <- scrape_elections(state = "new_hampshire", year_from = 2024, year_to = 2024)
```

---

### Idaho (includes precinct data)

```r
id <- scrape_elections(state = "idaho", year_from = 2024, year_to = 2024)
```

---

### Vermont

```r
vt <- scrape_elections(state = "vermont", year_from = 2024, year_to = 2024)
```

---

### New York

> **Note:** New York uses a headless browser to render JavaScript — expect this call to take 1–2 minutes.

```r
ny <- scrape_elections(state = "new_york", year_from = 2024, year_to = 2024)
```

---

### New Mexico (includes precinct data)

> **Note:** New Mexico uses a headless browser to render JavaScript — expect this call to take 1–2 minutes.

```r
nm <- scrape_elections(state = "new_mexico", year_from = 2024, year_to = 2024)
```

---

### South Carolina

> **Note:** South Carolina uses a headless browser to render JavaScript — expect this call to take 1–2 minutes.

```r
sc <- scrape_elections(state = "south_carolina", year_from = 2024, year_to = 2024)
```

---

### North Carolina

```r
nc <- scrape_elections(state = "NC", year_from = 2024, year_to = 2024)
```

---

### Georgia (includes precinct data)

> **Note:** Georgia returns state, county, and precinct data. Precinct scraping navigates each county page and may take several minutes for a full year.

```r
ga <- scrape_elections(state = "GA", year_from = 2022, year_to = 2022)
```

---

### Connecticut

```r
ct <- scrape_elections(state = "CT", year_from = 2024, year_to = 2024)
```

---

### Indiana

```r
ind <- scrape_elections(state = "IN", year_from = 2024, year_to = 2024)
```

---

### Louisiana

```r
la <- scrape_elections(state = "LA", year_from = 2023, year_to = 2023)
```

---

### Utah (includes precinct data)

> **Note:** Utah returns state, county, and precinct data. Precinct scraping navigates each county page and may take several minutes for a full year.

```r
ut <- scrape_elections(state = "UT", year_from = 2024, year_to = 2024)
```

---

## Step 5: Summarizing and auditing results with `summarize_results()`

### What it does

`summarize_results()` takes any data frame already in your environment from `scrape_elections()` and prints a formatted summary of what's in it. It returns a named list with:

| Field | Description |
|---|---|
| `state` | State name, auto-detected from the data or variable name |
| `years` | Vector of election years present |
| `n_years` | Number of distinct years |
| `n_elections` | Number of distinct races/elections |
| `n_candidates` | Number of distinct candidate names |
| `office_level_breakdown` | Count of elections by Federal / State / Local |
| `offices_by_level` | List of unique office names at each level |

### Basic usage

After running any `scrape_elections()` call, pass the resulting data frame (or the state/county/precinct sub-frame) directly to `summarize_results()`:

```r
co <- scrape_elections(state = "colorado", year_from = 2022, year_to = 2022)

summarize_results(co$state)
summarize_results(co$county)
```

For states that return a flat data frame rather than a list (e.g. when `level = "state"` is set):

```r
ma <- scrape_elections(state = "massachusetts", year_from = 2022, year_to = 2022, level = "state")
summarize_results(ma)
```

You can also summarize across multiple years at once — just scrape a wider range and pass the result:

```r
nc <- scrape_elections(state = "NC", year_from = 2020, year_to = 2024)
summarize_results(nc$state)
```

---

### Reviewing the office-level breakdown

The most important thing to check in the summary output is whether offices are being classified correctly as Federal, State, or Local. Here is what to expect for a few states:

**Colorado 2022 (state-level data):**

| Level | Expected offices |
|---|---|
| Federal | U.S. Senate, U.S. Representative (multiple districts) |
| State | Governor, Lieutenant Governor, Attorney General, Secretary of State, State Auditor, State Treasurer, State Board of Education, State Senate, State Representative, Supreme Court |
| Local | District Attorney |

Watch for:
- `State Representative` and `State Senate` (district-numbered) should be **State**, not Local
- `District Attorney` in Colorado is a **Local** office (county-level, not statewide)
- `State Board of Education` and `Regent of the University of Colorado` should be **State**

**North Carolina:**

NC House and NC Senate entries appear in the data as `NC HOUSE (10)`, `NC SENATE (42)`, etc. These should classify as **State**. If you see them falling into Local, that is a bug worth reporting.

**Georgia:**

- `U.S. Senate` and `U.S. Representative` → Federal
- `Governor`, `Lieutenant Governor`, `Attorney General`, `Secretary of State`, `State Senate`, `State House` → State
- `Sheriff`, `County Commissioner`, `Probate Judge`, `School Board` → Local

If you see `Sheriff` or `County Commissioner` showing up as State, or `Governor` as Local, please flag it.

**Things that commonly go wrong:**

- Offices with the word "District" in the name (e.g., `District Court Judge`) can sometimes be misclassified — check that they land in State, not Local
- County-level judiciary (e.g., `Probate Judge`, `Magistrate`) should be Local
- Multi-state data frames may show `"Unknown"` as the state if the `state` column is missing — pass `state = "CO"` explicitly to override

---

### Suggested additional statistics to add

The following would be useful additions to `summarize_results()` or companion functions:

**1. Uncontested race count**
Count races where only one candidate appeared. High uncontested rates in Local offices are normal but worth flagging.

```r
df |>
  group_by(office_level, office, district, election_year) |>
  summarise(n_candidates = n_distinct(candidate), .groups = "drop") |>
  filter(n_candidates == 1) |>
  count(office_level, name = "n_uncontested")
```

**2. Average winner margin by level**
Useful for sanity-checking that vote shares look plausible (e.g., winners with > 100% share would indicate a parsing bug).

```r
df |>
  filter(winner == TRUE) |>
  summarise(avg_vote_pct = mean(vote_pct, na.rm = TRUE), .by = office_level)
```

**3. Offices with suspiciously low total votes**
Local races with very few total votes (e.g., < 10) may indicate a parsing issue rather than a real contest.

```r
df |>
  group_by(office_level, office, district, election_year) |>
  summarise(total_votes = sum(votes, na.rm = TRUE), .groups = "drop") |>
  filter(total_votes < 50) |>
  arrange(total_votes)
```

**4. Year-over-year election count**
Useful when you pull multiple years to check that coverage is consistent.

```r
df |>
  distinct(election_year, office_level, office, district) |>
  count(election_year, office_level, name = "n_races") |>
  arrange(election_year, office_level)
```

**5. Party breakdown**
Check that party labels are being normalized (e.g., no raw strings like `"DEM"` or `"dem"` mixing with `"Democratic"`).

```r
df |>
  count(party, sort = TRUE)
```

---

## What to look for

As you test, please note anything in these categories:

**Installation and setup**
- Did any installation step fail? What was the error message?
- What operating system are you on (Windows / macOS / Linux)?

**Documentation**
- Was anything in the README or vignettes unclear or hard to follow?
- Is there anything you expected to find in the docs that wasn't there?
- Were the column descriptions in the data dictionary useful?

**Errors or crashes**
- Did any `scrape_elections()` call return an error? Please copy the full error message.
- Did any ever completely crash or stop at unexpected points?

**Data quality**
- Do the results look roughly correct for the state and year? (You can spot-check against a quick Google search for that election.)
- Are any columns missing values you'd expect to be there?
- Are there columns present that you don't find useful or that seem redundant?
- Are there additional columns you wish were included (e.g., additional candidate metadata, geographic identifiers, election type flags)?
- Are column names clear, consistent, and useful across the states you tested? Would you rename any?
- For states that return precinct data (CO, ID, NM, NC, GA, UT): do the precinct names look correct and human-readable?
- Do vote percentages and winner flags look correct at each geographic level?

**Usability**
- Is there data you wished the package returned but doesn't?
- Are there built in functions you wish the package had that would make it easier to work with this data?
- Is there a state or time period you tried that didn't work?
- Any other suggestions for improvement?

---

## Sharing your feedback

Please send feedback to Graham Chickering, grahamchickering@gmail.com, or Chris Warshaw, chris.warshaw@georgetown.edu. The most helpful reports include:

1. What you ran (the exact code or a brief description)
2. What happened (error message, unexpected output, or something confusing)
3. Your operating system (Windows, Mac, Linux, etc.) and R version (`R.version$version.string`)

**Note:** Are there any other specific elections or sites you would like to be added to the package? Please feel free to reach out directly if there are any elections your work would benefit from and we can work on adding these to the next version release. 

You're also welcome to file issues directly at https://github.com/gchickering21/DownBallotR/issues

**Thank you!!! — your feedback is extremely valuable and vital at this stage.**
