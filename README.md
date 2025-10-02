# Case Event Tokens & Bigrams: PySpark ML Model

**Event Analysis for Cases Ranked by Judge and Case Charges**  
_Engineered by Alan Newbold, BSEE_  
[data@e-newbold.com](mailto:data@e-newbold.com)

---

## Overview

This project analyzes court case events using PySpark ML to identify patterns and associations between event tokens/bigrams and case outcomes. The model ranks tokens and bigrams by their predictive lift for favorable outcomes, grouped by Judge and Charge.

---

## How It Works

- **Scope Window:**  
  Limits events to the 180 days before disposition for each case (`WINDOW_DAYS = 180`).

- **Token Extraction:**  
  Builds a normalized text token per event, prioritizing `EventDocuments → Description → other text`. Cleans, uppercases, and trims each token.

- **De-duplication:**  
  For token stats, considers only unique `(Case_Number, token)` pairs—prevents overweighting by repeated events in a case.

- **Outcome Attachment & Grouping:**  
  Joins each `(Case, token)` to case-level outcomes, then groups by `(Judge_Name, Current_Offense_Description, Current_Offense_Statute)` ("Judge × Charge").

- **Baselines & Conditional Rates:**

  - **Baseline (`p_fav`):** Favorable rate for each Judge × Charge group.
  - **Conditional (`p_fav_given_token`):** Favorable rate among cases with the token.
  - **Lift:** `lift = p_fav_given_token / p_fav`

- **Ranking:**  
  Tokens ranked within each group by:

  1. Lift (desc)
  2. Conditional rate (desc)
  3. Favorable cases with token (desc)
  4. Cases with token (desc)

- **Stability Filters:**  
  Enforces minimum cases per group and per token (e.g., `MIN_GROUP_CASES = 10`, `MIN_PATTERN_CASES = 3`).

- **Bigrams:**
  - Sequences events by `EventDate` per `(Case, Judge, Charge)`.
  - Forms bigrams as `"token | next_token"`.
  - De-dups per case and applies same logic/ranking as tokens.

---

## Model Interpretation

### Tokens

- **Indicator:**  
  "Does this case include at least one event matching this token in the 180-day window?"

- **Lift Interpretation:**

  - `> 1.0`: Associated with higher favorable outcomes.
  - `≈ 1.0`: Little/no association.
  - `< 1.0`: Associated with lower favorable outcomes.

- **Support:**
  - Prefer tokens with lift > 1 and decent support.
  - High lift but low support = hypothesis, not conclusion.

### Bigrams

- **Order Matters:**  
  "After event A, the next event is B"—captures procedural sequences.

- **Interpretation:**
  - Favor bigrams with reasonable support.
  - Rare sequences with extreme lift may be spurious.

---

## Guardrails & Pitfalls

- **Multiple Comparisons:**  
  Use support thresholds. Consider:

  - Empirical Bayes smoothing
  - Confidence intervals for lift
  - Fisher’s Exact Test or log-likelihood ratio (G²)

- **Simpson’s Paradox:**  
  Conditioning on Judge × Charge reduces confounding.

- **Data Leakage:**  
  Only pre-disposition events are considered.

- **Token Quality:**  
  Normalize aggressively, merge synonyms, avoid noisy catch-all tokens.

---

## Output Table Structure

Each row (token or bigram) per Judge × Charge group includes:

- `total_cases` (group size)
- `cases_with_pattern` (support)
- `fav_cases_with_pattern` (favorable support)
- `p_fav` (baseline)
- `p_fav_given_pattern` (conditional)
- `lift`
- `rank` (1..K after filters)

---

## Quick Rules of Thumb

- **Strong Leads:**  
  Rank 1–3, lift ≥ 1.25, cases_with_pattern ≥ 8–10
- **Reliable Context:**  
  Lift 1.05–1.20, cases_with_pattern ≥ 20
- **Risk Indicators:**  
  Lift < 1 (potential anti-playbooks)

---

## Suggested Enhancements

- **Smoothing:**  
  Add Beta prior to conditional rates.
- **Uncertainty:**  
  Attach 95% confidence intervals for lift.
- **Alternative Ranks:**  
  Compute PMI for bigrams or log-likelihood (G²).
- **Semantic Normalization:**  
  Cluster near-duplicate tokens to reduce fragmentation.

---

## Using the Outputs

- **Playbook Cards:**  
  Show top tokens/bigrams per Judge × Charge with lift, conditional rate, and support.
- **Triage View:**  
  Color-code by lift (>1 green, <1 red), dim if support < threshold.
- **Drill-through:**  
  Click token/bigram to list triggering cases and event timelines.
- **Vectorization:**  
  Prepare for RAG in Azure AI/OpenAI for attorney guidance (coming soon).

---

_For questions or collaboration, contact [data@e-newbold.com](mailto:data@e-newbold.com)._
