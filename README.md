# The Voices of Women in ParlaMint — Gendered Patterns in Parliamentary Discourse

**Bersun Şipal, Inés Martínez Fernández, Maria Elena López Justiniano**  
University of the Basque Country (EHU) · Language Analysis and Processing MSc · 2026

---

## Introduction

Parliamentary discourse is one of the most institutionally regulated speech contexts, and one of the few where large-scale, publicly recorded, machine-readable multilingual data is consistently available. This project examines gendered patterns in parliamentary debate using the ParlaMint 2022 corpus, drawing on speeches from four national parliaments: the United Kingdom, Spain, France, and Turkey.

The central question is whether female speakers are systematically associated with specific policy domains, and whether political ideology mediates that relationship. A secondary analysis examines whether women and men employ lexically distinct vocabularies in their speeches. The study builds on prior research suggesting that even as women's parliamentary representation increases, thematic segregation persists — a phenomenon described as "soft exclusion", where women are present in institutions but channeled towards topics traditionally associated with care, welfare, and social rights, while remaining less visible in areas such as defense or foreign affairs (Van der Pol et al., 2021).

---

## Corpus

The analysis uses the 2022 edition of the ParlaMint corpus, a multilingual comparable corpus of parliamentary discourse from 29 European countries, enriched with linguistic annotations and detailed speaker metadata including gender, party name, party orientation, and topic label. Four subcorpora were selected to cover typologically diverse languages and politically varied national contexts.

| Parliament | Files | Speech units | Female speeches | Female (%) |
|---|---|---|---|---|
| United Kingdom | 202 | 59,706 | 15,048 | 32.5% |
| Spain | 61 | 12,269 | 1,775 | 34.1% |
| France | 65 | 20,225 | 2,601 | 32.3% |
| Turkey | 92 | 41,281 | 2,528 | 15.4% |

The proportions of female speeches in the corpus closely mirror the actual share of women elected to each national parliament in 2022, suggesting the corpus is representative of the gender composition of each institution. The Turkish parliament stands out with notably lower female representation (15.4%), consistent with national figures.

---

## Methodology

The preprocessing pipeline merged speech texts and speaker metadata by speech identifier, standardized gender and topic labels, removed annotation errors, and normalized party ideology labels into a common classification scheme (Far-Left, Left, Center-Left, Center, Center-Right, Right, Far-Right). Speeches labeled as *Mix* or *Other* in the topic field were excluded, as they do not represent clearly defined policy domains. The final dataset consisted of 75,887 speech units, with a female-only subset of 21,952 speeches.

Three analyses were conducted. First, topic distributions were compared between female speakers and the full corpus using Pearson's chi-square tests of independence and Cramér's V to measure effect size, with standardized residuals (Z) used to identify domains of over- and underrepresentation at a 95% confidence level (|Z| > 1.96). Second, the interaction between gender and political ideology was examined by running the same statistical tests on the subset of female speakers grouped by party orientation. Third, a keyword analysis was performed on the raw speech texts to identify lexically distinctive terms for female and male speakers in each subcorpus, using language-specific stopword lists and excluding tokens appearing fewer than ten times (p < 0.05).

---

## Results

### Gender and topic distribution

Chi-square tests are statistically significant in all four parliaments (p < 0.001), confirming that topic distribution is associated with speaker gender across all national contexts. Effect sizes (Cramér's V) range from 0.138 in the UK to 0.190 in Turkey — moderate but consistent.

Across all four parliaments, female speakers are consistently overrepresented in Civil Rights and Social Welfare, and show overrepresentation in Health and Education in most contexts. At the same time, women are consistently underrepresented in International Affairs, Government Operations, and topics associated with macroeconomic management and defense. The Turkish parliament shows the clearest example: while Civil Rights accounts for 15.31% of all speeches, it represents 27.57% of speeches delivered by women.

### Political ideology

The interaction between gender and ideology is equally significant (p < 0.001 in all parliaments, Cramér's V between 0.154 and 0.229). The results show that thematic segregation is not uniform across the ideological spectrum. In the United Kingdom, Center-Left women are overrepresented in Social Welfare and Health, while Center-Right women show higher participation in International Affairs, Defense, and Technology. Similar ideological splits appear in Spain, France, and Turkey, suggesting that political affiliation can either reinforce or cut across the patterns of soft exclusion.

### Lexical analysis

The keyword analysis confirms that female and male speakers use lexically distinct vocabularies that align with the thematic differences found in the metadata analysis. Women's most distinctive keywords cluster around gender, social rights, and care: *women*, *children*, *care*, *violence* in the UK; *mujeres*, *igualdad*, *feminista* in Spain; *femmes*, *ivg*, *avortement* in France; *kadin*, *şiddet*, *sözleşmesi* (Istanbul Convention) in Turkey. Men's distinctive keywords, by contrast, point to defense, foreign affairs, and institutional debate across all four subcorpora.

---

## Limitations

The four subcorpora differ considerably in size, which may affect the stability of results, particularly for Spain. The keyword analysis was conducted in four different languages, limiting direct cross-national lexical comparisons. The study covers only the 2022 edition of ParlaMint, and results may not generalize to other legislative periods; the prominence of Health in the French corpus may partly reflect the ongoing shadow of the Covid-19 pandemic in parliamentary debate. Annotation inconsistencies were also detected in the original corpus and addressed during preprocessing, but some irregularities may remain.

---

## Repository Structure

```
parlamint-gender/
├── README.md
├── data/                  # Preprocessed TSV files 
├── notebooks/             # Analysis notebooks
└── figures/               # Topic distribution and keyword plots
```

---

## References

Childs, S. and Krook, M. L. (2009). Analysing women's substantive representation: From critical mass to critical actors. *Government and Opposition*, 44(2):125–145.

Fairclough, N. (1995). *Critical Discourse Analysis: The Critical Study of Language*. Longman.

Inter-Parliamentary Union. (2022). Data on women in national parliaments. Accessed June 2026.

Van der Pol, H. et al. (2021). ParlaMint: Towards comparable parliamentary corpora. In *Proceedings of the CLARIN Annual Conference*.

Wodak, R. (2009). *The Discourse of Politics in Action: Politics as Usual*. Palgrave Macmillan.