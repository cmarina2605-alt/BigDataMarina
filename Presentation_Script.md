# Presentation Script — Assignment 2

*Read this before presenting. Each section matches one slide. Keep it conversational — don't read word-for-word, just hit the key points.*

---

## Slide 1 — Title

> Hi everyone. So, my project is about immigration and socioeconomic indicators in the Basque Country. This is Assignment 2, so it picks up from where Assignment 1 left off — I already had the data ingested into Supabase, and now the goal was to transform it and actually get some answers out of it.

---

## Slide 2 — Research Questions

> So the whole point of the project is to answer five research questions using real public data. The first one looks at whether foreigners are over-represented in police detentions. The second one is about unemployment — are foreign workers more exposed to it? Then I look at whether immigration growth tracks housing prices, whether it tracks poverty, and finally whether election results shift as demographics change. These are all things people talk about a lot, especially in the Basque Country, but the data behind those conversations isn't usually very accessible. So I wanted to see what the numbers actually say.

---

## Slide 3 — Architecture

> Here's the architecture. It's four components: Supabase as the OLTP layer — that's where the raw data lives, in PostgreSQL. Then BigQuery as the data warehouse — I chose it because it's serverless, has native dbt support, and the free tier is more than enough. Then dbt handles all the SQL transformations in three layers. And finally Plotly generates a static HTML dashboard that anyone can open in a browser, no server needed.
>
> All of these were chosen with reliability, scalability, and maintainability in mind. Everything runs on managed services with automatic backups, BigQuery auto-scales, and dbt keeps the SQL modular and well-documented. The whole thing runs on free-tier resources.
>
> In a real team, you'd have a data engineer handling the ingestion and infrastructure, an analytics engineer writing the dbt models, and a data analyst building the dashboard. I did all three myself, but the architecture separates those concerns cleanly.

---

## Slide 4 — dbt Pipeline

> This is the transformation layer, which is where most of the work happened. I used dbt's standard three-layer pattern. The staging layer has 15 models — each one maps to a source table and just does basic cleanup: renaming columns, casting types, filtering. The intermediate layer has 4 models that handle shared calculations — things like the foreign population percentage, which is needed for four out of five research questions. And then the marts layer has 5 models, one per research question. Those are the final, analysis-ready tables that the dashboard reads from.
>
> In total that's 24 SQL models across the three layers, all documented with YAML schema files and tested with dbt's built-in testing framework.

---

## Slide 5 — RQ1: Crime

> OK so, the first research question: are foreigners over-represented in detentions? And the answer is yes, significantly. Foreign nationals account for about 58% of all detention events in the Basque Country, but they're only about 7% of the population. That's roughly an 8x over-representation ratio.
>
> But — and this is important — there are real caveats here. The data counts detention events, not unique people. So one person detained five times shows up five times. Also, the numerator includes non-resident foreigners like tourists, but the denominator only counts registered residents. So the ratio is probably inflated. That said, the gap is big enough that data quality issues alone don't explain it. Something real is going on, but the data can't tell us whether it's policing practices, socioeconomic vulnerability, or something else.

---

## Slide 6 — RQ2: Unemployment

> This was probably the strongest finding. There's a persistent 9 percentage point gap between foreign and Spanish unemployment rates — about 15% versus 5%. And it's been there consistently over time, though it narrowed a bit during the post-COVID recovery.
>
> The main explanation is sector concentration. If you look at the national EPA data, foreign workers are heavily concentrated in agriculture, construction, and hospitality — sectors that are cyclical and precarious. Spanish workers are spread more evenly, including into the public sector which is much more stable. So it's not really about individual characteristics — it's structural. The sectors where immigrants end up working are just more vulnerable to downturns.

---

## Slide 7 — RQ3: Housing

> For housing, the short answer is: yes, immigration growth and housing prices move together, but I can't say one causes the other. Between 2015 and 2022, both metrics rose across all three Basque provinces. Gipuzkoa has the highest prices and the highest foreign population share; Araba has the lowest of both.
>
> The origin breakdown is interesting — South American immigration dominates everywhere, which makes sense given the linguistic and historical ties with Latin America.
>
> But housing prices are driven by so many factors — limited supply, economic growth, interest rates, domestic migration — that it's really hard to isolate immigration's specific contribution. The correlation is there, but I'd be careful about drawing causal conclusions.

---

## Slide 8 — RQ4: Poverty

> This one had the most interesting pattern. Before 2018, poverty was actually declining while the foreign population share was increasing — so no correlation at all. But from 2018 onwards, both started rising together.
>
> The timing coincides with Spain's uneven recovery from the financial crisis and new migration waves, particularly from Venezuela. But again, the poverty rate measures the entire population, not just immigrants. And many of the new arrivals may themselves be among those counted as at risk of poverty. So it's hard to untangle what's driving what.

---

## Slide 9 — RQ5: Elections

> And the last one — elections. This was the weakest question in terms of what the data can tell us. You can see some clear trends: PNV has been stable forever, EH Bildu emerged in 2012, PP has been declining. But most foreigners can't vote in regional elections, so any demographic effect would have to be indirect — like influencing the issues that parties campaign on, or changing the composition of neighbourhoods.
>
> I can describe these trends happening at the same time, but I honestly can't claim one causes the other. If I were to redo this project, I'd probably replace this question with something the data can actually answer.

---

## Slide 10 — Conclusions

> So wrapping up — the strongest finding is RQ2, the unemployment gap. It's large, persistent, and well-explained by structural factors. RQ1 shows a strong signal but has data quality issues. RQ3 and RQ4 show correlations but with lots of confounding factors. And RQ5 is the weakest — interesting to explore but the data doesn't support strong conclusions.
>
> The overall takeaway is that immigration does correlate with several socioeconomic indicators, but in most cases correlation isn't causation, and the relationships are more complicated than the public debate usually suggests.

---

## Slide 11 — What I'd Improve

> If I had more time, five things I'd do differently. First, better data sources — especially person-level crime data instead of event counts. Second, incremental ETL instead of full-load replication. Third, more dbt tests for data quality. Fourth, actual statistical analysis — regression, not just visual correlation. And fifth, connecting the dashboard directly to BigQuery so it updates in real time instead of reading from static CSVs.

---

## Slide 12 — Thank You

> That's it! The code is all on GitHub if anyone wants to look at it. Happy to take questions.

---

*Tips: Aim for about 8-10 minutes total. Don't rush the caveats on RQ1 — showing you understand the limitations is important. If asked about methodology, point to the dbt schema tests and the three-layer architecture. If asked why you chose these specific questions, say immigration is a hot topic in the Basque Country and you wanted to see what the data actually supports vs. what people assume.*
