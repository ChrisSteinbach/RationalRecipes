# Recipe Queue

Starter queue for the recipe-drops pivot (`docs/design/recipe-drops.md`).

**Ground rules**
- A flat markdown file, not a database. Edit freely.
- Don't promote tooling beyond this until at least ten drops have shipped (per `RationalRecipes-kx9a`).
- "Combined" counts are from `output/catalog/corpus_title_survey.json` (generated 2026-04-10): RecipeNLG + WDC, en+sv filter, min 5 occurrences. They're a frequency hint, not a popularity ranking — the corpus is heavily US-skewed.
- Caveats baked into picks: avoid pure brand items (Cool Whip pies, Toll House cookies); avoid genericized categories ("vegetable casserole"); prefer dishes a competent home baker would actually want central-tendency data on.

## Up next

Initial sequence to test the pivot end-to-end. Sized for hand-cycle iteration.

- [ ] **Chocolate chip cookies** — `RationalRecipes-ehe7` hand-cycle target. Combined 1,414 (RecipeNLG 1,367 + WDC 47). Iconic baseline; well-trodden in both corpora; lots of room to disagree on butter ratio, sugar split, chocolate fraction.
- [ ] **Banana bread** — combined 2,384 (RecipeNLG 2,300 + WDC 84). Largest baking sample in the corpus. Useful follow-up; lots of variation in ripe banana mass, sugar level, leavening.
- [ ] **Peanut butter cookies** — combined 1,660 (RecipeNLG 1,627 + WDC 33). Three-ingredient baseline (PB, sugar, egg) plus everything-else variants — interesting averaging surface.

## Backlog

Sorted by interest, not by frequency. Frequency in parens.

### Cookies & bars

- [ ] Sugar cookies (1,401)
- [ ] Oatmeal cookies (1,121)
- [ ] Brownies (1,302)
- [ ] Snickerdoodles — small frequency but iconic; check survey
- [ ] Shortbread — check survey
- [ ] Gingerbread cookies — check survey

### Breads (quick + yeasted)

- [ ] Zucchini bread (2,435)
- [ ] Pumpkin bread (1,860)
- [ ] Banana nut bread (1,617) — or fold into banana bread
- [ ] Cornbread — check (Mexican cornbread is at 958; the plainer form is likely higher)
- [ ] Sourdough bread — check
- [ ] White bread — check
- [ ] Dinner rolls — check

### Cakes

- [ ] Carrot cake (1,657)
- [ ] Pound cake (1,335)
- [ ] Fresh apple cake (1,316)
- [ ] Apple cake (1,205)
- [ ] Cheesecake — check survey
- [ ] Pumpkin pie (1,009) — sub: spiced pumpkin pie?
- [ ] Pecan pie (2,411) — but careful with the Karo brand bias

### Savory & comfort

- [ ] Chili (1,419) — interesting because it's a mass-balance question with infinite "secret ingredient" variation
- [ ] Lasagna (1,455) — multi-component (RationalRecipes-0x1z relevance)
- [ ] Meat loaf (1,862) — ground beef/breadcrumb/egg ratio is unusually consistent in the corpus
- [ ] Beef stroganoff (1,193)
- [ ] Chicken pot pie (1,453) — multi-component
- [ ] Apple crisp (1,458)
- [ ] Peach cobbler (1,136)

### Less obvious / domain-curated

- [ ] Mac and cheese (homemade) — check survey
- [ ] Pancakes — check survey (corpus has pannkaka/pancake variants)
- [ ] Waffles — check survey
- [ ] Pizza dough — check survey
- [ ] Pie crust — check survey
- [ ] Buttercream frosting — check survey
- [ ] Ganache / chocolate frosting — check survey

### Swedish (en+sv scope)

The Swedish sample sizes are smaller (RecipeNLG is en-only; WDC has some sv). Expect 5–50 source recipes per cluster, not hundreds. May need to combine en+sv terms.

- [ ] Kanelbullar (10) — combine with en "cinnamon buns" / "swedish cinnamon rolls"
- [ ] Köttbullar (7) — combine with "swedish meatballs"
- [ ] Pannkakor / Swedish pancakes — corpus has multiple variants
- [ ] Kladdkaka (5)
- [ ] Smörgåstårta (6)
- [ ] Gravad lax (6)
- [ ] Glögg (8)

## Done

(Empty — the first drop has not shipped yet.)

## Sources

- `output/catalog/corpus_title_survey.json` — 45,882 ranked titles (en+sv, min 5 occurrences, generated 2026-04-10).
- Maintainer's domain knowledge — bakers' interests, recipes with notable measurement disagreements.
- Closed bead context — variants discussed in `RationalRecipes-vwt`, recipes mentioned in `rebuild-catalog.sh` smoke tests (now retired): pancake, pannkaka, punch bowl cake, peanut butter, pie crust, chili, bread and butter pickles, pumpkin bread.
