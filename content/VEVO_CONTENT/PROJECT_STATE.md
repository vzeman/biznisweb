# VEVO_CONTENT Project State

Date: 2026-08-20
Project: VEVO_CONTENT
Brand: VEVO
Domain: vevo.sk
Branch: codex/vevo-content-batch-44

## Current State

- On `2026-08-14`, the complete tracked `VEVO_CONTENT` snapshot was moved from the unrelated local-only `opan-claw` history onto a clean branch based on canonical `origin/main`. The canonical remote is `https://github.com/vzeman/biznisweb.git`; future VEVO work must use normal GitHub branches and pull requests.
- The migrated snapshot passed `38/38` regression tests and the complete project check. The live Blog catalog contained `820` records with zero duplicate-title groups and zero invalid slugs before batch 43 preparation.
- VEVO content work was previously stored at the repository root as `vevo-5000-content-plan.md` and `vevo_duplicate_guard.py`.
- Those files now live under `content/VEVO_CONTENT/`.
- Published VEVO batch history is still summarized in the root `PROJECT_STATE.md`; future VEVO-specific state belongs here.
- Published VEVO history is recorded through batch 42; batch-specific IDs and public URLs are authoritative in the sections below and their verification exports.
- Current batch workflow is established: merged live/local duplicate guard, link preflight, wording/depth/HTML guards, repo-local slug-safe MCP hidden-first publication, and independent final public verification.
- BiznisWeb editor workflow guard from batch 29: after filling rich HTML through the editor's HTML/source mode, toggle back out of source mode before saving the post, then verify the public article body. Saving while the source view is still active can leave the public `long` body empty.
- Duplicate guard now has canonical-intent blocking for laundry-symbol head terms (`symboly prania`, `pracie symboly`, `praci stitok`, `vysvetlivky na pranie`) after batch 25 exposed a title-similarity blind spot.
- Batch 30 is published and verified. Keep using the admin/source-mode workflow for clean slug/date preservation; the limited `add_news_post` connector still cannot set clean slug/date fields.
- Laundry perfume category link insertion was completed on `2026-06-29` for 94 existing VEVO posts. URL preservation is now an explicit guard: do not use the one-off `add_laundry_perfume_links_2026_06_29.py --update-live` path for existing public posts; use the guarded admin/import workflow and verify original public URLs after any edit.
- Batch 31 material guides are in partial publication state: satén is live and verified as post `2275`; flanel/manšester remain hidden as posts `2278` and `2279`; duplicate extra posts `2276` and `2277` were deleted.
- Batch 32 robot-vacuum guides are published and verified as post IDs `2280-2290`; their repaired rich HTML and required links passed final live verification.
- Batch 33 floor/kitchen cleaning guides are published and verified: post IDs `2291`, `2292`, and `2293`, dated `2025-09-19`, with clean slugs and public URLs. They were published by editing the existing hidden draft IDs only; no duplicate posts were created.
- Batch 34 bathroom-cleaning guides are published and verified: post IDs `2294`, `2295`, and `2297`, current publish date `2026-07-08` per updated user instruction, with clean slugs and public URLs. Bad hidden test draft `2296` was deleted after `update_news_post` changed its slug to `111`.
- Batch 35 C01-C06 laundry/fragrance guides are published and verified with clean public slugs. The earlier direct remote MCP attempt created post IDs `2298-2307` with broken `/n/111...` URLs; all 10 bad posts were deleted and the final publication was completed through the VEVO admin UI source-mode workflow instead.
- Batch 36 bedding-care guides are published and publicly verified with clean slugs and intact rich HTML.
- Batch 37 overlooked-home-surface guides are published as post IDs `2324-2328` through the repo-local `biznisweb-vevo-content` MCP. All five exact clean URLs and rich HTML passed independent public verification; do not recreate these slugs.
- Batch 38 cleaning-tool and overlooked-area guides are published as post IDs `2329-2333` through the same hidden-first content MCP. All five clean URLs, rich HTML bodies, links, and article-depth checks passed independent public verification.
- Batch 39 test article about textile GSM is published as post ID `2334` with an exact clean slug, `2230` prepared visible words, rich HTML, product/category cards, and independent public verification.
- Batch 40 material decision guides are published as post IDs `2335-2338` through the repo-local hidden-first content MCP. All four clean URLs, long-form rich HTML bodies, links, product/category cards, and responsive article layout passed independent verification.
- Batch 41 material blends and performance guides are published as post IDs `2340-2343` through the repo-local hidden-first content MCP. All four clean URLs, expert long-form bodies, links, cards, and responsive layout passed independent verification.
- Batch 42 textile construction and durability guides are published as post IDs `2345-2348` through the repo-local hidden-first content MCP. All four clean URLs, rich HTML bodies, technical sources, internal links, product/category cards, and responsive article layout passed independent verification.
- Cross-section duplicate audit and remediation on `2026-07-14` inspected all `829` admin records in glossary block `1905`, FAQ block `774`, and Blog block `765`. Exact public title/body duplicates were resolved and `23` semantic-overlap articles were expanded without changing their existing titles or slugs. The final audit has zero public exact-title and zero public exact-body groups.

## Verified

- `content-plan/vevo-5000-content-plan.md` exists under VEVO_CONTENT.
- `tools/vevo_duplicate_guard.py` is VEVO-specific and points to `https://www.vevo.sk`.
- VEVO and ROY now have separate directories, state files, workflow folders, and tool areas.
- Batch 15 was imported and verified on `2026-06-09`: articles `2125-2144`, all public, dated `2025-10-07`, with clean URLs, styled HTML, no malformed hrefs, and no fixed prices in article content.
- Batch 16 was imported and verified on `2026-06-10`: articles `2145-2164`, all public, dated `2025-10-06`, with clean URLs, styled HTML, no malformed hrefs, and no fixed prices in article content.
- Batch 17 was imported and verified on `2026-06-10`: articles `2165-2184`, all public, dated `2025-10-05`, with clean URLs, styled HTML, no malformed hrefs, and no fixed prices in article content.
- Batch 18 was imported and verified on `2026-06-10`: articles `2185-2204`, all public, dated `2025-10-04`, with clean URLs, styled HTML, no malformed hrefs, and no fixed prices in article content.
- Batch 19 was imported and verified on `2026-06-10`: articles `2205-2224`, all public, dated `2025-10-03`, with clean URLs, styled HTML, no malformed hrefs, and no fixed prices in article content.
- Content plan was expanded on `2026-06-10` with a new C09A backlog for 60 broader expert material guides such as `co-je-polyester-vlastnosti-vyhody-a-starostlivost`.
- Batch 20 was imported and verified on `2026-06-10`: articles `2225-2229`, all public, dated `2025-10-02`, with clean URLs, styled HTML, no malformed hrefs, no fixed prices, and expert material sources in article content.
- Batch 21 was imported and verified on `2026-06-10`: articles `2230-2234`, all public, dated `2025-10-01`, with clean URLs, styled HTML, no malformed hrefs, no fixed prices, and expert material sources in article content.
- Batch 22 was created and verified on `2026-06-11`: articles `2235-2239`, all public, dated `2025-09-30`, with clean URLs, styled HTML, no malformed hrefs, no fixed prices, and expert material sources in article content.
- Batch 23 was created and verified on `2026-06-11`: articles `2240-2244`, all public, dated `2025-09-29`, with clean URLs, styled HTML, no malformed hrefs, no fixed prices, and expert material sources in article content.
- Batch 30 was published and verified on `2026-06-29`: articles `2271-2273`, all public, dated `2025-09-22`, with clean URLs, styled HTML, no malformed hrefs, no fixed prices, product/category recommendation blocks, and public wording guard clean.
- Laundry perfume category link insertion was verified on `2026-06-29`: recovery audit `exports/laundry-perfume-link-recovery-2026-06-29.json` reports `record_count=94`, `recovered_count=94`, and `all_ok=true`. All checked original URLs return HTTP 200, contain exactly the intended `parfumy do prania` link target, and have no escaped HTML. The target category `https://www.vevo.sk/c/vevo-fragrance/parfum-do-prania` returns HTTP 200. Post `2272` was manually restored in admin SEO from `maekky` to the original `makky` slug; the wrong `maekky` variant returns HTTP 404 again.
- Batch 31 entered partial publication on `2026-06-29`: local depth guard passed with visible word counts `1511`, `1504`, and `1502`; public wording guard passed; link preflight passed with 32 links and 0 failures. Satén is live at `https://www.vevo.sk/n/co-je-saten-nie-je-to-vzdy-hodvab-a-ako-ho-spravne-prat`; flanel/manšester are hidden drafts pending UI-only date/slug publication; duplicate extra posts `2276` and `2277` were deleted.
- Batch 32 was prepared on `2026-07-05`: 11 robot-vacuum articles, planned for `2025-09-20`, duplicate guard `OK` for all titles against 766 known VEVO records, public wording guard clean, link preflight clean, required robot-vacuum-cleaner category/product links present in every article, and depth guard passed with visible word counts from 1569 to 1722.
- Batch 33 was published and verified on `2026-07-06`: three home-care articles, dated `2025-09-19`, duplicate guard `OK` for all titles against 777 known VEVO RSS records, public wording guard clean, link preflight clean with 29 checked links and 0 failures, depth guard passed with visible word counts `1672`, `1533`, and `1599`, and public verification `exports/batch-33-2026-07-06-publication.json` reports `record_count=3`, `ok_count=3`, and `all_ok=true`.
- Batch 34 was published and verified on `2026-07-08`: three bathroom-cleaning articles, current publish date allowed by the user, duplicate guard `OK` for all titles against 780 known VEVO RSS records, public wording guard clean, link preflight clean with 32 links and 0 failures, depth guard passed with visible word counts `1531`, `1551`, and `1500`, and public verification `exports/batch-34-2026-07-08-publication.json` reports `record_count=3`, `ok_count=3`, and `all_ok=true`.
- Batch 35 was published and verified on `2026-07-08`: ten C01-C06 laundry/fragrance articles, duplicate guard passed against 783 known VEVO records after replacing an exact duplicate candidate, public wording guard clean, link preflight checked 22 links with 0 failures, article-depth guard passed with visible word counts `1660`, `1579`, `1589`, `1545`, `1627`, `1546`, `1544`, `1508`, `1540`, and `1557`, and final public verification reports `record_count=10`, `ok_count=10`, `all_ok=true`.
- Project audit on `2026-07-14` found no code-level blockers after fixes. Seven guard regression tests pass, all 37 scripts capable of mutating live news now have an explicit live opt-in flag, and the latest batch passes duplicate, wording, depth, HTML, slug, and link checks.
- Batch 40 was published and independently verified on `2026-07-16`: four C09A material decision guides, post IDs `2335-2338`, visible word counts `2010-2133`, 19 H2 headings, two tables, nine styled blocks, product/category cards without fixed prices, and zero one-character paragraphs in every prepared article.
- Batch 42 was published and independently verified on `2026-07-22`: four C09A textile construction and durability guides, post IDs `2345-2348`, visible word counts `2460-2578`, 23 H2 headings, two tables, 11 styled blocks, product/category cards without fixed prices, six reader-facing FAQ questions, and zero one-character paragraphs in every prepared article.

## Laundry Perfume Category Link Insert - 2026-06-29

- User request: add a meaningful contextual sentence into at least 50 existing VEVO articles, linking the exact anchor text `parfumy do prania` to `https://www.vevo.sk/c/vevo-fragrance/parfum-do-prania`, without changing article URLs.
- Scope completed: 94 existing posts were updated because the first live run continued past the requested 50 while public verification was failing. The final recovery audit confirms all 94 original mapped URLs are healthy.
- Evidence:
  - Insert report: `exports/laundry-perfume-link-insert-2026-06-29.json`.
  - Recovery/final verification report: `exports/laundry-perfume-link-recovery-2026-06-29.json`.
- Incident and fix: the initial direct `update_news_post` live path with only `long` content caused temporary public 404s for updated posts. A recovery script resent full article payloads and restored 93 URLs. Post `2272` required manual admin SEO correction because BiznisWeb transliterated `mäkký` as `maekky`; the SEO `urlident` was reset to `ako-prat-zupan-aby-zostal-makky-savy-a-nezatuchol-po-sprche`.
- Guard added: `scripts/add_laundry_perfume_links_2026_06_29.py --update-live` now exits unless the explicit escape hatch is passed, because this one-off path is not safe for repeated public existing-post edits.
- Next exact step: continue future VEVO content work only after duplicate/URL checks; for existing articles, never change title/theme/URL unless the user explicitly asks, and verify every original URL after publishing or updating.

## Batch 30 - Published 2026-06-29

- Candidate file: `batches/batch-30-candidates-2026-06-29.txt`.
- Article export: `imports/batch-30-2026-06-29-articles.json`.
- Mapping export: `exports/batch-30-2026-06-29-mapping.json`.
- Verification export: `exports/batch-30-2026-06-29-verification.json`.
- Preflight export: `exports/batch-30-2026-06-29-preflight.json`.
- Blocker export: `exports/batch-30-2026-06-29-blocker.json`.
- Prepared XLS: `C:/Users/Patrik jankech/AppData/Local/Temp/vevo-batch-30-bathroom-bedding-clean-urls.xls`.
- Final duplicate guard result: `OK` for all 3 candidates against 762 existing VEVO records. The first draft was rejected as too similar to existing kitchen-towel, sauna/pool-towel, and bedding-storage topics, then replaced.
- Published IDs and URLs:
  - `2271` `https://www.vevo.sk/n/ako-prat-kupelnovu-predlozku-guma-vlhkost-chlpy-a-zapach-po-sprchovani`
  - `2272` `https://www.vevo.sk/n/ako-prat-zupan-aby-zostal-makky-savy-a-nezatuchol-po-sprche`
  - `2273` `https://www.vevo.sk/n/ako-osviezit-postel-medzi-praniami-vetranie-pyzamo-matrac-a-jemna-vona`
- Local verification: depth guard passed with visible word counts `1527`, `1592`, and `1549`; all article/product/category/source links returned HTTP 200; generated text contains no fixed prices and no public internal workflow terms.
- Public verification: all three public URLs returned HTTP 200 with expected publish times `08:00`, `08:12`, and `08:24`, public article bodies above 10k HTML characters, quick-answer intro, styled blocks, two tables, at least two product/category links, at least five article links, no escaped HTML, no fixed prices, and no internal workflow wording. The accidental `maekky` variant for post `2272` returns HTTP 404.
- Editor guard: post `2272` temporarily broke after admin/source-mode handling left escaped HTML in `long`; the fix was to open the HTML/source textarea, replace it with raw HTML from the batch JSON, toggle back to the visual editor, then save and verify the public body. Do not save VEVO articles while the editor holds escaped `&lt;p&gt;` content.
- Git note: local commits were created, but `git push` is blocked in this checkout because branch `opan-claw` has no configured push destination/upstream.

## Batch 31 - Prepared, Superseded By Partial Publication 2026-06-29

- Candidate file: `batches/batch-31-candidates-2026-06-29.txt`.
- Article export: `imports/batch-31-2026-06-29-articles.json`.
- Batch generator/source: `scripts/build_batch_31_material_articles.py`.
- Content-plan brief: `content-plan/batch-31-material-guides-2026-06-29.md`.
- Duplicate-guard export: `exports/batch-31-2026-06-29-duplicate-guard.json`.
- Public-content guard export: `exports/batch-31-2026-06-29-public-content-guard.json`.
- Link preflight export: `exports/batch-31-2026-06-29-link-preflight.json`.
- Blocker export: `exports/batch-31-2026-06-29-blocker.json`.
- Prepared XLS: `C:/Users/Patrik jankech/AppData/Local/Temp/vevo-batch-31-material-guides-clean-urls.xls`.
- Topics selected after duplicate/material coverage check:
  - `Čo je satén: nie je to vždy hodváb a ako ho správne prať`
  - `Čo je flanel: prečo hreje, ako sa perie a prečo môže žmolkovať`
  - `Čo je manšester: rebrovaná látka, prach v rebrách a správne pranie`
- Planned publish date: all three on `2025-09-21`, before the required `2025-10-12` cutoff.
- Local verification: duplicate guard returned `OK` for all three titles against 765 existing VEVO RSS records; article-depth guard passed with visible word counts `1511`, `1504`, and `1502`; public-content guard found `remaining_hit_count=0`; link preflight checked 32 article/product/category/source links with 0 failures.
- Earlier publishing blocker: the available browser automation could click the BiznisWeb file upload dialog but could not attach a local file to `input[type=file]` (`setInputFiles` was not exposed and setting/typing the file path was blocked by browser security). This note is retained as history, but it is no longer the active next step.
- Superseded status: batch 31 later moved into partial publication. Satén was published and verified as post `2275`; flanel and manšester remain as hidden draft posts `2278` and `2279`; duplicate extra posts `2276` and `2277` were deleted.
- Current next exact step is in the main `Next Exact Step` section: verify the admin list has no duplicate flanel/manšester entries, then finish UI-only publication for hidden posts `2278` and `2279`.

## Batch 15 - Published 2026-06-09

- Candidate file: `batches/batch-15-candidates-2026-06-09.txt`.
- Article export: `imports/batch-15-2026-06-09-articles.json`.
- Mapping export: `exports/batch-15-2026-06-09-mapping.json`.
- Pre-import duplicate guard result: `OK` for all 20 candidates against 619 existing RSS articles.
- Published IDs:
  - `2125` Ako prať softshell bundu a nohavice bez poškodenia membrány
  - `2126` Ako obnoviť impregnáciu softshellu po praní a kedy ju neriešiť
  - `2127` Ako prať pršiplášť a reflexné nepremokavé nohavice po daždi
  - `2128` Ako odstrániť zápach z bežeckých legín po tréningu
  - `2129` Ako prať futbalový dres, štucne a tréningové veci po zápase
  - `2130` Ako prať hokejový dres a textilné vrstvy z výstroja
  - `2131` Ako odstrániť zápach z ponožiek a športovej obuvi po tréningu
  - `2132` Ako prať biele ponožky, aby nezošedli a nezostali tvrdé
  - `2133` Ako zabrániť púšťaniu farby pri praní nového oblečenia
  - `2134` Pustila farba v práčke: čo urobiť s bielym tričkom a ružovou bielizňou
  - `2135` Ako vyprať kari a kurkumu z bavlneného trička bez žltého tieňa
  - `2136` Ako odstrániť škvrny od horčice z trička, obrusu a utierky
  - `2137` Ako odstrániť majonézu a dressing z obrusu bez mastného fľaku
  - `2138` Ako vyprať kakao z pyžama a posteľnej bielizne bez mliečneho zápachu
  - `2139` Ako odstrániť mlieko a jogurt z textilu bez kyslého zápachu
  - `2140` Ako odstrániť živicu z nohavíc, bundy a detského oblečenia
  - `2141` Ako odstrániť žuvačku z nohavíc, mikiny a poťahu
  - `2142` Ako vyčistiť zásobník práčky od usadenín pracieho gélu a aviváže
  - `2143` Ako vyčistiť filter práčky, keď bielizeň zapácha alebo voda odteká pomaly
  - `2144` Ako prať menštruačné nohavičky bezpečne a hygienicky

## Batch 16 - Published 2026-06-10

- Candidate file: `batches/batch-16-candidates-2026-06-10.txt`.
- Article export: `imports/batch-16-2026-06-10-articles.json`.
- Mapping export: `exports/batch-16-2026-06-10-mapping.json`.
- Pre-import duplicate guard result: `OK` for all 20 candidates against 639 existing RSS articles after narrowing two initially risky topics.
- Published IDs:
  - `2145` Ako odstrániť lak na nechty z textilu bez rozmazania škvrny
  - `2146` Ako odstrániť maskaru z uteráka, županu a bielej osušky
  - `2147` Ako odstrániť podkladový krém z goliera blúzky a košele
  - `2148` Ako odstrániť rúž z košele, šálu a látkovej servítky
  - `2149` Ako odstrániť parfumový fľak z oblečenia a jemných látok
  - `2150` Ako prať podprsenku a jemnú spodnú bielizeň bez deformácie
  - `2151` Ako prať kašmírový sveter doma bez zrazenia a žmolkov
  - `2152` Ako prať vlnený sveter, keď zapácha po nosení
  - `2153` Ako prať viskózovú blúzku, aby nestratila tvar a neostala vyťahaná
  - `2154` Ako prať ľanovú košeľu, aby nezostala tvrdá a pokrčená
  - `2155` Ako prať rifľovú bundu a tmavé džínsy, aby nepúšťali farbu
  - `2156` Ako prať sako doma a kedy ho radšej dať do čistiarne
  - `2157` Ako odstrániť pivo z trička, obrusu a sedačky bez zápachu
  - `2158` Ako vyprať čierny čaj z bieleho obrusu bez hnedých máp
  - `2159` Ako odstrániť čerešne z detského trička a letných šiat
  - `2160` Ako vyprať granátové jablko z oblečenia bez ružových máp
  - `2161` Ako odstrániť moč z matraca, plachty a detského pyžama
  - `2162` Ako odstrániť zvratky z koberca, oblečenia a posteľnej bielizne
  - `2163` Ako odstrániť chlpy z oblečenia pri praní, keď máte psa alebo mačku
  - `2164` Ako prať textílie v domácnosti so psom počas pĺznutia

## Batch 17 - Published 2026-06-10

- Candidate file: `batches/batch-17-candidates-2026-06-10.txt`.
- Article export: `imports/batch-17-2026-06-10-articles.json`.
- Mapping export: `exports/batch-17-2026-06-10-mapping.json`.
- Pre-import duplicate guard result: `OK` for all 20 candidates against 659 existing RSS articles after replacing or narrowing five initially risky topics.
- Published IDs:
  - `2165` Ako odstrániť lepidlo z oblečenia po tvorení s deťmi
  - `2166` Ako odstrániť sekundové lepidlo z textilu a kedy to nerobiť doma
  - `2167` Ako odstrániť akrylovú farbu z trička bez zafixovania
  - `2168` Ako odstrániť vodové farby z detskej zástery a rukávov mikiny
  - `2169` Ako odstrániť plastelínu z teplákov, koberca a poťahu
  - `2170` Ako odstrániť sliz z detského trička a deky bez lepkavých zvyškov
  - `2171` Ako vyprať voskovky z peračníka a textilného obalu
  - `2172` Ako odstrániť zvýrazňovač z rukáva mikiny a školského trička
  - `2173` Ako odstrániť červenú papriku z trička a kuchynskej utierky
  - `2174` Ako odstrániť sójovú omáčku z košele, obrusu a prestierania
  - `2175` Ako odstrániť balzamikový ocot z bieleho obrusu
  - `2176` Ako odstrániť olivový olej z ľanovej košele bez mastnej mapy
  - `2177` Ako odstrániť vlasové sérum z uteráka a goliera košele
  - `2178` Ako odstrániť krém na ruky z rukávov svetra a deky
  - `2179` Ako vyprať suchý šampón z čierneho trička a goliera
  - `2180` Ako odstrániť lak na vlasy z goliera košele a šatky
  - `2181` Ako odstrániť trblietky z šiat, saka a kabáta po oslave
  - `2182` Ako odstrániť pach z kostýmu po karnevale bez poškodenia látky
  - `2183` Ako prať oblečenie po kaderníctve od vlasov, farby a lakov
  - `2184` Ako vyprať pracovné tričko po záhradkárčení od hliny a potu

## Batch 18 - Published 2026-06-10

- Candidate file: `batches/batch-18-candidates-2026-06-10.txt`.
- Article export: `imports/batch-18-2026-06-10-articles.json`.
- Mapping export: `exports/batch-18-2026-06-10-mapping.json`.
- Pre-import duplicate guard result: `OK` for all 20 candidates against 679 existing RSS articles after replacing or narrowing six initially risky topics.
- Published IDs:
  - `2185` Ako dostať kúsky papierovej vreckovky z čiernych nohavíc a mikiny
  - `2186` Ako odstrániť biele šmuhy od pracieho prášku z čierneho oblečenia
  - `2187` Ako predísť dierkam v tričkách po praní a sušení
  - `2188` Ako prať oblečenie so zipsami a suchým zipsom bez zatrhnutia
  - `2189` Ako prať oblečenie s flitrami, korálkami a aplikáciami
  - `2190` Ako prať tylovú sukňu, závoj a jemný tyl bez potrhania
  - `2191` Ako prať spoločenské šaty doma a kedy zvoliť čistiareň
  - `2192` Ako odstrániť hrdzu z oblečenia, obrusu a pracovných nohavíc
  - `2193` Ako odstrániť mapy od vody zo sedačky, závesov a čalúnenia
  - `2194` Ako odstrániť sadze z oblečenia po sviečke, grile alebo krbe
  - `2195` Ako odstrániť arašidové maslo z trička, obrusu a detskej mikiny
  - `2196` Ako odstrániť vajíčko z oblečenia, obrusu a kuchynskej utierky
  - `2197` Ako odstrániť vitamínový sirup z detského body a podbradníka
  - `2198` Ako odstrániť jód a dezinfekciu z oblečenia bez zväčšenia fľaku
  - `2199` Ako odstrániť repelent z outdoorovej čiapky a návlekov na ruky
  - `2200` Ako vyprať opaľovací olej z plážovej tuniky a uteráka
  - `2201` Ako vyčistiť bubon práčky po praní pelechu, topánok alebo pracovných vecí
  - `2202` Ako vyčistiť tesnenie práčky po praní pelechu plného chlpov
  - `2203` Ako prať kompresné pančuchy a elastické zdravotné návleky
  - `2204` Ako prať textilné návleky na kočík po prechádzke v daždi

## Batch 19 - Published 2026-06-10

- Candidate file: `batches/batch-19-candidates-2026-06-10.txt`.
- Article export: `imports/batch-19-2026-06-10-articles.json`.
- Mapping export: `exports/batch-19-2026-06-10-mapping.json`.
- Pre-import duplicate guard result: `OK` for all 20 candidates against 699 existing RSS articles after narrowing five initially risky topics.
- Published IDs:
  - `2205` Ako odstrániť soľné mapy z nohavíc a kabáta po zime
  - `2206` Ako vyčistiť rohožku a textílie v predsieni od posypovej soli
  - `2207` Ako odstrániť soľ a mokrý sneh z lyžiarskych rukavíc s membránou
  - `2208` Ako prať kuklu, nákrčník a termo čiapku po lyžovaní
  - `2209` Ako odstrániť vosk na lyže z lyžiarskej bundy a rukavíc
  - `2210` Ako vyčistiť textilné vložky do topánok po zime
  - `2211` Ako vyčistiť cyklistické návleky na tretry po daždi a blate
  - `2212` Ako vyčistiť návlek na autosedačku po zime a posypovej soli
  - `2213` Ako dostať piesok z detských šortiek a trička po pláži pred praním
  - `2214` Ako prať plážové pareo, šatku a ľahkú tuniku po dovolenke
  - `2215` Ako odstrániť zápach z cestovného vankúša po lietadle
  - `2216` Ako prať cestovné oblečenie po dlhom lete alebo vlaku
  - `2217` Ako odstrániť mastnú masť z uteráka, pyžama a trička
  - `2218` Ako odstrániť zinkovú masť z detského body a prebaľovacej podložky
  - `2219` Ako prať poťah na termofor a hrejivý vankúšik bez poškodenia výplne
  - `2220` Ako prať látkové rúška a textilné obaly hygienicky
  - `2221` Ako striasť peľ z bundy a mikiny po prechádzke pred praním
  - `2222` Ako prať oblečenie pri peľovej alergii po príchode zvonka
  - `2223` Ako vyčistiť sušiak na bielizeň, aby neprenášal špinu na prádlo
  - `2224` Ako odstrániť hrdzavé fľaky od štipcov a šnúry na bielizeň

## Batch 20 - Published 2026-06-10

- Candidate file: `batches/batch-20-candidates-2026-06-10.txt`.
- Article export: `imports/batch-20-2026-06-10-articles.json`.
- Mapping export: `exports/batch-20-2026-06-10-mapping.json`.
- Pre-import duplicate guard result: `OK` for all 5 candidates against 719 existing RSS articles.
- Manual duplicate safeguard: the originally planned `Čo je polyester: vlastnosti, výhody, nevýhody a starostlivosť` was not published because VEVO already has `Čo je polyester a ako ho prať, aby nezapáchal`; polyester should be handled later as an expansion of the existing URL.
- Published IDs:
  - `2225` Polyester vs bavlna: rozdiely pri nosení, praní a vôni
  - `2226` Čo je viskóza: vlastnosti, krčivosť, zrážanie a starostlivosť
  - `2227` Čo je elastan: prečo je v legínach, spodnej bielizni a športovom oblečení
  - `2228` Čo je merino vlna: výhody, nevýhody a pranie bez zrazenia
  - `2229` Čo je mikrovlákno: výhody, nevýhody, savosť a pranie

## Existing Article Expansion - Polyester Pillar - Updated 2026-06-10

- Existing post updated instead of creating a duplicate new article:
  - Post ID: `1864`
  - URL: `https://www.vevo.sk/n/co-je-polyester-a-ako-ho-prat-aby-nezapachal`
  - Title kept: `Čo je polyester a ako ho prať, aby nezapáchal`
- Expanded the article into the planned `co-je-polyester-vlastnosti-vyhody-a-starostlivost` style pillar:
  - quick answer,
  - broader `čo je polyester` explanation,
  - properties table,
  - advantages and disadvantages,
  - longtail sections for polyester tričko, bunda, posteľná bielizeň, zápach po praní,
  - polyester vs bavlna/elastan/viskóza comparison,
  - source-backed expert section,
  - VEVO product card and category links without fixed prices.
- Local source-of-truth files:
  - `imports/update-polyester-pillar-2026-06-10.html`
  - `imports/update-polyester-pillar-2026-06-10.meta.json`
  - `imports/update-polyester-pillar-2026-06-10.json`
  - `exports/update-polyester-pillar-2026-06-10-verification.json`
- Verification:
  - Correct URL returns HTTP 200.
  - Post ID `1864` is present on the page.
  - Styled HTML blocks are present.
  - Recommendation/product card is present.
  - All 10 internal product/category/article links return HTTP 200.
  - No fixed prices and no customer-facing `CTA` wording in article content.
  - Corrupted intermediate slug `/n/o-je-polyester-a-ako-ho-pra-aby-nezapchal` returns HTTP 404 after recovery.
- Workflow guard added from this incident:
  - Do not send Slovak rich HTML payloads through PowerShell stdin into `python -`; this can corrupt diacritics before the API call.
  - For future VEVO updates, save article HTML/metadata as UTF-8 files and have the API script read from disk.

## Batch 21 - Published 2026-06-10

- Candidate file: `batches/batch-21-candidates-2026-06-10.txt`.
- Article export: `imports/batch-21-2026-06-10-articles.json`.
- Batch generator/source: `imports/build_batch_21_material_guides.py`.
- Previous blocker export kept for audit: `exports/batch-21-2026-06-10-blocker.json`.
- Mapping export: `exports/batch-21-2026-06-10-mapping.json`.
- Verification export: `exports/batch-21-2026-06-10-verification.json`.
- Published through BiznisWeb admin manual create from prepared JSON after login was restored; the XLS upload path remained unavailable in the browser automation surface.
- News block: `765`.
- Published dates: all 5 articles on `2025-10-01`, between `08:00:00` and `08:48:00`, before the required `2025-10-12` cutoff.
- Final duplicate guard result: `OK` for all 5 candidates against 724 RSS articles.
- Preflight result: 17 internal/external links checked, no fixed product prices, no customer-facing `CTA` wording in article content.
- Published IDs:
  - `2230` Recyklovaný polyester: čo znamená, aké má výhody a ako sa oň starať
  - `2231` Čo je polyamid alebo nylon: vlastnosti, odolnosť a pranie
  - `2232` Polyamid vs polyester: ktorý materiál lepšie znáša pot, šport a časté pranie
  - `2233` Modal v oblečení: čo znamená, prečo je mäkký a ako ho prať
  - `2234` Čo je lyocell alebo Tencel: priedušnosť, jemnosť a starostlivosť
- Verification: public frontend returned HTTP 200 for every clean slug, `datePublished` meta values match `2025-10-01T08:00:00` through `08:48:00`, rich inline-styled HTML blocks are present, product/category cards are present without fixed prices in article content, no customer-facing `CTA` wording in article content, no malformed hrefs, all internal links checked HTTP 200, and all external expert source links checked HTTP 200.
- RSS note: `/rss.xml` closed the connection during the final verification run; post IDs and dates were verified from public article HTML meta tags instead.

## Batch 22 - Published 2026-06-11

- Candidate file: `batches/batch-22-candidates-2026-06-11.txt`.
- Article export: `imports/batch-22-2026-06-11-articles.json`.
- Batch generator/source: `imports/build_batch_22_material_guides.py`.
- Previous blocker export kept for audit: `exports/batch-22-2026-06-11-blocker.json`.
- Mapping export: `exports/batch-22-2026-06-11-mapping.json`.
- Verification export: `exports/batch-22-2026-06-11-verification.json`.
- Published through BiznisWeb admin manual create from prepared JSON after login was restored; the XLS upload path remained unavailable in the browser automation surface.
- News block: `765`.
- Published dates: all 5 articles on `2025-09-30`, between `08:00:00` and `08:48:00`, before the required `2025-10-12` cutoff.
- Duplicate guard result: `OK` for all 5 final candidates against 729 existing RSS articles.
- Preflight result: 17 internal/external links checked, no fixed product prices, no customer-facing `CTA` wording in article content.
- Source-link correction: the original `https://global-standard.org/` source returned HTTP 500 during verification, so the two cotton articles now use `https://textileexchange.org/organic-cotton-certification/` instead.
- Published IDs:
  - `2235` Čo je bavlna: vlastnosti, výhody, nevýhody a starostlivosť
  - `2236` Organická bavlna: čo znamená a či sa perie inak ako bežná bavlna
  - `2237` Čo je ľan: prečo sa krčí, ako ho prať a ako ho zjemniť
  - `2238` Ľan vs bavlna: rozdiely v savosti, krčivosti a starostlivosti
  - `2239` Modal vs lyocell vs viskóza: ako sa líšia pri praní a nosení
- Verification: public frontend returned HTTP 200 for every clean slug, `datePublished` meta values match `2025-09-30T08:00:00` through `08:48:00`, rich inline-styled HTML blocks are present, product/category cards are present without fixed prices in article content, no customer-facing `CTA` wording in article content, no malformed hrefs, all internal links checked HTTP 200, and all external expert source links checked HTTP 200.
- RSS note: IDs and dates were verified from public article HTML meta tags.

## Batch 23 - Published 2026-06-11

- Candidate file: `batches/batch-23-candidates-2026-06-11.txt`.
- Article export: `imports/batch-23-2026-06-11-articles.json`.
- Batch generator/source: `imports/build_batch_23_material_guides.py`.
- Previous blocker export kept for audit: `exports/batch-23-2026-06-11-blocker.json`.
- Mapping export: `exports/batch-23-2026-06-11-mapping.json`.
- Verification export: `exports/batch-23-2026-06-11-verification.json`.
- Published through BiznisWeb admin manual create from prepared JSON after login was restored; the XLS upload path remained unavailable in the browser automation surface.
- News block: `765`.
- Published dates: all 5 articles on `2025-09-29`, between `08:00:00` and `08:48:00`, before the required `2025-10-12` cutoff.
- Duplicate guard result: `OK` for all 5 final candidates against 734 existing RSS articles.
- Preflight result: 21 internal/external links checked, no fixed product prices, no customer-facing `CTA` wording in article content.
- Published IDs:
  - `2240` Co je bambusova viskoza: makkost, marketingove tvrdenia a realna starostlivost
  - `2241` Bambusove vlakno vs bavlna: vyhody, nevyhody a pranie pri citlivej pokozke
  - `2242` Co je akryl: preco pripomina vlnu a ako sa perie
  - `2243` Akryl vs vlna: zmolkovanie, teplo, zapach a starostlivost
  - `2244` Co je zmesovy material: preco sa oblecenie zraza alebo sprava inak nez cakate
- Verification: public frontend returned HTTP 200 for every clean slug, `datePublished` meta values match `2025-09-29T08:00:00` through `08:48:00`, visible dates match the same times, rich inline-styled HTML blocks are present, product/category cards are present without fixed prices in article content, no customer-facing `CTA` wording in article content, no malformed hrefs, and all article links checked HTTP 200.
- RSS note: `/rss.xml` returned HTTP 404 during final verification, so post IDs were read from the BiznisWeb admin edit form and dates were verified from public article HTML meta tags.

## Batch 24 - Published 2026-06-16

- Candidate file: `batches/batch-24-candidates-2026-06-16.txt`.
- Article export: `imports/batch-24-2026-06-16-articles.json`.
- Batch generator/source: `imports/build_batch_24_material_guides.py`.
- Mapping export: `exports/batch-24-2026-06-16-mapping.json`.
- Verification export: `exports/batch-24-2026-06-16-verification.json`.
- Published through BiznisWeb admin manual create from prepared JSON after login was restored; the XLS upload path remained unavailable in the browser automation surface.
- News block: `765`.
- Published dates: all 5 articles on `2025-09-28`, between `08:00:00` and `08:48:00`, before the required `2025-10-12` cutoff.
- Batch topics: fleece, softshell, membrane clothing, clothing pilling, and microplastics from synthetic clothing.
- Duplicate guard result: `OK` for all 5 candidates against 739 existing RSS articles.
- Preflight result: 19 internal/external links checked, no fixed product prices, no customer-facing `CTA` wording in article content, rich inline-styled HTML blocks present in every article, and product/category cards present in every article.
- Published IDs:
  - `2245` Čo je fleece: hrejivosť, žmolkovanie a starostlivosť pri praní
  - `2246` Čo je softshell: vrstvy, membrána, impregnácia a správna starostlivosť
  - `2247` Čo je membránové oblečenie: vodný stĺpec, priedušnosť a pranie bez poškodenia
  - `2248` Prečo sa oblečenie žmolkuje: vlákna, trenie, pranie a sušenie
  - `2249` Mikroplasty z oblečenia: ako prať syntetiku zodpovednejšie bez paniky
- Verification: public frontend returned HTTP 200 for every clean slug, `datePublished` meta values match `2025-09-28T08:00:00` through `08:48:00`, visible dates match the same times, rich inline-styled HTML blocks are present, product/category cards are present without fixed prices in article content, no customer-facing `CTA` wording in article content, no malformed hrefs, and all 19 article links checked HTTP 200.

## Batch 25 - Published 2026-06-16

- Candidate file: `batches/batch-25-candidates-2026-06-16.txt`.
- Article export: `imports/batch-25-2026-06-16-articles.json`.
- Batch generator/source: `imports/build_batch_25_laundry_science.py`.
- Mapping export: `exports/batch-25-2026-06-16-mapping.json`.
- Verification export: `exports/batch-25-2026-06-16-verification.json`.
- Duplicate cleanup export: `exports/batch-25-2026-06-16-duplicate-cleanup.json`.
- Published through BiznisWeb admin manual create from prepared JSON. Dates were corrected in the admin datepicker; rich HTML was then normalized through `biznisweb-update_news_post` because the WYSIWYG save path emptied the `long` field.
- News block: `765`.
- Published dates: all 5 articles on `2025-09-27`, between `08:00:00` and `08:48:00`, before the required `2025-10-12` cutoff.
- Batch topics: reading clothing labels, laundry care symbols, clothing shrinkage, first wash for new clothes, and textile certifications.
- Duplicate guard result: `OK` for all 5 candidates against 744 existing RSS articles.
- Preflight result: 23 internal/external links checked, no fixed product prices, no customer-facing `CTA` wording in article content, rich inline-styled HTML blocks present in every article, and product/category cards present in every article.
- Published IDs:
  - `2250` Ako čítať štítok na oblečení: materiál, symboly prania a správny program
  - `2251` Symboly prania na štítku: čo znamená vanička, trojuholník, kruh, štvorec a žehlička
  - `2252` Prečo sa oblečenie zrazí po praní: teplota, vlákna, sušička a prevencia
  - `2253` Ako prať nové oblečenie prvýkrát: farby, chemický pach, zrážanie a štítok
  - `2254` Certifikáty na textile: OEKO-TEX, GOTS, recyklované vlákna a čo znamenajú pri praní
- Verification: public frontend returned HTTP 200 for every clean URL, `datePublished` meta values match `2025-09-27T08:00:00` through `08:48:00`, rich inline-styled HTML blocks are present, product/category cards are present without fixed prices in article content, no customer-facing `CTA` wording in article content, no malformed hrefs, and all 23 article links checked HTTP 200.
- Post-publish cleanup: `2251` was identified as overlapping with existing laundry-symbol articles, especially `https://www.vevo.sk/n/symboly-prania-kompletny-sprievodca-praciim-stitkom`, and was set to `visible=false`; its public URL now returns HTTP 404. Remaining batch 25 posts `2250`, `2252`, `2253`, and `2254` still return HTTP 200.
- Guard fix: `tools/vevo_duplicate_guard.py` now blocks canonical laundry-symbol intent duplicates even when generic Jaccard title similarity is below threshold.

## Batch 26 - Published 2026-06-16

- Candidate file: `batches/batch-26-candidates-2026-06-16.txt`.
- Article export: `imports/batch-26-2026-06-16-articles.json`.
- Batch generator/source: `imports/build_batch_26_laundry_process.py`.
- Mapping export: `exports/batch-26-2026-06-16-mapping.json`.
- Verification export: `exports/batch-26-2026-06-16-verification.json`.
- Published through BiznisWeb admin manual create from prepared JSON. Dates and clean slugs were set in the admin form; rich HTML was then normalized through `biznisweb-update_news_post` to avoid WYSIWYG content loss.
- News block: `765`.
- Published dates: all 5 articles on `2025-09-26`, between `08:00:00` and `08:48:00`, before the required `2025-10-12` cutoff.
- Batch topics: detergent chemistry and dosing, prewash use, spin speed, overloaded washer, and hard/sticky laundry from residue and rinse problems.
- Duplicate guard result: `OK` for all 5 final candidates against 746 existing RSS articles after replacing hard-water and low-temperature topics that overlapped existing VEVO articles.
- Preflight result: 23 internal/external links checked, no fixed product prices, no customer-facing `CTA` wording in article content, rich inline-styled HTML blocks present in every article, and product/category cards present in every article.
- Published IDs:
  - `2255` Ako funguje praci gel: tenzidy, enzymy, pH a davkovanie pri beznom prani
  - `2256` Predpieranie v pracke: kedy ma zmysel a kedy len mina vodu, cas a praci prostriedok
  - `2257` Otacky pri odstredovani: ako ovplyvnuju vlhkost, krcenie a opotrebovanie oblecenia
  - `2258` Preplnena pracka: preco sa bielizen nevyperie, neoplachne a zapacha
  - `2259` Preco je bielizen po prani tvrda alebo lepkava: zvysky gelu, davkovanie a oplach
- Source-link correction: the first article originally used two Britannica links that returned HTTP 403 to verification; both were replaced in the generator, article JSON, and live post with accessible EPA and PubMed Central sources.
- Verification: public frontend returned HTTP 200 for every clean URL, `datePublished` meta values match `2025-09-26T08:00:00` through `08:48:00`, rich inline-styled HTML blocks are present, product/category cards are present without fixed prices in article content, no customer-facing `CTA` wording in article content, no malformed hrefs, and all 23 article links checked HTTP 200.

## Quality Reset And Batch 27 Fan-Out - Prepared 2026-06-16

- Added a dedicated fan-out plan for the next daily VEVO batch: `content-plan/batch-27-quality-fanout-2026-06-16.md`.
- Fan-out is based on the last published laundry-process articles `2255-2259` and expands them into narrower, non-duplicate intents with detailed sub-queries.
- Added a stronger article standard in `workflows/article-quality-and-sales-blocks.md`: daily batches should be 3 to 5 deeper articles, each with quick answer, diagnostic section, steps, table, mistake section, expert context where useful, FAQ, internal links, one concrete product card, and one category card.
- Added batch 27 quality shortlist: `batches/batch-27-quality-candidates-2026-06-16.txt`.
- Duplicate guard result for the shortlist: `OK` for all 5 candidates against 751 existing RSS articles.
- Updated `workflows/biznisweb-news-import.md` so the required order includes a fan-out brief before duplicate guard and writing.
- No article was published in this step; this is a quality/workflow reset before the next VEVO content batch.

## Batch 26 Quality Retrofit - Published 2026-06-16

- Applied the new quality standard directly to the last published VEVO articles `2255-2259`; titles, slugs, visibility, and `2025-09-26` publication dates were preserved.
- Added per-article fan-out blocks with real sub-query coverage, not generic keyword text.
- Added deeper expert/practical expansions and diagnostic tables to each article.
- Replaced the previous generic recommendation block with problem-matched product and category cards:
  - `2255`: prací gél + `/c/vevo-home-care/pranie/praci-gel`.
  - `2256`: Vevo Shot + `/c/vevo-home-care/pranie/detox-pracky`.
  - `2257`: prírodné vlnené gule do sušičky + `/c/vevo-home-care/pranie/gule-do-susicky`.
  - `2258`: Vevo Shot + `/c/vevo-home-care/pranie/detox-pracky`.
  - `2259`: pravá octová aviváž lesná zmes + `/c/vevo-home-care/pranie/avivaz/octova-avivaz`.
- Local source-of-truth files:
  - `imports/retrofit_batch_26_quality_2026_06_16.py`
  - `imports/batch-26-2026-06-16-quality-update.json`
  - `exports/batch-26-2026-06-16-quality-update-mcp-results.json`
  - `exports/batch-26-2026-06-16-quality-update-verification.json`
- Verification: live public URLs returned HTTP 200 for all 5 updated articles, fan-out sections and expert diagnostic tables are present, product/category cards are present, no internal marketing jargon in article source, no fixed prices in article source, no escaped quote artifacts, no malformed hrefs in article source, and all 29 unique article links returned HTTP 200.

## Batch 27 - Published 2026-06-16

- Candidate file: `batches/batch-27-quality-candidates-2026-06-16.txt`.
- Article export: `imports/batch-27-2026-06-16-articles.json`.
- Batch generator/source: `imports/build_batch_27_laundry_quality.py`.
- Rich HTML update script: `imports/update_batch_27_rich_html.py`.
- Public verification script: `imports/verify_batch_27_public.py`.
- Mapping export: `exports/batch-27-2026-06-16-mapping.json`.
- MCP update export: `exports/batch-27-2026-06-16-mcp-results.json`.
- Verification export: `exports/batch-27-2026-06-16-verification.json`.
- Published through BiznisWeb admin manual create from prepared JSON. Clean slugs were set in the admin form; dates were corrected through the admin datepicker because plain text date input did not update the hidden `date_posted` field. Rich HTML was then normalized through `biznisweb-update_news_post`.
- News block: `765`.
- Published dates: all 5 articles on `2025-09-25`, between `08:00:00` and `08:48:00`, before the required `2025-10-12` cutoff.
- Batch topics: laundry gel dosing by water hardness/load/soil, gel vs powder, extra rinse, short washer programs, and practical washer load capacity.
- Duplicate guard result: `OK` for all 5 candidates against 751 existing RSS articles.
- Published IDs:
  - `2260` Ako davkovat praci gel podla tvrdosti vody, naplne a znecistenia
  - `2261` Praci gel alebo praci prasok: kedy co funguje lepsie a preco
  - `2262` Extra oplach v pracke: kedy pomoze pri zapachu, tvrdej bielizni a citlivej pokozke
  - `2263` Kratky program v pracke: kedy staci a kedy zhorsuje zvysky pracieho prostriedku
  - `2264` Kolko bielizne dat do pracky: prakticka kapacita podla uterakov, obliecok a sportu
- Verification: public frontend returned HTTP 200 for every clean URL, `datePublished` meta values match `2025-09-25T08:00:00` through `08:48:00`, rich inline-styled HTML blocks are present, fan-out sections are present, product/category cards are present without fixed prices in article content, no customer-facing `CTA` wording in article content, no malformed hrefs, and all 23 article links returned HTTP 200.

## Internal Public Wording Cleanup - Published 2026-06-16

- Removed internal SEO/workflow wording from customer-facing VEVO article fields after public text included terms such as `longtail` and "cielene pokryvame".
- Source cleanup affected 63 article records across batches 15, 17, and 21-27, including the batch 26 quality-update export.
- Updated generators/workflows so regenerated article HTML no longer emits internal wording:
  - `imports/build_batch_21_material_guides.py`
  - `imports/build_batch_25_laundry_science.py`
  - `imports/build_batch_26_laundry_process.py`
  - `imports/build_batch_27_laundry_quality.py`
  - `imports/retrofit_batch_26_quality_2026_06_16.py`
  - `workflows/article-quality-and-sales-blocks.md`
  - `workflows/biznisweb-news-import.md`
- Added `tools/vevo_public_content_guard.py` and wired it into `scripts/check.ps1`; it scans public article fields for internal wording before publication.
- Live update: 62 reachable mapped posts were updated through BiznisWeb `update_news_post`; 5 posts initially hit the BiznisWeb rate limit and were retried successfully with slower calls.
- One cleanup candidate, the duplicate laundry-symbol post `2251`, returned an invalid BiznisWeb reference and its public URL is already HTTP 404, so there is no reachable public article text to update.
- Cleanup/export evidence:
  - `exports/internal-public-terms-cleanup-2026-06-16-source.json`
  - `exports/internal-public-terms-cleanup-2026-06-16-mcp-results.json`
  - `exports/internal-public-terms-cleanup-2026-06-16-mcp-retry-results.json`
  - `exports/internal-public-terms-cleanup-2026-06-16-live-verification.json`
- Verification:
  - Local guard: `remaining_hit_count=0`.
  - `scripts/check.ps1`: passed.
  - Live verification scanned 140 known mapped VEVO article URLs; 63 were cleanup candidates.
  - `all_public_text_ok=true`: no forbidden internal wording remained in reachable public article text.
  - Two older BiznisWeb URL variants were resolved through sitemap fallback (`maekkost` / `maekky`) and verified clean.
  - One historical duplicate mapping remains unreachable: `symboly-prania-na-stitku-co-znamena-vanicka-trojuholnik-kruh-stvorec-a-zehlicka` returns HTTP 404 and has no public article text to clean.

## Longer Article Standard - Set 2026-06-16

- New VEVO content batches should be smaller and longer: default 2 to 3 articles per batch, with 5 articles only when topics are very close and quality can stay high.
- Standard expert/practical articles should target at least 1500 visible words; pillar articles should target 2200+ visible words.
- Added `tools/vevo_article_depth_guard.py` for new batch JSON files. It checks visible word count, section count, table count, styled blocks, product/category links, and FAQ depth.
- Updated `workflows/article-quality-and-sales-blocks.md` and `workflows/biznisweb-news-import.md` so future batches must use the longer-article standard before publication.

## Conservative Article Retrofit Wave 01 - Published 2026-06-16

- Added the conservative retrofit workflow: `workflows/conservative-article-retrofit.md`.
- Added retrofit inventory and live verification tooling:
  - `tools/vevo_retrofit_inventory.py`
  - `tools/vevo_retrofit_live_verify.py`
- Generated the current retrofit priority plan:
  - `content-plan/retrofit-priority-2026-06-16.md`
  - `exports/retrofit-inventory-2026-06-16.json`
- Current inventory after wave 01: 140 known VEVO article sources; `major_expand=126`, `medium_expand=11`, `watch=3`.
- Conservatively expanded and republished three washer-care articles while preserving titles, slugs, visibility, and existing core content:
  - `2142` https://www.vevo.sk/n/ako-vycistit-zasobnik-pracky-od-usadenin-pracieho-gelu-a-avivaze
  - `2202` https://www.vevo.sk/n/ako-vycistit-tesnenie-pracky-po-prani-pelechu-plneho-chlpov
  - `2201` https://www.vevo.sk/n/ako-vycistit-bubon-pracky-po-prani-pelechu-topanok-alebo-pracovnych-veci
- Source and evidence files:
  - `imports/retrofit_wave_01_washer_care_2026_06_16.py`
  - `exports/retrofit-wave-01-washer-care-2026-06-16.json`
  - `exports/retrofit-wave-01-washer-care-2026-06-16-mcp-results.json`
  - `exports/retrofit-wave-01-washer-care-2026-06-16-verification.json`
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1625, 1534, and 1519.
  - `tools/vevo_retrofit_live_verify.py`: passed; 3 public article URLs returned HTTP 200 and all 13 article links returned HTTP 200.
  - Product and category blocks are present without fixed prices; no public `CTA`, `longtail`, or internal workflow wording remains in these three articles.

## Conservative Article Retrofit Wave 02 - Published 2026-06-16

- Strengthened the retrofit workflow with the hard rule that existing article topic, public title, slug, and URL must never be changed during retrofit; retrofit is additive expansion only.
- Conservatively expanded and republished three existing core laundry articles while preserving titles, slugs, URLs, visibility, and short descriptions:
  - `2143` https://www.vevo.sk/n/ako-vycistit-filter-pracky-ked-bielizen-zapacha-alebo-voda-odteka-pomaly
  - `2134` https://www.vevo.sk/n/pustila-farba-v-pracke-co-urobit-s-bielym-trickom-a-ruzovou-bieliznou
  - `2250` https://www.vevo.sk/n/ako-citat-stitok-na-obleceni-material-symboly-prania-a-spravny-program
- Source and evidence files:
  - `imports/retrofit_wave_02_core_laundry_2026_06_16.py`
  - `exports/retrofit-wave-02-core-laundry-2026-06-16.json`
  - `exports/retrofit-wave-02-core-laundry-2026-06-16-mcp-results.json`
  - `exports/retrofit-wave-02-core-laundry-2026-06-16-verification.json`
- Current inventory after wave 02: 140 known VEVO article sources; `major_expand=123`, `medium_expand=11`, `watch=6`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Local link check before publish: 15 internal/product/category links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1528, 1513, and 1618.
  - `tools/vevo_retrofit_live_verify.py`: passed; 3 public article URLs returned HTTP 200 and all 15 article links returned HTTP 200.
  - Product and category blocks are present without fixed prices; no public `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was added.

## Conservative Article Retrofit Wave 03 - Published 2026-06-16

- Conservatively expanded and republished three existing laundry-program articles while preserving titles, slugs, URLs, visibility, and short descriptions:
  - `2263` https://www.vevo.sk/n/kratky-program-v-pracke-kedy-staci-a-kedy-zhorsuje-zvysky-pracieho-prostriedku
  - `2264` https://www.vevo.sk/n/kolko-bielizne-dat-do-pracky-prakticka-kapacita-podla-uterakov-obliecok-a-sportu
  - `2262` https://www.vevo.sk/n/extra-oplach-v-pracke-kedy-pomoze-pri-zapachu-tvrdej-bielizni-a-citlivej-pokozke
- Source and evidence files:
  - `imports/retrofit_wave_03_laundry_programs_2026_06_16.py`
  - `exports/retrofit-wave-03-laundry-programs-2026-06-16.json`
  - `exports/retrofit-wave-03-laundry-programs-2026-06-16-mcp-results.json`
  - `exports/retrofit-wave-03-laundry-programs-2026-06-16-verification.json`
- Current inventory after wave 03: 140 known VEVO article sources; `major_expand=123`, `medium_expand=8`, `watch=9`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Local link check before publish: 13 internal/product/category links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1517, 1534, and 1517.
  - `tools/vevo_retrofit_live_verify.py`: passed; 3 public article URLs returned HTTP 200 and all 13 article links returned HTTP 200.
  - Article-specific live markers were verified for all three additions.
  - Product and category blocks remain present without fixed prices; no public `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was added.

## Conservative Article Retrofit Wave 04 - Published 2026-06-16

- Conservatively expanded and republished three existing laundry-decision articles while preserving titles, slugs, URLs, visibility, and short descriptions:
  - `2261` https://www.vevo.sk/n/praci-gel-alebo-praci-prasok-kedy-co-funguje-lepsie-a-preco
  - `2256` https://www.vevo.sk/n/predpieranie-v-pracke-kedy-ma-zmysel-a-kedy-len-mina-vodu-cas-a-praci-prostriedok
  - `2257` https://www.vevo.sk/n/otacky-pri-odstredovani-ako-ovplyvnuju-vlhkost-krcenie-a-opotrebovanie-oblecenia
- Source and evidence files:
  - `imports/retrofit_wave_04_laundry_decisions_2026_06_16.py`
  - `exports/retrofit-wave-04-laundry-decisions-2026-06-16.json`
  - `exports/retrofit-wave-04-laundry-decisions-2026-06-16-mcp-results.json`
  - `exports/retrofit-wave-04-laundry-decisions-2026-06-16-verification.json`
- Current inventory after wave 04: 140 known VEVO article sources; `major_expand=123`, `medium_expand=5`, and `watch=12`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Local link check before final publish: 15 internal/product/category links returned HTTP 200.
  - Local depth guard passed; visible word counts are 1583, 1529, and 1565.
  - `tools/vevo_retrofit_live_verify.py`: passed; 3 public article URLs returned HTTP 200 and all 15 article links returned HTTP 200.
  - Article-specific live markers were verified for all three final additions.
  - Product and category blocks remain present without fixed prices; no public `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was added.

## Conservative Article Retrofit Wave 05 - Published 2026-06-16

- Conservatively expanded and republished three existing laundry residue/load articles while preserving titles, slugs, URLs, visibility, and short descriptions:
  - `2259` https://www.vevo.sk/n/preco-je-bielizen-po-prani-tvrda-alebo-lepkava-zvysky-gelu-davkovanie-a-oplach
  - `2260` https://www.vevo.sk/n/ako-davkovat-praci-gel-podla-tvrdosti-vody-naplne-a-znecistenia
  - `2258` https://www.vevo.sk/n/preplnena-pracka-preco-sa-bielizen-nevyperie-neoplachne-a-zapacha
- Source and evidence files:
  - `imports/retrofit_wave_05_laundry_residue_load_2026_06_16.py`
  - `exports/retrofit-wave-05-laundry-residue-load-2026-06-16.json`
  - `exports/retrofit-wave-05-laundry-residue-load-2026-06-16-mcp-results.json`
  - `exports/retrofit-wave-05-laundry-residue-load-2026-06-16-verification.json`
- Current inventory after wave 05: 140 known VEVO article sources; `major_expand=123`, `medium_expand=2`, and `watch=15`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Local link check before publish: 15 internal/product/category links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1557, 1536, and 1521.
  - `tools/vevo_retrofit_live_verify.py`: passed; 3 public article URLs returned HTTP 200 and all 15 article links returned HTTP 200.
  - Article-specific live markers were verified for all three additions.
  - Product and category blocks remain present without fixed prices; no public `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was added.

## Conservative Article Retrofit Wave 06 - Published 2026-06-16

- Conservatively expanded and republished five existing mixed laundry/material/stain articles while preserving titles, slugs, URLs, visibility, and short descriptions:
  - `2255` https://www.vevo.sk/n/ako-funguje-praci-gel-tenzidy-enzymy-ph-a-davkovanie-pri-beznom-prani
  - `2225` https://www.vevo.sk/n/polyester-vs-bavlna-rozdiely-pri-noseni-prani-a-voni
  - `2157` https://www.vevo.sk/n/ako-odstranit-pivo-z-tricka-obrusu-a-sedacky-bez-zapachu
  - `2152` https://www.vevo.sk/n/ako-prat-vlneny-sveter-ked-zapacha-po-noseni
  - `2149` https://www.vevo.sk/n/ako-odstranit-parfumovy-flak-z-oblecenia-a-jemnych-latok
- Source and evidence files:
  - `imports/retrofit_wave_06_mixed_five_2026_06_16.py`
  - `exports/retrofit-wave-06-mixed-five-2026-06-16.json`
  - `exports/retrofit-wave-06-mixed-five-2026-06-16-mcp-results.json`
  - `exports/retrofit-wave-06-mixed-five-2026-06-16-verification.json`
- Current inventory after wave 06: 140 known VEVO article sources; `watch=20` and `major_expand=120`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Duplicate guard returned exact slug/title matches only because this wave intentionally expanded existing articles; no new duplicate article was created.
  - Local link check before publish: 20 internal/product/category links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1577, 1560, 1532, 1515, and 1523.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 20 article links returned HTTP 200.
  - Article-specific live marker fragments were verified for all five additions.
  - Product and category blocks remain present without fixed prices; no public `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was added.

## Conservative Article Retrofit Wave 07 - Published 2026-06-16

- Conservatively expanded and republished five existing odor/fragrance articles while preserving titles, slugs, URLs, visibility, and short descriptions:
  - `2215` https://www.vevo.sk/n/ako-odstranit-zapach-z-cestovneho-vankusa-po-lietadle
  - `2139` https://www.vevo.sk/n/ako-odstranit-mlieko-a-jogurt-z-textilu-bez-kysleho-zapachu
  - `2138` https://www.vevo.sk/n/ako-vyprat-kakao-z-pyzama-a-postelnej-bielizne-bez-mliecneho-zapachu
  - `2131` https://www.vevo.sk/n/ako-odstranit-zapach-z-ponoziek-a-sportovej-obuvi-po-treningu
  - `2128` https://www.vevo.sk/n/ako-odstranit-zapach-z-bezeckych-legin-po-treningu
- Source and evidence files:
  - `imports/retrofit_wave_07_odor_five_2026_06_16.py`
  - `exports/retrofit-wave-07-odor-five-2026-06-16.json`
  - `exports/retrofit-wave-07-odor-five-2026-06-16-mcp-results.json`
  - `exports/retrofit-wave-07-odor-five-2026-06-16-verification.json`
- Current inventory after wave 07: 140 known VEVO article sources; `watch=25` and `major_expand=115`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Duplicate guard returned expected exact slug/title matches for existing retrofit targets only; no new duplicate article was created.
  - Local link check before publish: 21 internal/product/category links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1523, 1531, 1520, 1514, and 1506.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 21 article links returned HTTP 200.
  - Article-specific live marker fragments were verified for all five additions.
  - Product and category blocks remain present without fixed prices; no public `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was added.

## Conservative Article Retrofit Wave 08 - Published 2026-06-16

- Conservatively expanded and republished five existing bedding/material/stain articles while preserving titles, slugs, URLs, visibility, and short descriptions:
  - `2243` https://www.vevo.sk/n/akryl-vs-vlna-zmolkovanie-teplo-zapach-a-starostlivost
  - `2162` https://www.vevo.sk/n/ako-odstranit-zvratky-z-koberca-oblecenia-a-postelnej-bielizne
  - `2200` https://www.vevo.sk/n/ako-vyprat-opalovaci-olej-z-plazovej-tuniky-a-uteraka
  - `2161` https://www.vevo.sk/n/ako-odstranit-moc-z-matraca-plachty-a-detskeho-pyzama
  - `2177` https://www.vevo.sk/n/ako-odstranit-vlasove-serum-z-uteraka-a-goliera-kosele
- Source and evidence files:
  - `imports/retrofit_wave_08_bedding_stains_five_2026_06_16.py`
  - `exports/retrofit-wave-08-bedding-stains-five-2026-06-16.json`
  - `exports/retrofit-wave-08-bedding-stains-five-2026-06-16-mcp-results.json`
  - `exports/retrofit-wave-08-bedding-stains-five-2026-06-16-verification.json`
- Current inventory after wave 08: 140 known VEVO article sources; `watch=30` and `major_expand=110`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Duplicate guard returned expected exact slug/title matches for existing retrofit targets only; no new duplicate article was created.
  - Local link check before publish: 22 internal/product/category links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1594, 1521, 1515, 1509, and 1522.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 22 article links returned HTTP 200.
  - Article-specific live marker fragments were verified for all five additions.
  - Product and category blocks remain present without fixed prices; no public `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was added.

## Conservative Article Retrofit Wave 09 - Published 2026-06-16

- Conservatively expanded and republished five existing material/cosmetic/stain articles while preserving titles, slugs, URLs, visibility, and short descriptions:
  - `2146` https://www.vevo.sk/n/ako-odstranit-maskaru-z-uteraka-zupanu-a-bielej-osusky
  - `2217` https://www.vevo.sk/n/ako-odstranit-mastnu-mast-z-uteraka-pyzama-a-tricka
  - `2153` https://www.vevo.sk/n/ako-prat-viskozovu-bluzku-aby-nestratila-tvar-a-neostala-vytahana
  - `2167` https://www.vevo.sk/n/ako-odstranit-akrylovu-farbu-z-tricka-bez-zafixovania
  - `2126` https://www.vevo.sk/n/ako-obnovit-impregnaciu-softshellu-po-prani-a-kedy-ju-neriesit
- Source and evidence files:
  - `imports/retrofit_wave_09_material_cosmetic_five_2026_06_16.py`
  - `exports/retrofit-wave-09-material-cosmetic-five-2026-06-16.json`
  - `exports/retrofit-wave-09-material-cosmetic-five-2026-06-16-mcp-results.json`
  - `exports/retrofit-wave-09-material-cosmetic-five-2026-06-16-verification.json`
- Current inventory after wave 09: 140 known VEVO article sources; `watch=35` and `major_expand=105`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Duplicate guard returned expected exact slug/title matches for existing retrofit targets only; no new duplicate article was created.
  - Local link check before publish: 23 internal/product/category links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1539, 1530, 1570, 1543, and 1525.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 23 article links returned HTTP 200.
  - Article-specific live marker fragments were verified for all five additions.
  - Product and category blocks remain present without fixed prices; no public `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was added.

## Conservative Article Retrofit Wave 10 - Published 2026-06-16

- Conservatively expanded and republished five existing functional/material articles while preserving titles, public URLs, visibility, and short descriptions:
  - `2125` https://www.vevo.sk/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany
  - `2234` https://www.vevo.sk/n/co-je-lyocell-alebo-tencel-priedusnost-jemnost-a-starostlivost
  - `2231` https://www.vevo.sk/n/co-je-polyamid-alebo-nylon-vlastnosti-odolnost-a-pranie
  - `2233` https://www.vevo.sk/n/modal-v-obleceni-co-znamena-preco-je-maekky-a-ako-ho-prat
  - `2232` https://www.vevo.sk/n/polyamid-vs-polyester-ktory-material-lepsie-znasa-pot-sport-a-caste-pranie
- Source and evidence files:
  - `imports/retrofit_wave_10_functional_materials_five_2026_06_16.py`
  - `exports/retrofit-wave-10-functional-materials-five-2026-06-16.json`
  - `exports/retrofit-wave-10-functional-materials-five-2026-06-16-mcp-results.json`
  - `exports/retrofit-wave-10-functional-materials-five-2026-06-16-verification.json`
  - `exports/modal-link-repair-2026-06-16.json`
- Modal URL correction:
  - The existing public modal article URL is `modal-v-obleceni-co-znamena-preco-je-maekky-a-ako-ho-prat`; the older local planning slug with `makky` returned HTTP 404.
  - Wave 10 corrected local source-of-truth references to the live URL and repaired the related batch 22 live internal link in post `2239` (`Modal vs lyocell vs viskóza...`) without changing the post topic.
- Current inventory after wave 10: 140 known VEVO article sources; `watch=40` and `major_expand=100`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Duplicate guard returned expected existing-article matches only; no new duplicate article was created.
  - Local link check before publish: 23 internal/product/category links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1522, 1508, 1501, 1519, and 1520.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 23 article links returned HTTP 200.
  - Article-specific live marker fragments were verified for all five additions.
  - Additional live repair verification confirmed post `2239` no longer contains the old `makky` href and the corrected `maekky` href returns HTTP 200.
  - Product and category blocks remain present without fixed prices; no public `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was added.

## Conservative Article Retrofit Wave 11 - Published 2026-06-16

- Conservatively expanded and republished five existing natural/material articles while preserving titles, public URLs, visibility, and short descriptions:
  - `2238` https://www.vevo.sk/n/lan-vs-bavlna-rozdiely-v-savosti-krcivosti-a-starostlivosti
  - `2242` https://www.vevo.sk/n/co-je-akryl-preco-pripomina-vlnu-a-ako-sa-perie
  - `2239` https://www.vevo.sk/n/modal-vs-lyocell-vs-viskoza-ako-sa-lisia-pri-prani-a-noseni
  - `2236` https://www.vevo.sk/n/organicka-bavlna-co-znamena-a-ci-sa-perie-inak-ako-bezna-bavlna
  - `2235` https://www.vevo.sk/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost
- Source and evidence files:
  - `imports/retrofit_wave_11_natural_materials_five_2026_06_16.py`
  - `exports/retrofit-wave-11-natural-materials-five-2026-06-16.json`
  - `exports/retrofit-wave-11-natural-materials-five-2026-06-16-mcp-results.json`
  - `exports/retrofit-wave-11-natural-materials-five-2026-06-16-verification.json`
- Current inventory after wave 11: 140 known VEVO article sources; `watch=45` and `major_expand=95`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Duplicate guard returned expected existing-article matches only; no new duplicate article was created.
  - Local link check before publish: 22 internal/product/category links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1666, 1573, 1585, 1529, and 1532.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 22 article links returned HTTP 200.
  - Article-specific live marker fragments were verified for all five additions.
  - Product and category blocks remain present without fixed prices; no public `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was added.

## Conservative Article Retrofit Wave 12 - Published 2026-06-16

- Conservatively expanded and republished five existing material/outdoor articles while preserving titles, public URLs, visibility, and short descriptions:
  - `2241` https://www.vevo.sk/n/bambusove-vlakno-vs-bavlna-vyhody-nevyhody-a-pranie-pri-citlivej-pokozke
  - `2244` https://www.vevo.sk/n/co-je-zmesovy-material-preco-sa-oblecenie-zraza-alebo-sprava-inak-nez-cakate
  - `2230` https://www.vevo.sk/n/recyklovany-polyester-co-znamena-ake-ma-vyhody-a-ako-sa-on-starat
  - `2246` https://www.vevo.sk/n/co-je-softshell-vrstvy-membrana-impregnacia-a-spravna-starostlivost
  - `2245` https://www.vevo.sk/n/co-je-fleece-hrejivost-zmolkovanie-a-starostlivost-pri-prani
- Source and evidence files:
  - `imports/retrofit_wave_12_materials_outdoor_five_2026_06_16.py`
  - `exports/retrofit-wave-12-materials-outdoor-five-2026-06-16.json`
  - `exports/retrofit-wave-12-materials-outdoor-five-2026-06-16-mcp-results.json`
  - `exports/retrofit-wave-12-materials-outdoor-five-2026-06-16-verification.json`
- Bambusová viskóza URL correction:
  - The existing public URL for post `2240` uses `co-je-bambusova-viskoza-maekkost-marketingove-tvrdenia-a-realna-starostlivost`; the older local `makkost` slug returned HTTP 404.
  - Wave 12 corrected the source article slug in `batch-23-2026-06-11-articles.json` and the batch 23 mapping/verification records, and used the live `maekkost` URL in new internal links.
- Current inventory after wave 12: 140 known VEVO article sources; `watch=50` and `major_expand=90`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Duplicate guard returned expected existing-article matches only; no new duplicate article was created.
  - Local link check before publish: 16 internal/product/category links returned HTTP 200 after repairing the bambusová viskóza URL.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1639, 1552, 1510, 1529, and 1533.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 16 article links returned HTTP 200.
  - Article-specific live marker fragments were verified for all five additions.
  - Product and category blocks remain present without fixed prices; no public `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was added.

## Conservative Article Retrofit Wave 13 - Published 2026-06-16

- Conservatively expanded and republished five existing material/stain articles while preserving titles, public URLs, visibility, and short descriptions:
  - `2240` https://www.vevo.sk/n/co-je-bambusova-viskoza-maekkost-marketingove-tvrdenia-a-realna-starostlivost
  - `2228` https://www.vevo.sk/n/co-je-merino-vlna-vyhody-nevyhody-a-pranie-bez-zrazenia
  - `2227` https://www.vevo.sk/n/co-je-elastan-preco-je-v-leginach-spodnej-bielizni-a-sportovom-obleceni
  - `2226` https://www.vevo.sk/n/co-je-viskoza-vlastnosti-krcivost-zrazanie-a-starostlivost
  - `2195` https://www.vevo.sk/n/ako-odstranit-arasidove-maslo-z-tricka-obrusu-a-detskej-mikiny
- Source and evidence files:
  - `imports/retrofit_wave_13_materials_stain_five_2026_06_16.py`
  - `exports/retrofit-wave-13-materials-stain-five-2026-06-16.json`
  - `exports/retrofit-wave-13-materials-stain-five-2026-06-16-mcp-results.json`
  - `exports/retrofit-wave-13-materials-stain-five-2026-06-16-verification.json`
- Current inventory after wave 13: 140 known VEVO article sources; `watch=55` and `major_expand=85`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Duplicate guard returned expected existing-article matches only; no new duplicate article was created.
  - Local link check before publish: 24 internal/product/category links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1591, 1573, 1547, 1571, and 1504.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 24 article links returned HTTP 200.
  - Article-specific live marker fragments were verified for all five additions.
  - Product and category blocks remain present without fixed prices; no public `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was added.

## Conservative Article Retrofit Wave 14 - Published 2026-06-16

- Conservatively expanded and republished five existing kids/school stain articles while preserving titles, public URLs, visibility, publication dates, and short descriptions:
  - `2159` https://www.vevo.sk/n/ako-odstranit-ceresne-z-detskeho-tricka-a-letnych-siat
  - `2197` https://www.vevo.sk/n/ako-odstranit-vitaminovy-sirup-z-detskeho-body-a-podbradnika
  - `2170` https://www.vevo.sk/n/ako-odstranit-sliz-z-detskeho-tricka-a-deky-bez-lepkavych-zvyskov
  - `2172` https://www.vevo.sk/n/ako-odstranit-zvyraznovac-z-rukava-mikiny-a-skolskeho-tricka
  - `2168` https://www.vevo.sk/n/ako-odstranit-vodove-farby-z-detskej-zastery-a-rukavov-mikiny
- Source and evidence files:
  - `imports/retrofit_wave_14_kids_school_stains_five_2026_06_16.py`
  - `exports/retrofit-wave-14-kids-school-stains-five-2026-06-16.json`
  - `exports/retrofit-wave-14-kids-school-stains-five-2026-06-16-mcp-results.json`
  - `exports/retrofit-wave-14-kids-school-stains-five-2026-06-16-verification.json`
- Jod/dezinfekcia URL correction for the next wave:
  - The existing public URL for post `2198` uses `ako-odstranit-jod-a-dezinfekciu-z-oblecenia-bez-zvaecsenia-flaku`; the older local `zvacsenia` slug returned HTTP 404.
  - Wave 14 corrected the local source article link in `batch-18-2026-06-10-articles.json` so retrofit inventory can map the live post ID and URL.
- Current inventory after wave 14: 140 known VEVO article sources; `watch=60` and `major_expand=80`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Duplicate guard returned expected existing-article exact slug/title matches only; no new duplicate article was created.
  - Local link check before publish: 22 article/product/category/source links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1697, 1610, 1620, 1605, and 1643.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 21 checked article links returned HTTP 200.
  - Article-specific live marker fragments, checklist section, product link, and category link were verified on all five live articles.
  - Product and category blocks remain present without fixed prices; no public `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was added.

## Conservative Article Retrofit Wave 15 - Published 2026-06-16

- Conservatively expanded and republished five existing kids/cosmetic/outdoor stain articles while preserving titles, public URLs, visibility, publication dates, and short descriptions:
  - `2140` https://www.vevo.sk/n/ako-odstranit-zivicu-z-nohavic-bundy-a-detskeho-oblecenia
  - `2218` https://www.vevo.sk/n/ako-odstranit-zinkovu-mast-z-detskeho-body-a-prebalovacej-podlozky
  - `2213` https://www.vevo.sk/n/ako-dostat-piesok-z-detskych-sortiek-a-tricka-po-plazi-pred-pranim
  - `2160` https://www.vevo.sk/n/ako-vyprat-granatove-jablko-z-oblecenia-bez-ruzovych-map
  - `2198` https://www.vevo.sk/n/ako-odstranit-jod-a-dezinfekciu-z-oblecenia-bez-zvaecsenia-flaku
- Source and evidence files:
  - `imports/retrofit_wave_15_kids_cosmetic_stains_five_2026_06_16.py`
  - `exports/retrofit-wave-15-kids-cosmetic-stains-five-2026-06-16.json`
  - `exports/retrofit-wave-15-kids-cosmetic-stains-five-2026-06-16-mcp-results.json`
  - `exports/retrofit-wave-15-kids-cosmetic-stains-five-2026-06-16-verification.json`
- Current inventory after wave 15: 140 known VEVO article sources; `watch=65` and `major_expand=75`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Duplicate guard returned expected existing-article exact slug/title matches only; no new duplicate article was created.
  - Local link check before publish: 25 article/product/category/source links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1643, 1611, 1599, 1530, and 1554.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 24 checked article links returned HTTP 200.
  - Article-specific live marker fragments, checklist section, product link, and category link were verified on all five live articles.
  - Product and category blocks remain present without fixed prices; no public `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was added.

## Conservative Article Retrofit Wave 16 - Published 2026-06-16

- Conservatively expanded and republished five existing cosmetic/hair/craft stain articles while preserving titles, public URLs, visibility, publication dates, and short descriptions:
  - `2148` https://www.vevo.sk/n/ako-odstranit-ruz-z-kosele-salu-a-latkovej-servitky
  - `2183` https://www.vevo.sk/n/ako-prat-oblecenie-po-kadernictve-od-vlasov-farby-a-lakov
  - `2147` https://www.vevo.sk/n/ako-odstranit-podkladovy-krem-z-goliera-bluzky-a-kosele
  - `2169` https://www.vevo.sk/n/ako-odstranit-plastelinu-z-teplakov-koberca-a-potahu
  - `2178` https://www.vevo.sk/n/ako-odstranit-krem-na-ruky-z-rukavov-svetra-a-deky
- Source and evidence files:
  - `imports/retrofit_wave_16_cosmetic_haircraft_stains_five_2026_06_16.py`
  - `exports/retrofit-wave-16-cosmetic-haircraft-stains-five-2026-06-16.json`
  - `exports/retrofit-wave-16-cosmetic-haircraft-stains-five-2026-06-16-mcp-results.json`
  - `exports/retrofit-wave-16-cosmetic-haircraft-stains-five-2026-06-16-verification.json`
- Current inventory after wave 16: 140 known VEVO article sources; `watch=70` and `major_expand=70`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Duplicate guard returned expected existing-article exact slug/title matches only; no new duplicate article was created.
  - Local link check before publish: 30 article/product/category/source links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1638, 1637, 1618, 1573, and 1597.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 29 checked article links returned HTTP 200.
  - Article-specific live marker fragments, checklist section, product link, and category link were verified on all five live articles.
  - Product and category blocks remain present without fixed prices; no public `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was added.

## Conservative Article Retrofit Wave 17 - Published 2026-06-16

- Conservatively expanded and republished five existing mixed stain/travel articles while preserving titles, public URLs, visibility, publication dates, and short descriptions:
  - `2180` https://www.vevo.sk/n/ako-odstranit-lak-na-vlasy-z-goliera-kosele-a-satky
  - `2137` https://www.vevo.sk/n/ako-odstranit-majonezu-a-dressing-z-obrusu-bez-mastneho-flaku
  - `2224` https://www.vevo.sk/n/ako-odstranit-hrdzave-flaky-od-stipcov-a-snury-na-bielizen
  - `2145` https://www.vevo.sk/n/ako-odstranit-lak-na-nechty-z-textilu-bez-rozmazania-skvrny
  - `2216` https://www.vevo.sk/n/ako-prat-cestovne-oblecenie-po-dlhom-lete-alebo-vlaku
- Source and evidence files:
  - `imports/retrofit_wave_17_mixed_stains_travel_five_2026_06_16.py`
  - `exports/retrofit-wave-17-mixed-stains-travel-five-2026-06-16.json`
  - `exports/retrofit-wave-17-mixed-stains-travel-five-2026-06-16-mcp-results.json`
  - `exports/retrofit-wave-17-mixed-stains-travel-five-2026-06-16-verification.json`
- Current inventory after wave 17: 140 known VEVO article sources; `watch=75` and `major_expand=65`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Duplicate guard returned expected existing-article exact slug/title matches only; no new duplicate article was created.
  - Local link check before publish: 32 article/product/category/source links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1697, 1637, 1628, 1651, and 1666.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 31 checked article links returned HTTP 200.
  - Article-specific live marker fragments, checklist section, product link, and category link were verified on all five live articles.
  - Product and category blocks remain present without fixed prices; no public `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was added.

## Conservative Article Retrofit Wave 18 - Published 2026-06-17

- Conservatively expanded and republished five existing material/food articles while preserving titles, public URLs, visibility, publication dates, and short descriptions:
  - `2252` https://www.vevo.sk/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia
  - `2248` https://www.vevo.sk/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie
  - `2254` https://www.vevo.sk/n/certifikaty-na-textile-oeko-tex-gots-recyklovane-vlakna-a-co-znamenaju-pri-prani
  - `2229` https://www.vevo.sk/n/co-je-mikrovlakno-vyhody-nevyhody-savost-a-pranie
  - `2196` https://www.vevo.sk/n/ako-odstranit-vajicko-z-oblecenia-obrusu-a-kuchynskej-utierky
- Source and evidence files:
  - `imports/retrofit_wave_18_materials_food_five_2026_06_17.py`
  - `exports/retrofit-wave-18-materials-food-five-2026-06-17.json`
  - `exports/retrofit-wave-18-materials-food-five-2026-06-17-mcp-results.json`
  - `exports/retrofit-wave-18-materials-food-five-2026-06-17-verification.json`
- Current inventory after wave 18: 140 known VEVO article sources; `watch=80` and `major_expand=60`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Duplicate guard returned expected existing-article exact slug/title matches only; no new duplicate article was created.
  - Local link check before publish: 41 article/product/category/source links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1918, 1900, 1899, 1975, and 1535.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 31 checked article links returned HTTP 200.
  - Article-specific live marker fragments, checklist section, product link, and category link were verified on all five live articles.
  - Product and category blocks remain present without fixed prices; no public `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was added.

## Conservative Article Retrofit Wave 19 - Published 2026-06-17

- Conservatively expanded and republished five existing food/elastic care articles while preserving titles, public URLs, visibility, publication dates, and short descriptions:
  - `2158` https://www.vevo.sk/n/ako-vyprat-cierny-caj-z-bieleho-obrusu-bez-hnedych-map
  - `2174` https://www.vevo.sk/n/ako-odstranit-sojovu-omacku-z-kosele-obrusu-a-prestierania
  - `2176` https://www.vevo.sk/n/ako-odstranit-olivovy-olej-z-lanovej-kosele-bez-mastnej-mapy
  - `2135` https://www.vevo.sk/n/ako-vyprat-kari-a-kurkumu-z-bavlneneho-tricka-bez-zlteho-tiena
  - `2203` https://www.vevo.sk/n/ako-prat-kompresne-pancuchy-a-elasticke-zdravotne-navleky
- Source and evidence files:
  - `imports/retrofit_wave_19_food_elastic_five_2026_06_17.py`
  - `exports/retrofit-wave-19-food-elastic-five-2026-06-17.json`
  - `exports/retrofit-wave-19-food-elastic-five-2026-06-17-mcp-results.json`
  - `exports/retrofit-wave-19-food-elastic-five-2026-06-17-verification.json`
- Current inventory after wave 19: 140 known VEVO article sources; `watch=85` and `major_expand=55`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Duplicate guard returned expected existing-article exact slug/title matches only; no new duplicate article was created.
  - Local link check before publish: 25 article/product/category/source links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1561, 1532, 1538, 1579, and 1512.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 24 checked article links returned HTTP 200.
  - Article-specific live marker fragments, checklist section, product link, and category link were verified on all five live articles.
  - Product and category blocks remain present without fixed prices; no public `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was added.

## Conservative Article Retrofit Wave 20 - Published 2026-06-17

- Conservatively expanded and republished five existing delicate/stain care articles while preserving titles, public URLs, visibility, publication dates, and short descriptions:
  - `2150` https://www.vevo.sk/n/ako-prat-podprsenku-a-jemnu-spodnu-bielizen-bez-deformacie
  - `2194` https://www.vevo.sk/n/ako-odstranit-sadze-z-oblecenia-po-sviecke-grile-alebo-krbe
  - `2151` https://www.vevo.sk/n/ako-prat-kasmirovy-sveter-doma-bez-zrazenia-a-zmolkov
  - `2192` https://www.vevo.sk/n/ako-odstranit-hrdzu-z-oblecenia-obrusu-a-pracovnych-nohavic
  - `2190` https://www.vevo.sk/n/ako-prat-tylovu-suknu-zavoj-a-jemny-tyl-bez-potrhania
- Source and evidence files:
  - `imports/retrofit_wave_20_delicates_soot_cashmere_rust_tulle_five_2026_06_17.py`
  - `exports/retrofit-wave-20-delicates-soot-cashmere-rust-tulle-five-2026-06-17.json`
  - `exports/retrofit-wave-20-delicates-soot-cashmere-rust-tulle-five-2026-06-17-mcp-results.json`
  - `exports/retrofit-wave-20-delicates-soot-cashmere-rust-tulle-five-2026-06-17-verification.json`
- Current inventory after wave 20: 140 known VEVO article sources; `watch=90` and `major_expand=50`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Duplicate guard returned expected existing-article exact slug/title matches only; no new duplicate article was created.
  - Local link check before publish: 30 article/product/category/source links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1551, 1528, 1527, 1534, and 1522.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 29 checked article links returned HTTP 200.
  - Article-specific live marker fragments, checklist section, product link, and category link were verified on all five live articles.
  - Product and category blocks remain present without fixed prices; no public `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was added.

## Conservative Article Retrofit Wave 21 - Published 2026-06-17

- Conservatively expanded and republished five existing embellished/textile care articles while preserving titles, public URLs, visibility, publication dates, and short descriptions:
  - `2189` https://www.vevo.sk/n/ako-prat-oblecenie-s-flitrami-koralkami-a-aplikaciami
  - `2193` https://www.vevo.sk/n/ako-odstranit-mapy-od-vody-zo-sedacky-zavesov-a-calunenia
  - `2156` https://www.vevo.sk/n/ako-prat-sako-doma-a-kedy-ho-radsej-dat-do-cistiarne
  - `2155` https://www.vevo.sk/n/ako-prat-riflovu-bundu-a-tmave-dzinsy-aby-nepustali-farbu
  - `2175` https://www.vevo.sk/n/ako-odstranit-balzamikovy-ocot-z-bieleho-obrusu
- Source and evidence files:
  - `imports/retrofit_wave_21_embellished_water_suit_denim_balsamic_five_2026_06_17.py`
  - `exports/retrofit-wave-21-embellished-water-suit-denim-balsamic-five-2026-06-17.json`
  - `exports/retrofit-wave-21-embellished-water-suit-denim-balsamic-five-2026-06-17-mcp-results.json`
  - `exports/retrofit-wave-21-embellished-water-suit-denim-balsamic-five-2026-06-17-verification.json`
- Current inventory after wave 21: 140 known VEVO article sources; `watch=95` and `major_expand=45`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Duplicate guard returned expected existing-article exact slug/title matches only; no new duplicate article was created.
  - Local link check before publish: 31 article/product/category/source links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1555, 1580, 1539, 1536, and 1526.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 29 checked article links returned HTTP 200.
  - Article-specific live marker fragments, checklist section, product link, and category link were verified on all five live articles.
  - Product and category blocks remain present without fixed prices; public `hľadané výrazy`, `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was not left in the updated articles.

## Conservative Article Retrofit Wave 22 - Published 2026-06-17

- Conservatively expanded and republished five existing outdoor/material/school care articles while preserving titles, public URLs, visibility, publication dates, and short descriptions:
  - `2199` https://www.vevo.sk/n/ako-odstranit-repelent-z-outdoorovej-ciapky-a-navlekov-na-ruky
  - `2154` https://www.vevo.sk/n/ako-prat-lanovu-koselu-aby-nezostala-tvrda-a-pokrcena
  - `2191` https://www.vevo.sk/n/ako-prat-spolocenske-saty-doma-a-kedy-zvolit-cistiaren
  - `2171` https://www.vevo.sk/n/ako-vyprat-voskovky-z-peracnika-a-textilneho-obalu
  - `2188` https://www.vevo.sk/n/ako-prat-oblecenie-so-zipsami-a-suchym-zipsom-bez-zatrhnutia
- Source and evidence files:
  - `imports/retrofit_wave_22_repellent_linen_formal_crayons_zippers_five_2026_06_17.py`
  - `exports/retrofit-wave-22-repellent-linen-formal-crayons-zippers-five-2026-06-17.json`
  - `exports/retrofit-wave-22-repellent-linen-formal-crayons-zippers-five-2026-06-17-mcp-results.json`
  - `exports/retrofit-wave-22-repellent-linen-formal-crayons-zippers-five-2026-06-17-verification.json`
- Current inventory after wave 22: 140 known VEVO article sources; `watch=100` and `major_expand=40`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Duplicate guard returned expected existing-article exact slug/title matches only; no new duplicate article was created.
  - Local link check before publish: 32 article/product/category/source links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1597, 1541, 1529, 1527, and 1553.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 30 checked article links returned HTTP 200.
  - Article-specific live marker fragments, checklist section, product link, and category link were verified on all five live articles.
  - Product and category blocks remain present without fixed prices; public `hľadané výrazy`, `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was not left in the updated articles.

## Conservative Article Retrofit Wave 23 - Published 2026-06-17

- Conservatively expanded and republished five existing pet/stain/residue/wear care articles while preserving titles, public URLs, visibility, publication dates, and short descriptions:
  - `2164` https://www.vevo.sk/n/ako-prat-textilie-v-domacnosti-so-psom-pocas-plznutia
  - `2173` https://www.vevo.sk/n/ako-odstranit-cervenu-papriku-z-tricka-a-kuchynskej-utierky
  - `2163` https://www.vevo.sk/n/ako-odstranit-chlpy-z-oblecenia-pri-prani-ked-mate-psa-alebo-macku
  - `2186` https://www.vevo.sk/n/ako-odstranit-biele-smuhy-od-pracieho-prasku-z-cierneho-oblecenia
  - `2187` https://www.vevo.sk/n/ako-predist-dierkam-v-trickach-po-prani-a-suseni
- Source and evidence files:
  - `imports/retrofit_wave_23_pets_paprika_hair_residue_holes_five_2026_06_17.py`
  - `exports/retrofit-wave-23-pets-paprika-hair-residue-holes-five-2026-06-17.json`
  - `exports/retrofit-wave-23-pets-paprika-hair-residue-holes-five-2026-06-17-mcp-results.json`
  - `exports/retrofit-wave-23-pets-paprika-hair-residue-holes-five-2026-06-17-verification.json`
- Current inventory after wave 23: 140 known VEVO article sources; `watch=105` and `major_expand=35`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Duplicate guard returned expected existing-article exact slug/title matches only; no new duplicate article was created.
  - Local link check before publish: 29 article/product/category/source links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1594, 1506, 1549, 1539, and 1532.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 28 checked article links returned HTTP 200.
  - Article-specific live marker fragments, checklist section, product link, and category link were verified on all five live articles.
  - Product and category blocks remain present without fixed prices; public `hľadané výrazy`, `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was not left in the updated articles.

## Conservative Article Retrofit Wave 24 - Published 2026-06-17

- Conservatively expanded and republished five existing childcare/costume/cosmetic/sticky/food care articles while preserving titles, public URLs, visibility, publication dates, and short descriptions:
  - `2204` https://www.vevo.sk/n/ako-prat-textilne-navleky-na-kocik-po-prechadzke-v-dazdi
  - `2182` https://www.vevo.sk/n/ako-odstranit-pach-z-kostymu-po-karnevale-bez-poskodenia-latky
  - `2179` https://www.vevo.sk/n/ako-vyprat-suchy-sampon-z-cierneho-tricka-a-goliera
  - `2141` https://www.vevo.sk/n/ako-odstranit-zuvacku-z-nohavic-mikiny-a-potahu
  - `2136` https://www.vevo.sk/n/ako-odstranit-skvrny-od-horcice-z-tricka-obrusu-a-utierky
- Source and evidence files:
  - `imports/retrofit_wave_24_childcare_costume_cosmetic_sticky_food_five_2026_06_17.py`
  - `exports/retrofit-wave-24-childcare-costume-cosmetic-sticky-food-five-2026-06-17.json`
  - `exports/retrofit-wave-24-childcare-costume-cosmetic-sticky-food-five-2026-06-17-mcp-results.json`
  - `exports/retrofit-wave-24-childcare-costume-cosmetic-sticky-food-five-2026-06-17-verification.json`
- Current inventory after wave 24: 140 known VEVO article sources; `watch=110` and `major_expand=30`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Duplicate guard returned expected existing-article exact slug/title matches only; no new duplicate article was created.
  - Local link check before publish: 35 article/product/category/source links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1644, 1554, 1568, 1504, and 1530.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 32 checked article links returned HTTP 200.
  - Article-specific live marker fragments, checklist section, product link, and category link were verified on all five live articles.
  - Product and category blocks remain present without fixed prices; public `hľadané výrazy`, `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was not left in the updated articles.

## Conservative Article Retrofit Wave 25 - Published 2026-06-17

- Conservatively expanded and republished five existing workwear/hygiene/heat/beach/mask care articles while preserving titles, public URLs, visibility, publication dates, and short descriptions:
  - `2184` https://www.vevo.sk/n/ako-vyprat-pracovne-tricko-po-zahradkarceni-od-hliny-a-potu
  - `2144` https://www.vevo.sk/n/ako-prat-menstruacne-nohavicky-bezpecne-a-hygienicky
  - `2219` https://www.vevo.sk/n/ako-prat-potah-na-termofor-a-hrejivy-vankusik-bez-poskodenia-vyplne
  - `2214` https://www.vevo.sk/n/ako-prat-plazove-pareo-satku-a-lahku-tuniku-po-dovolenke
  - `2220` https://www.vevo.sk/n/ako-prat-latkove-ruska-a-textilne-obaly-hygienicky
- Source and evidence files:
  - `imports/retrofit_wave_25_workwear_hygiene_heat_beach_masks_five_2026_06_17.py`
  - `exports/retrofit-wave-25-workwear-hygiene-heat-beach-masks-five-2026-06-17.json`
  - `exports/retrofit-wave-25-workwear-hygiene-heat-beach-masks-five-2026-06-17-mcp-results.json`
  - `exports/retrofit-wave-25-workwear-hygiene-heat-beach-masks-five-2026-06-17-verification.json`
- Current inventory after wave 25: 140 known VEVO article sources; `watch=115` and `major_expand=25`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Duplicate guard returned expected existing-article exact slug/title matches only; no new duplicate article was created.
  - Local link check before publish: 33 article/product/category/source links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1600, 1544, 1514, 1556, and 1620.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 31 checked article links returned HTTP 200.
  - Article-specific live marker fragments, checklist section, product link, and category link were verified on all five live articles.
  - Product and category blocks remain present without fixed prices; public `hľadané výrazy`, `CTA`, `longtail`, `SEO`, `fan-out`, or `sub-query` wording was not left in the updated articles.

## Conservative Article Retrofit Wave 26 - Published 2026-06-17

- Conservatively expanded and republished five existing car/glitter/insoles/rack/pollen care articles while preserving titles, public URLs, visibility, publication dates, and short descriptions:
  - `2212` https://www.vevo.sk/n/ako-vycistit-navlek-na-autosedacku-po-zime-a-posypovej-soli
  - `2181` https://www.vevo.sk/n/ako-odstranit-trblietky-z-siat-saka-a-kabata-po-oslave
  - `2210` https://www.vevo.sk/n/ako-vycistit-textilne-vlozky-do-topanok-po-zime
  - `2223` https://www.vevo.sk/n/ako-vycistit-susiak-na-bielizen-aby-neprenasal-spinu-na-pradlo
  - `2221` https://www.vevo.sk/n/ako-striast-pel-z-bundy-a-mikiny-po-prechadzke-pred-pranim
- Source and evidence files:
  - `imports/retrofit_wave_26_car_glitter_insoles_rack_pollen_five_2026_06_17.py`
  - `exports/retrofit-wave-26-car-glitter-insoles-rack-pollen-five-2026-06-17.json`
  - `exports/retrofit-wave-26-car-glitter-insoles-rack-pollen-five-2026-06-17-mcp-results.json`
  - `exports/retrofit-wave-26-car-glitter-insoles-rack-pollen-five-2026-06-17-verification.json`
- Current inventory after wave 26: 140 known VEVO article sources; `watch=120` and `major_expand=20`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Duplicate guard returned expected existing-article exact slug/title matches only; filtered duplicate check found no non-self conflicts.
  - Local link check before publish: 35 unique internal article/product/category links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1634, 1569, 1568, 1615, and 1640.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 35 checked article links returned HTTP 200.
  - Article-specific live marker fragments, checklist section, product link, and category link were verified on all five live articles.
  - Product and category blocks remain present without fixed prices; public internal workflow wording was not left in the updated article content. The literal `CTA` string seen in live HTML is from the global theme element ID `vevo-sticky-cta`, not the article body.

## Conservative Article Retrofit Wave 27 - Published 2026-06-17

- Conservatively expanded and republished five existing cycling/pollen/ski-wax/tissue/glue care articles while preserving titles, public URLs, visibility, publication dates, and short descriptions:
  - `2211` https://www.vevo.sk/n/ako-vycistit-cyklisticke-navleky-na-tretry-po-dazdi-a-blate
  - `2222` https://www.vevo.sk/n/ako-prat-oblecenie-pri-pelovej-alergii-po-prichode-zvonka
  - `2209` https://www.vevo.sk/n/ako-odstranit-vosk-na-lyze-z-lyziarskej-bundy-a-rukavic
  - `2185` https://www.vevo.sk/n/ako-dostat-kusky-papierovej-vreckovky-z-ciernych-nohavic-a-mikiny
  - `2166` https://www.vevo.sk/n/ako-odstranit-sekundove-lepidlo-z-textilu-a-kedy-to-nerobit-doma
- Source and evidence files:
  - `imports/retrofit_wave_27_cycling_pollen_wax_tissue_glue_five_2026_06_17.py`
  - `exports/retrofit-wave-27-cycling-pollen-wax-tissue-glue-five-2026-06-17.json`
  - `exports/retrofit-wave-27-cycling-pollen-wax-tissue-glue-five-2026-06-17-mcp-results.json`
  - `exports/retrofit-wave-27-cycling-pollen-wax-tissue-glue-five-2026-06-17-verification.json`
- Current inventory after wave 27: 140 known VEVO article sources; `watch=125` and `major_expand=15`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Filtered duplicate guard found no non-self conflicts.
  - Local link check before publish: 22 unique internal article/product/category links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1651, 1619, 1559, 1542, and 1569.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 22 checked article links returned HTTP 200.
  - Article-specific live marker fragments, checklist section, product link, category link, and small-test section were verified on all five live articles.
  - Product and category blocks remain present without fixed prices; public internal workflow wording was not left in the updated article content.

## Conservative Article Retrofit Wave 28 - Published 2026-06-17

- Conservatively expanded and republished five existing ski/socks/thermal/rain/mat care articles while preserving titles, public URLs, visibility, publication dates, and short descriptions:
  - `2207` https://www.vevo.sk/n/ako-odstranit-sol-a-mokry-sneh-z-lyziarskych-rukavic-s-membranou
  - `2132` https://www.vevo.sk/n/ako-prat-biele-ponozky-aby-nezosedli-a-nezostali-tvrde
  - `2208` https://www.vevo.sk/n/ako-prat-kuklu-nakrcnik-a-termo-ciapku-po-lyzovani
  - `2127` https://www.vevo.sk/n/ako-prat-prsiplast-a-reflexne-nepremokave-nohavice-po-dazdi
  - `2206` https://www.vevo.sk/n/ako-vycistit-rohozku-a-textilie-v-predsieni-od-posypovej-soli
- Source and evidence files:
  - `imports/retrofit_wave_28_ski_socks_thermal_rain_mat_five_2026_06_17.py`
  - `exports/retrofit-wave-28-ski-socks-thermal-rain-mat-five-2026-06-17.json`
  - `exports/retrofit-wave-28-ski-socks-thermal-rain-mat-five-2026-06-17-mcp-results.json`
  - `exports/retrofit-wave-28-ski-socks-thermal-rain-mat-five-2026-06-17-verification.json`
- Current inventory after wave 28: 140 known VEVO article sources; `watch=130` and `major_expand=10`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Filtered duplicate guard found no non-self conflicts.
  - Local link check before publish: 27 unique internal article/product/category links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1626, 1623, 1629, 1536, and 1581.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 27 checked article links returned HTTP 200.
  - Article-specific live marker fragments, checklist section, product link, category link, and local-cleaning section were verified on all five live articles.
  - Product and category blocks remain present without fixed prices; public internal workflow wording was not left in the updated article content.

## Conservative Article Retrofit Wave 29 - Published 2026-06-17

- Conservatively expanded and republished five existing sports/glue/color/salt care articles while preserving titles, public URLs, visibility, publication dates, and short descriptions:
  - `2129` https://www.vevo.sk/n/ako-prat-futbalovy-dres-stucne-a-treningove-veci-po-zapase
  - `2130` https://www.vevo.sk/n/ako-prat-hokejovy-dres-a-textilne-vrstvy-z-vystroja
  - `2165` https://www.vevo.sk/n/ako-odstranit-lepidlo-z-oblecenia-po-tvoreni-s-detmi
  - `2133` https://www.vevo.sk/n/ako-zabranit-pustaniu-farby-pri-prani-noveho-oblecenia
  - `2205` https://www.vevo.sk/n/ako-odstranit-solne-mapy-z-nohavic-a-kabata-po-zime
- Source and evidence files:
  - `imports/retrofit_wave_29_sports_glue_color_salt_five_2026_06_17.py`
  - `exports/retrofit-wave-29-sports-glue-color-salt-five-2026-06-17.json`
  - `exports/retrofit-wave-29-sports-glue-color-salt-five-2026-06-17-mcp-results.json`
  - `exports/retrofit-wave-29-sports-glue-color-salt-five-2026-06-17-verification.json`
- Current inventory after wave 29: 140 known VEVO article sources; `watch=135` and `major_expand=5`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - Filtered duplicate guard found no non-self conflicts.
  - Local link check before publish: 26 unique internal article/product/category links returned HTTP 200.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1606, 1583, 1552, 1533, and 1593.
  - `tools/vevo_retrofit_live_verify.py`: passed; 5 public article URLs returned HTTP 200 and all 26 checked article links returned HTTP 200.
  - Article-specific live marker fragments, checklist section, product link, category link, and split-load section were verified on all five live articles.
  - Product and category blocks remain present without fixed prices; public internal workflow wording was not left in the updated article content.

## Conservative Article Retrofit Wave 30 - Published 2026-06-17

- Conservatively expanded the final five existing expert/material articles while preserving titles, URLs, publication dates, and short descriptions:
  - `2237` https://www.vevo.sk/n/co-je-lan-preco-sa-krci-ako-ho-prat-a-ako-ho-zjemnit
  - `2251` https://www.vevo.sk/n/symboly-prania-na-stitku-co-znamena-vanicka-trojuholnik-kruh-stvorec-a-zehlicka
  - `2253` https://www.vevo.sk/n/ako-prat-nove-oblecenie-prvykrat-farby-chemicky-pach-zrazanie-a-stitok
  - `2247` https://www.vevo.sk/n/co-je-membranove-oblecenie-vodny-stlpec-priedusnost-a-pranie-bez-poskodenia
  - `2249` https://www.vevo.sk/n/mikroplasty-z-oblecenia-ako-prat-syntetiku-zodpovednejsie-bez-paniky
- Live publishing result: 4 public articles were updated in Biznisweb; post `2251` was not republished because it was intentionally hidden after batch 25 as a duplicate of the canonical laundry-symbol guide.
- Source and evidence files:
  - `imports/retrofit_wave_30_expert_material_symbols_new_membrane_microplastics_five_2026_06_17.py`
  - `exports/retrofit-wave-30-expert-material-symbols-new-membrane-microplastics-five-2026-06-17.json`
  - `exports/retrofit-wave-30-expert-material-symbols-new-membrane-microplastics-five-2026-06-17-live-public.json`
  - `exports/retrofit-wave-30-expert-material-symbols-new-membrane-microplastics-five-2026-06-17-mcp-results.json`
  - `exports/retrofit-wave-30-expert-material-symbols-new-membrane-microplastics-five-2026-06-17-verification.json`
- Current inventory after wave 30: 140 known VEVO article sources; `watch=140` and `major_expand=0`.
- Verification:
  - `scripts/check.ps1`: passed, `remaining_hit_count=0`.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1622, 1526, 1577, 1553, and 1561.
  - Local internal link check: 20 unique internal article/product/category links returned HTTP 200.
  - `tools/vevo_retrofit_live_verify.py`: passed on the 4 public articles; 4 article URLs returned HTTP 200 and all 17 checked public-article links returned HTTP 200.
  - Hidden duplicate URL for post `2251` still returns HTTP 404 by design.
  - Product and category blocks remain present without fixed prices; public internal workflow wording was not left in the updated article content.

## Batch 28 - Published 2026-06-17

- Prepared the next small VEVO new-content batch from the broader fragrance/laundry content plan. The batch is intentionally limited to 3 longer articles to keep quality high after the retrofit backlog:
  - `Ako vybrať vôňu do prania na zimu: deky, svetre, šály a sezónne textílie`
  - `Ako prevoňať bielizeň v malej kúpeľni: vlhkosť, sušenie a jemná vôňa bez zatuchnutia`
  - `Parfum do prania pri citlivej pokožke: kedy voliť jemnú vôňu a kedy radšej bez parfumácie`
- Target publish date for all three articles: `2025-09-24`, before the required `2025-10-12` cutoff.
- Published into VEVO Blog news block `765` on page `309` with clean slugs and public URLs:
  - `2265` `https://www.vevo.sk/n/ako-vybrat-vonu-do-prania-na-zimu-deky-svetre-saly-a-sezonne-textilie`
  - `2266` `https://www.vevo.sk/n/ako-prevonat-bielizen-v-malej-kupelni-vlhkost-susenie-a-jemna-vona-bez-zatuchnutia`
  - `2267` `https://www.vevo.sk/n/parfum-do-prania-pri-citlivej-pokozke-kedy-volit-jemnu-vonu-a-kedy-radsej-bez-parfumacie`
- Source and evidence files:
  - `content-plan/batch-28-fragrance-laundry-fanout-2026-06-17.md`
  - `batches/batch-28-candidates-2026-06-17.txt`
  - `imports/build_batch_28_fragrance_laundry.py`
  - `imports/batch-28-2026-06-17-articles.json`
  - `exports/batch-28-2026-06-17-duplicate-guard.json`
  - `exports/batch-28-2026-06-17-mapping.json`
  - `exports/batch-28-2026-06-17-preflight.json`
  - `exports/batch-28-2026-06-17-public-content-guard.json`
  - `exports/batch-28-2026-06-17-verification.json`
  - temp XLS: `C:\Users\Patrik jankech\AppData\Local\Temp\vevo-batch-28-fragrance-laundry-clean-urls.xls`
- Verification completed before publication:
  - `tools/vevo_duplicate_guard.py`: one article `OK`; two articles `REVIEW` only because they are in the broad fragrance cluster, manually accepted with distinct intents (small-bathroom humidity and sensitive-skin fragrance boundary).
  - Link preflight: 23 article/product/category/source links returned HTTP 200 after replacing three initially broken internal URLs with live VEVO URLs.
  - `tools/vevo_public_content_guard.py`: passed, `remaining_hit_count=0`.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1728, 1566, and 1627.
  - `scripts/check.ps1`: passed.
- Publication verification:
  - `exports/batch-28-2026-06-17-verification.json`: all three public URLs returned HTTP 200, RSS dates are `Wed, 24 Sep 2025`, titles and canonicals are present, styled HTML blocks are present, product cards and category links are present, all checked source links returned HTTP 200, and article source text has no public internal workflow wording.
  - Full-page HTML contains the global layout id `vevo-sticky-cta`; this is not article content and is called out separately in the verification export.

## Batch 29 - Published 2026-06-17

- Prepared the next small VEVO new-content batch from the fragrance/laundry use-case plan. The batch stayed at 3 longer articles to preserve quality:
  - `najcastejsie-chyby-pri-parfumoch-do-prania-privela-vone-zly-oplach-a-miesanie-s-avivazou`
  - `ako-prevonat-oblecenie-do-kancelarie-jemna-vona-kosela-pri-krku-a-pradlo-bez-tazkej-parfumacie`
  - `vona-oblecenia-v-kufri-ako-balit-cistu-bielizen-na-cestu-aby-nezatuchla`
- Target publish date for all three articles: `2025-09-23`, before the required `2025-10-12` cutoff.
- Published into VEVO Blog news block `765` on page `309` with clean slugs and public URLs:
  - `2268` `https://www.vevo.sk/n/najcastejsie-chyby-pri-parfumoch-do-prania-privela-vone-zly-oplach-a-miesanie-s-avivazou`
  - `2269` `https://www.vevo.sk/n/ako-prevonat-oblecenie-do-kancelarie-jemna-vona-kosela-pri-krku-a-pradlo-bez-tazkej-parfumacie`
  - `2270` `https://www.vevo.sk/n/vona-oblecenia-v-kufri-ako-balit-cistu-bielizen-na-cestu-aby-nezatuchla`
- Source and evidence files:
  - `content-plan/batch-29-fragrance-use-cases-2026-06-17.md`
  - `batches/batch-29-candidates-2026-06-17.txt`
  - `imports/build_batch_29_fragrance_use_cases.py`
  - `imports/batch-29-2026-06-17-articles.json`
  - `exports/batch-29-2026-06-17-duplicate-guard.json`
  - `exports/batch-29-2026-06-17-mapping.json`
  - `exports/batch-29-2026-06-17-preflight.json`
  - `exports/batch-29-2026-06-17-verification.json`
  - temp XLS: `C:\Users\Patrik jankech\AppData\Local\Temp\vevo-batch-29-fragrance-use-cases-clean-urls.xls`
- Verification completed before publication:
  - `tools/vevo_duplicate_guard.py`: two articles `OK`; one article `REVIEW` only because it shares the broad fragrance cluster, manually accepted as a distinct office/workwear intent.
  - Link preflight: 23 article/product/category/source links returned HTTP 200 after replacing one broken external source URL and correcting one internal article URL.
  - `tools/vevo_public_content_guard.py`: passed, `remaining_hit_count=0`.
  - `tools/vevo_article_depth_guard.py`: passed; visible word counts are 1662, 1659, and 1612.
  - `scripts/check.ps1`: passed.
- Publication verification:
  - `exports/batch-29-2026-06-17-verification.json`: all three public URLs returned HTTP 200, `datePublished` values match `2025-09-23T08:00:00`, `2025-09-23T08:12:00`, and `2025-09-23T08:24:00`, long article bodies are present, styled HTML blocks and tables are present, product links and category links are present, checked article links returned HTTP 200, and article body text has no fixed prices or public internal workflow wording.
  - Recovery note: the first manual save created public pages with title/short/meta only and empty long bodies. Each post was reopened, the prepared HTML was inserted through source mode, the editor was toggled back before saving, and the block was saved again. The final public verification confirms non-empty long bodies for all three posts.

## Batch 31 - Partial publication 2026-06-29

- Prepared the next small VEVO new-content batch focused on material guides that were not already covered in the known VEVO article inventory:
  - `co-je-saten-nie-je-to-vzdy-hodvab-a-ako-ho-spravne-prat`
  - `co-je-flanel-preco-hreje-ako-sa-perie-a-preco-moze-zmolkovat`
  - `co-je-mansester-rebrovana-latka-prach-v-rebrach-a-spravne-pranie`
- Target publish date for all three articles: `2025-09-21`, before the required `2025-10-12` cutoff.
- Published and verified:
  - `2275` `https://www.vevo.sk/n/co-je-saten-nie-je-to-vzdy-hodvab-a-ako-ho-spravne-prat`
  - Public verification returned HTTP 200, canonical URL matched the clean slug, visible date text was `21.9.2025`, styled article blocks were present, and product/category links were present.
  - The initial numeric public URL created by the API, `https://www.vevo.sk/n/111`, now returns HTTP 404 after the admin UI slug fix.
- Created but intentionally left hidden until the admin UI date/slug/publish flow is completed:
  - `2278` `co-je-flanel-preco-hreje-ako-sa-perie-a-preco-moze-zmolkovat`
  - `2279` `co-je-mansester-rebrovana-latka-prach-v-rebrach-a-spravne-pranie`
  - Their target public URLs returned HTTP 404 while hidden, which is the desired safe state until final UI publication.
- Source and evidence files:
  - `imports/batch-31-2026-06-29-articles.json`
  - `exports/batch-31-2026-06-29-partial-publication.json`
- Verification completed before publication:
  - Duplicate guard passed against the known VEVO duplicate set.
  - Article depth guard passed; each article was prepared as a longer practical/expert guide.
  - Public wording guard passed; no public article body contains internal workflow wording such as `longtail`, `SEO`, `fan-out`, `sub-query`, or `CTA`.
  - Link preflight passed for checked internal article/product/category/source links.
- Workflow correction:
  - BiznisWeb API creation can create posts, but it does not reliably set final date and slug.
  - Final publication for posts created this way must be done through the admin UI: set `Aktívne`, select the date through the date picker so the hidden ISO date field changes, fill the SEO URL field, save the post detail, then save the parent `Novinky` block.
  - Do not set `visible=true` through the API after the UI slug/date edit, because it can leave or recreate numeric URL/date drift.
- Current blocker:
  - The in-app browser connection timed out while reading the admin page and again while taking a lightweight screenshot of `https://vevo.flox.sk/erp/main/pages/309`.
  - No further UI publication was attempted after those timeouts, because hidden posts `2278` and `2279` are safer than public posts with wrong date or numeric URL.
- Duplicate cleanup:
  - The admin list showed duplicate flanel and manšester posts.
  - Root cause: the first create attempt likely succeeded, but its response was parsed from the wrong JSON path, so a second create attempt created duplicate posts.
  - Deleted extra post IDs `2276` and `2277` through `biznisweb-delete_news_post`; both delete calls returned success.
  - Public verification after cleanup: satén clean URL returned HTTP 200; flanel and manšester clean URLs returned 404 while hidden; numeric URLs `108` through `120` were not publicly available.
  - Added hard duplicate-safety rules to `workflows/biznisweb-news-import.md` and `README_DEV.md`: never retry a create call after null/malformed response until the real created ID is resolved.

## Batch 32 - Partially Published 2026-07-05

- Prepared 11 new VEVO robot-vacuum articles requested by the user, outside the existing content plan but inside the home-care content scope:
  - `ako-vybrat-roboticky-vysavac`
  - `ako-vybrat-roboticky-vysavac-s-mopom`
  - `ako-vycistit-roboticky-vysavac`
  - `ako-restartovat-roboticky-vysavac`
  - `ako-sparovat-roboticky-vysavac-xiaomi`
  - `ako-dlho-sa-nabija-roboticky-vysavac`
  - `ako-funguje-roboticky-vysavac`
  - `ako-zapnut-roboticky-vysavac`
  - `ako-odvapnit-roboticky-vysavac`
  - `roboticky-vysavac-je-offline`
  - `kam-umiestnit-roboticky-vysavac-a-kam-ho-schovat`
- Target publish date for all articles: `2025-09-20`, before the required `2025-10-12` cutoff.
- Required VEVO links are present in every article:
  - `https://www.vevo.sk/c/vevo-home-care/upratovanie/cistiace-prostriedky/cistic-do-robotickeho-vysavaca`
  - `https://www.vevo.sk/p-1635/vevo-cistic-podlah-pre-vsetky-vysavace-ylang-absolute`
- Source and evidence files:
  - `content-plan/batch-32-robot-vacuum-2026-07-05.md`
  - `batches/batch-32-candidates-2026-07-05.txt`
  - `imports/build_batch_32_robot_vacuum.py`
  - `imports/batch-32-2026-07-05-articles.json`
  - `exports/batch-32-2026-07-05-duplicate-guard.json`
  - `exports/batch-32-2026-07-05-public-content-guard.json`
  - `exports/batch-32-2026-07-05-depth-guard.json`
  - `exports/batch-32-2026-07-05-link-preflight.json`
  - `exports/batch-32-2026-07-05-blocker.json`
  - temp XLS: `C:/Users/Patrik jankech/AppData/Local/Temp/vevo-batch-32-robot-vacuum-clean-urls.xls`
- Verification completed:
  - Duplicate guard passed for all 11 titles against 766 known VEVO records.
  - Public wording guard passed with `remaining_hit_count=0`.
  - Link preflight passed: all checked links returned OK and both required VEVO targets are present in every article.
  - Article depth guard passed; visible word counts are 1569, 1571, 1574, 1583, 1661, 1632, 1714, 1614, 1717, 1628, and 1722.
  - Generated article text contains no fixed prices and no public internal workflow wording.
- Publication status:
  - Published through the VEVO admin UI: `https://www.vevo.sk/n/ako-vybrat-roboticky-vysavac` returned HTTP 200 after save.
  - Pre-save verification for the published post confirmed title `Ako vybrať robotický vysávač`, date `20.9.2025`, time `08:00:00`, active visibility, clean slug `ako-vybrat-roboticky-vysavac`, exact prepared long HTML, and both required robot-vacuum cleaner links.
  - The other 10 batch 32 clean public URLs were checked immediately after the failed continuation and returned HTTP 404, so they were not created/published in this attempt.
  - Direct `biznisweb_add_news_post` was still intentionally not used for the remaining public creation because it cannot set clean slugs or `date_posted/time_posted`.
  - Browser blocker: after the first successful UI save, the next automated `Nový príspevok` attempt did not expose the expected title field, then the in-app browser automation started timing out even on targeted tab/page-state reads and reload attempts. The separate BiznisWeb news-block connector also timed out on `list_news_blocks`, so continuing would risk duplicate or malformed posts.
- Next exact step: reopen VEVO admin in a fresh working browser session, verify the Blog news block `765` list contains exactly one `Ako vybrať robotický vysávač`, then continue batch 32 from `ako-vybrat-roboticky-vysavac-s-mopom` through the remaining 10 articles. Do not recreate `ako-vybrat-roboticky-vysavac`.

## Batch 32 - Published and Verified 2026-07-05

- Completion update: all 11 robot-vacuum articles are now public with clean slugs and the target publish date `2025-09-20`.
- Public post IDs and slugs:
  - `2280` - `ako-vybrat-roboticky-vysavac`
  - `2281` - `ako-vybrat-roboticky-vysavac-s-mopom`
  - `2282` - `ako-vycistit-roboticky-vysavac`
  - `2283` - `ako-restartovat-roboticky-vysavac`
  - `2284` - `ako-sparovat-roboticky-vysavac-xiaomi`
  - `2285` - `ako-dlho-sa-nabija-roboticky-vysavac`
  - `2286` - `ako-funguje-roboticky-vysavac`
  - `2287` - `ako-zapnut-roboticky-vysavac`
  - `2288` - `ako-odvapnit-roboticky-vysavac`
  - `2289` - `roboticky-vysavac-je-offline`
  - `2290` - `kam-umiestnit-roboticky-vysavac-a-kam-ho-schovat`
- Publication method: admin UI created the public posts with clean slugs/date; `imports/update_batch_32_rich_html.py` then normalized rich HTML through `biznisweb-update_news_post` because the admin WYSIWYG save path stored `short`/meta but emptied the public `long` body for posts `2281-2290`.
- Verification exports:
  - `exports/batch-32-2026-07-05-public-id-map.json`
  - `exports/batch-32-2026-07-05-rich-html-results.json`
- Final verification: `record_count=11`, `updated_count=10`, `ok_count=11`, `all_ok=true`. Every public URL returned HTTP 200, public body length is 16012 to 19866 chars, required category and product links are present, styled article blocks are present, escaped quote artifacts are absent, and forbidden public/internal wording checks passed.
- Required target links were also verified directly:
  - `https://www.vevo.sk/c/vevo-home-care/upratovanie/cistiace-prostriedky/cistic-do-robotickeho-vysavaca` -> HTTP 200
  - `https://www.vevo.sk/p-1635/vevo-cistic-podlah-pre-vsetky-vysavace-ylang-absolute` -> HTTP 200

## Batch 32 - Layout Repair 2026-07-05

- User reported broken public paragraphs where words were rendered as one character per line. Root cause: `imports/build_batch_32_robot_vacuum.py` rendered some `sections` values as iterable strings instead of treating them as paragraph strings, producing long runs such as `<p>P</p><p>r</p><p>e</p>`.
- Fixed the batch generator so string section content is wrapped as one paragraph, and fixed generated times so the final article uses `09:00:00` instead of invalid `08:60:00`.
- Added a structural guard to `tools/vevo_article_depth_guard.py`: it now fails batches with long runs of very short paragraphs. The broken pre-fix batch failed with 10/11 articles affected; the regenerated batch now passes with `failure_count=0`, word counts `1525-2074`, `max_short_paragraph_run=0`, and `short_paragraph_count=0`.
- Updated `workflows/biznisweb-news-import.md` to explicitly block one-character paragraph runs before publication.
- Republished all existing batch 32 post IDs `2280-2290` through `imports/update_batch_32_rich_html.py` using existing post IDs only; no new posts were created.
- Updated `imports/update_batch_32_rich_html.py` verification so required links must be real `<a href>` elements and so one-character paragraph runs fail the report.
- Verification exports:
  - `exports/batch-32-2026-07-05-depth-guard.json`
  - `exports/batch-32-2026-07-05-layout-repair-verification.json`
  - `exports/batch-32-2026-07-05-rich-html-results.json`
- Final live verification: `record_count=11`, `ok_count=11`, `all_ok=true`; every public URL returned HTTP 200, `max_short_paragraph_run=0`, `short_paragraph_count=0`, required category/product links exist as real anchors, and body lengths are `15459-19534`.

## Batch 33 - Published and Verified 2026-07-06

- Prepared the next small VEVO new-content batch from home-care clusters C17/C19:
  - `ako-umyt-podlahu-bez-smuh-laminat-vinyl-dlazba-a-mopovanie-v-praxi`
  - `ako-vycistit-kuchynsku-linku-od-mastnoty-prachu-a-smuh-bez-poskodenia-povrchu`
  - `ako-vycistit-drez-a-bateriu-vodny-kamen-mastnota-zapach-a-bezpecne-cistenie`
- Target publish date for all three articles: `2025-09-19`, before the required `2025-10-12` cutoff.
- Source and evidence files:
  - `content-plan/batch-33-floor-kitchen-cleaning-2026-07-06.md`
  - `batches/batch-33-candidates-2026-07-06.txt`
  - `imports/build_batch_33_floor_kitchen_cleaning.py`
  - `imports/create_batch_33_hidden_drafts.py`
  - `imports/batch-33-2026-07-06-articles.json`
  - `exports/batch-33-2026-07-06-duplicate-guard.json`
  - `exports/batch-33-2026-07-06-public-content-guard.json`
  - `exports/batch-33-2026-07-06-depth-guard.json`
  - `exports/batch-33-2026-07-06-link-preflight.json`
  - `exports/batch-33-2026-07-06-hidden-drafts.json`
  - `exports/batch-33-2026-07-06-hidden-verification.json`
  - temp XLS: `C:/Users/Patrik jankech/AppData/Local/Temp/vevo-batch-33-floor-kitchen-cleaning-clean-urls.xls`
- Verification completed before creation:
  - Duplicate guard returned `OK` for all 3 titles against 777 existing VEVO RSS records.
  - Public wording guard returned `remaining_hit_count=0`.
  - Link preflight checked 29 article/product/category/source links with 0 failures.
  - Article depth guard passed: visible word counts are `1672`, `1533`, and `1599`; every article has 2 tables, styled blocks, product/category links, FAQ, and `max_short_paragraph_run=0`.
  - `scripts/check.ps1` passed.
- Creation status:
  - Created hidden draft `2291` for the floor article through direct tool call after admin UI was found logged out.
  - Created hidden drafts `2292` and `2293` through `imports/create_batch_33_hidden_drafts.py`, which reads the generated JSON, skips already mapped titles, and saves the real `news_post.id` immediately after each successful create.
  - No article was made public through direct API because it cannot safely set clean slugs or backdated publish dates.
  - Target clean URLs returned HTTP 404 while hidden, which was the expected safe state before final UI publication.
- Publication status:
  - Existing hidden draft IDs `2291`, `2292`, and `2293` were edited in the VEVO admin UI only; no new duplicate posts were created.
  - All three posts were marked active, assigned clean slugs, dated `2025-09-19`, saved in the parent Blog news block `765`, and verified on public URLs.
  - Published URLs:
    - `2291` `https://www.vevo.sk/n/ako-umyt-podlahu-bez-smuh-laminat-vinyl-dlazba-a-mopovanie-v-praxi`
    - `2292` `https://www.vevo.sk/n/ako-vycistit-kuchynsku-linku-od-mastnoty-prachu-a-smuh-bez-poskodenia-povrchu`
    - `2293` `https://www.vevo.sk/n/ako-vycistit-drez-a-bateriu-vodny-kamen-mastnota-zapach-a-bezpecne-cistenie`
  - Public verification export `exports/batch-33-2026-07-06-publication.json`: `record_count=3`, `ok_count=3`, `all_ok=true`; every public URL returned HTTP 200, dates are present, long rich HTML bodies are present, styled blocks and tables are present, all checked article/product/category/source links returned OK, no escaped HTML was found, public/internal wording guard is clean, and `max_short_paragraph_run=0`.
  - No rich HTML API normalization was needed for this batch because the admin save preserved public long bodies.

## Batch 34 - Published and Verified 2026-07-08

- Prepared and published the next small VEVO bathroom-cleaning batch from C18 home-care topics:
  - `ako-vycistit-sprchovy-kut-vodny-kamen-mydlove-usadeniny-skary-a-sklo-bez-smuh`
  - `ako-vycistit-skary-v-kupelni-plesen-vodny-kamen-a-zazltnute-miesta-bez-poskodenia`
  - `ako-vycistit-sprchovu-hlavicu-vodny-kamen-slaby-prud-a-hygienicka-udrzba`
- Publish date: current date `2026-07-08`, explicitly allowed by the user after the backdated admin date field proved unreliable. Clean URL remained mandatory.
- Published IDs and URLs:
  - `2294` `https://www.vevo.sk/n/ako-vycistit-sprchovy-kut-vodny-kamen-mydlove-usadeniny-skary-a-sklo-bez-smuh`
  - `2295` `https://www.vevo.sk/n/ako-vycistit-skary-v-kupelni-plesen-vodny-kamen-a-zazltnute-miesta-bez-poskodenia`
  - `2297` `https://www.vevo.sk/n/ako-vycistit-sprchovu-hlavicu-vodny-kamen-slaby-prud-a-hygienicka-udrzba`
- Source and evidence files:
  - `content-plan/batch-34-bathroom-odor-cleaning-2026-07-08.md`
  - `batches/batch-34-candidates-2026-07-08.txt`
  - `imports/build_batch_34_bathroom_cleaning.py`
  - `imports/batch-34-2026-07-08-articles.json`
  - `exports/batch-34-2026-07-08-duplicate-guard.json`
  - `exports/batch-34-2026-07-08-public-content-guard.json`
  - `exports/batch-34-2026-07-08-depth-guard.json`
  - `exports/batch-34-2026-07-08-link-preflight.json`
  - `exports/batch-34-2026-07-08-admin-publish-attempt.json`
  - `exports/batch-34-2026-07-08-publication.json`
  - temp XLS: `C:/Users/Patrik jankech/AppData/Local/Temp/vevo-batch-34-bathroom-cleaning-clean-urls.xls`
- Verification completed before publication attempt:
  - First third candidate about odpadkovy kos was rejected by duplicate guard because VEVO already has `Ako odstrániť zápach z odpadkového koša v kuchyni`; it was replaced with the sprchova hlavica topic.
  - Duplicate guard returned `OK` for all 3 final titles against 780 existing VEVO RSS records.
  - Link preflight checked 32 article/product/category/source links with 0 failures.
  - Public wording guard returned `remaining_hit_count=0`.
  - Article depth guard passed with visible word counts `1531`, `1551`, and `1500`; every article has at least 2 tables, styled recommendation blocks, product/category links, FAQ, and `max_short_paragraph_run=0`.
  - `scripts/check.ps1` passed.
- Publication attempt:
  - VEVO admin was logged in and Blog page `309` / news block `765` was opened.
  - The `Nový príspevok` form was reachable and the first article could be partially filled.
  - The visible `date_posted_xdate` value accepted `18.9.2025`, but the hidden `date_posted` field stayed empty. Saving was stopped to avoid creating a public article with the wrong/current date.
  - The unsaved modal was cancelled; no batch 34 public posts or hidden posts were created.
- Final publication after updated user instruction:
  - User changed the requirement: current publish date is acceptable, clean URL is required.
  - Hidden-first create via `add_news_post` was used only to generate and inspect clean slugs in Blog page `309` / news block `765`.
  - Final rich HTML was filled through BiznisWeb admin source mode. Required save sequence: click `Upraviť HTML zdroj`, fill `short` and `long`, click the `HTML` toggle back to visual mode, then save. Skipping the toggle can leave old/draft long content in public output.
  - Active checkbox sometimes timed out through selector control; direct coordinate click on the checkbox was used after reading its bounding rectangle.
  - `update_news_post` was tested on hidden draft `2296` with `visible=false`; it changed the slug to `111`, so the draft was deleted and recreated as `2297`. Do not use remote `update_news_post` for VEVO slug-sensitive articles.
  - Public verification export `exports/batch-34-2026-07-08-publication.json`: `record_count=3`, `ok_count=3`, `all_ok=true`; all public URLs returned HTTP 200, no draft text remained, inline styles and tables are present, product/category links are present, and no escaped HTML was found.

## Batch 35 - Published and Verified 2026-07-08

- Prepared the requested batch of 10 VEVO articles from C01-C06 laundry/fragrance clusters:
  - `preco-parfum-do-prania-necitit-po-ususeni`
  - `parfum-do-prania-pri-rucnom-prani`
  - `parfum-do-prania-pri-prani-na-30-stupnov`
  - `parfum-do-prania-pri-prani-na-60-stupnov`
  - `praci-gel-alebo-prasok-co-sa-hodi-na-bezne-pranie`
  - `praci-gel-na-cierne-oblecenie`
  - `praci-gel-na-biele-oblecenie`
  - `praci-gel-na-farebne-oblecenie`
  - `praci-gel-pri-nizkych-teplotach`
  - `preplnena-pracka-ako-kazi-pranie`
- Source and evidence files:
  - `content-plan/batch-35-c01-c06-pranie-fragrance-2026-07-08.md`
  - `batches/batch-35-candidates-2026-07-08.txt`
  - `imports/build_batch_35_c01_c06.py`
  - `imports/batch-35-2026-07-08-articles.json`
  - `imports/publish_batch_35_via_mcp.py`
  - `exports/batch-35-2026-07-08-duplicate-guard.json`
  - `exports/batch-35-2026-07-08-link-preflight.json`
  - `exports/batch-35-2026-07-08-public-content-guard.json`
  - `exports/batch-35-2026-07-08-depth-guard.json`
  - `exports/batch-35-2026-07-08-publication.json`
  - `exports/batch-35-2026-07-08-publication-final.json`
  - `exports/batch-35-2026-07-08-cleanup.json`
- Verification completed before publication attempt:
  - Initial exact duplicate candidate `Ako dávkovať parfum do prania podľa množstva bielizne` was removed before writing final articles.
  - Duplicate guard passed against 783 known VEVO records.
  - Link preflight checked 22 article/product/category/source links with 0 failures.
  - Public wording guard returned `remaining_hit_count=0`.
  - Article depth guard passed with visible word counts `1660`, `1579`, `1589`, `1545`, `1627`, `1546`, `1544`, `1508`, `1540`, and `1557`; every article has rich inline-styled HTML, tables, product/category cards without fixed prices, FAQ, and `max_short_paragraph_run=0`.
- Publication attempt and rollback:
  - Remote MCP `biznisweb-add_news_post`/`biznisweb-update_news_post` was tested because the user asked for direct MCP publishing.
  - The remote MCP created post IDs `2298-2307`, but generated public URLs as `/n/111`, `/n/111111`, and similar repeated-`1` slugs instead of the expected clean slugs.
  - Passing an extra `link` field to `biznisweb-update_news_post` did not repair the public URL.
  - All 10 bad post IDs `2298-2307` were deleted through MCP cleanup; all checked bad `/n/111...` URLs returned HTTP 404 afterward.
  - `imports/publish_batch_35_via_mcp.py --publish` now refuses by default unless the explicit `--allow-unsafe-mcp-publish` escape hatch is passed. Do not use that escape hatch for production publishing.
- Final publication:
  - Published all 10 prepared articles through the VEVO admin UI in Blog page/news block `765`, using the current publish date as allowed by the user and explicitly filling each SEO `link` field with the prepared clean slug.
  - Reliable UI sequence for this batch: fill visible `title` and `short`, click the TinyMCE source icon `a.mce_code` / `*_code`, fill the underlying `textarea[name="long"]` directly with raw HTML, switch to the SEO tab, fill `title_tag`, `link`, and `description`, then save the visible post form. Do not use the visible `HTML` text button as the source-entry control; it can leave the editor in a confusing WYSIWYG state.
  - Public URLs:
    - `https://www.vevo.sk/n/preco-parfum-do-prania-necitit-po-ususeni`
    - `https://www.vevo.sk/n/parfum-do-prania-pri-rucnom-prani`
    - `https://www.vevo.sk/n/parfum-do-prania-pri-prani-na-30-stupnov`
    - `https://www.vevo.sk/n/parfum-do-prania-pri-prani-na-60-stupnov`
    - `https://www.vevo.sk/n/praci-gel-alebo-prasok-co-sa-hodi-na-bezne-pranie`
    - `https://www.vevo.sk/n/praci-gel-na-cierne-oblecenie`
    - `https://www.vevo.sk/n/praci-gel-na-biele-oblecenie`
    - `https://www.vevo.sk/n/praci-gel-na-farebne-oblecenie`
    - `https://www.vevo.sk/n/praci-gel-pri-nizkych-teplotach`
    - `https://www.vevo.sk/n/preplnena-pracka-ako-kazi-pranie`
  - Final public verification export `exports/batch-35-2026-07-08-publication-final.json`: `record_count=10`, `ok_count=10`, `all_ok=true`; every public URL returned HTTP 200 with the clean expected slug, non-404 H1, inline styles, tables, product/category links, no escaped HTML, no one-letter paragraph runs, and no repeated-`1` slug.
- Next exact step for this batch: none; batch 35 is complete. For any future VEVO batch, keep using clean-slug admin/source-mode or XLS workflow until MCP supports explicit `link` preservation.

## Batch 36 - Bedding Care Articles

- Date: 2026-07-09
- Status: complete and publicly verified.
- Scope: 5 new VEVO Blog articles focused on bedding-adjacent laundry care.
- Published public URLs:
  - `https://www.vevo.sk/n/ako-prat-chranic-matraca-pot-prach-roztoce-a-spravne-susenie`
  - `https://www.vevo.sk/n/ako-prat-paplon-a-prikryvku-velkost-bubna-vypln-a-susenie-bez-zapachu`
  - `https://www.vevo.sk/n/ako-prat-plachtu-s-gumou-rohy-pot-zrazenie-a-susenie-bez-pokrcenia`
  - `https://www.vevo.sk/n/ako-prat-prehoz-na-postel-prach-chlpy-objem-a-susenie`
  - `https://www.vevo.sk/n/ako-prat-postelnu-suknu-a-textilie-okolo-postele-prach-chlpy-a-sezonne-pranie`
- Source and evidence files:
  - `content-plan/batch-36-bedding-care-2026-07-09.md`
  - `batches/batch-36-candidates-2026-07-09.txt`
  - `imports/build_batch_36_bedding_care.py`
  - `imports/batch-36-2026-07-09-articles.json`
  - `exports/batch-36-2026-07-09-duplicate-guard.json`
  - `exports/batch-36-2026-07-09-link-preflight.json`
  - `exports/batch-36-2026-07-09-public-content-guard.json`
  - `exports/batch-36-2026-07-09-depth-guard.json`
  - `exports/batch-36-2026-07-09-html-safety-guard.json`
  - `exports/batch-36-2026-07-09-publication-verify.json`
- Verification before publication:
  - Duplicate guard passed against 793 known VEVO records.
  - Link preflight checked 17 article/product/category/source links with 0 failures.
  - Depth guard passed for all 5 articles; visible word counts were `1761`, `1578`, `1629`, `1570`, and `1541`.
  - Public wording guard returned `remaining_hit_count=0`.
  - HTML safety guard passed: inline styles, tables, product/category cards, no fixed prices, no escaped HTML, and no one-letter paragraph runs.
- Publication notes:
  - Published through VEVO admin UI in Blog page/news block `765` with clean manually filled SEO `link` slugs.
  - Critical admin workflow: do not rely on programmatic `fill()` for TinyMCE long body. Open the TinyMCE source icon, paste the HTML into the visible `textarea[name="long"]` via clipboard/keyboard input, click the visible `HTML` button to exit source mode, then fill SEO fields and save.
  - The first article initially saved with an empty long body; it was repaired in admin and reverified before publishing the rest of the batch.
- Final public verification:
  - `exports/batch-36-2026-07-09-publication-verify.json`: all 5 public URLs returned HTTP 200 with the expected clean slug, title, quick-answer section, inline styles, tables, product/category links, perfume-category link, external source links, no escaped HTML, and no one-letter paragraph runs.
- Next exact step for this batch: none; batch 36 is complete. Do not recreate any batch 36 slug.

## Project Audit and Guard Hardening - 2026-07-14

- Added `tools/vevo_project_audit.py` with a JSON report at `exports/vevo-project-audit-2026-07-14.json`. The audit checks required project files, branch/remote state, current publication evidence, live catalog health, and every Python entrypoint capable of mutating VEVO news.
- Expanded `tools/vevo_duplicate_guard.py` from RSS-only title similarity to merged live RSS, FAQ, and local prepared-batch coverage. It now checks cross-candidate collisions, same action/subject intent, exact live-title duplicates, and invalid repeated-`1` slugs.
- Added `tools/vevo_html_safety_guard.py` to block malformed slugs, repeated-`1` placeholders, escaped HTML, scripts/event handlers, malformed links, fixed prices, missing product/category blocks, and one-character paragraph damage.
- Fixed Unicode word counting in `tools/vevo_article_depth_guard.py` and added persistent JSON reports.
- Added seven regression tests in `tests/test_content_guards.py`; `scripts/check.ps1` runs them before project and article checks.
- Added explicit `--execute-live` hard gates to five historical scripts that could previously mutate live content merely by being executed. The audit currently identifies 39 live-mutation entrypoints; every CLI has an explicit opt-in flag and the repo-local MCP server has hidden-first plus publish/delete confirmation guards.
- Live catalog finding: RSS contains seven exact-title duplicate groups. Six use a canonical slug plus a second slug ending in `1`; the seventh is the curtain-ironing article duplicated at `/n/111111111111111111`. No destructive cleanup was performed because canonical post IDs and redirects must first be confirmed in admin.
- Git finding: this checkout has no configured remote and branch `opan-claw` has no upstream. `git fetch --all --prune` succeeds as a no-op, but `git pull --rebase` and `git push` cannot complete until a remote/upstream is configured.

## Batch 37 - Published Through Slug-Safe MCP 2026-07-14

- Final topics selected only after rejecting an initial curtain/drape/blind/tablecloth/kitchen-towel set as duplicate or near-duplicate coverage:
  - `ako-vycistit-radiator-od-prachu-rebra-zadna-strana-mastnota-a-bezpecna-udrzba`
  - `ako-vycistit-parapety-a-okenne-ramy-prach-pel-cierne-mapy-a-skvrny`
  - `ako-vycistit-interierove-dvere-a-zarubne-odtlacky-mastnota-a-povrch-bez-smuh`
  - `ako-vycistit-vypinace-a-klucky-dotykove-miesta-mastnota-a-bezpecny-postup`
  - `ako-vycistit-soklove-listy-prach-smuhy-chlpy-a-rohy-bez-poskodenia`
- Source and evidence files:
  - `content-plan/batch-37-overlooked-home-surfaces-2026-07-14.md`
  - `batches/batch-37-candidates-2026-07-14.txt`
  - `imports/build_batch_37_overlooked_home_surfaces.py`
  - `imports/batch-37-2026-07-14-articles.json`
  - `imports/publish_vevo_batch_via_content_mcp.py`
  - `imports/verify_batch_37_public.py`
  - `exports/batch-37-2026-07-14-duplicate-guard.json`
  - `exports/batch-37-2026-07-14-link-preflight.json`
  - `exports/batch-37-2026-07-14-public-content-guard.json`
  - `exports/batch-37-2026-07-14-depth-guard.json`
  - `exports/batch-37-2026-07-14-html-safety-guard.json`
  - `exports/vevo-mcp-slug-smoke-2026-07-14.json`
  - `exports/batch-37-2026-07-14-mcp-publication.json`
  - `exports/batch-37-2026-07-14-publication-verify.json`
- Local verification: all five target URLs returned HTTP 404 before creation; all 14 unique article/product/category/source destinations returned HTTP 200; the duplicate guard passed all five topics against 801 merged known records.
- Quality metrics: visible word counts are `1790`, `1757`, `1727`, `1735`, and `1762`. Every article has 19 H2 sections, two tables, six styled blocks, one product link, two category links, five FAQ questions, a dedicated 120-165 character meta description, and no one-character paragraphs.
- MCP implementation:
  - Added repo-local server `tools/biznisweb_vevo_content_mcp.py`, registered locally as `biznisweb-vevo-content`.
  - The server requires explicit clean ASCII `link`, rejects repeated-`1` placeholders, scans up to 2,000 admin rows for title/slug duplicates, creates hidden first, performs title/slug/HTML admin readback, and requires `confirm_visible=true` for publication.
  - The legacy remote `biznisweb-add_news_post`/`biznisweb-update_news_post` tools remain blocked; they are not the helper used for this batch.
- Disposable MCP smoke:
  - Hidden test post `2323` preserved exact slug `codex-vevo-content-mcp-smoke-20260714063006`, returned the rich HTML marker intact, stayed public `404`, and was deleted.
  - Post-delete verification found zero admin matches and public `404`; `exports/vevo-mcp-slug-smoke-2026-07-14.json` reports `all_ok=true`.
- Publication result:
  - `2324`: `https://www.vevo.sk/n/ako-vycistit-radiator-od-prachu-rebra-zadna-strana-mastnota-a-bezpecna-udrzba`
  - `2325`: `https://www.vevo.sk/n/ako-vycistit-parapety-a-okenne-ramy-prach-pel-cierne-mapy-a-skvrny`
  - `2326`: `https://www.vevo.sk/n/ako-vycistit-interierove-dvere-a-zarubne-odtlacky-mastnota-a-povrch-bez-smuh`
  - `2327`: `https://www.vevo.sk/n/ako-vycistit-vypinace-a-klucky-dotykove-miesta-mastnota-a-bezpecny-postup`
  - `2328`: `https://www.vevo.sk/n/ako-vycistit-soklove-listy-prach-smuhy-chlpy-a-rohy-bez-poskodenia`
- Final verification:
  - MCP publication report: `record_count=5`, `public_ok_count=5`, `all_ok=true`.
  - Independent public verifier: `article_count=5`, `links_checked=14`, `all_ok=true`; it checked exact title/slug, inline styles, tables, product/category links, external sources, escaped HTML, and one-character paragraph damage.
  - Full post-publication project check passed with `27` unit/regression tests, zero article depth failures, zero HTML safety failures, and project audit `block_count=0`.
- Workflow defect found and fixed during the project audit:
  - `scripts/check.ps1` previously invoked Python guards without checking `$LASTEXITCODE`, so PowerShell could continue after duplicate guard exit `1` or `2` and still print `VEVO_CONTENT check OK`.
  - All Python calls now run through `Invoke-PythonChecked`; a negative test against the already-live batch 37 candidates confirms the wrapper stops and returns nonzero when duplicate guard blocks them.
  - `tools/vevo_project_audit.py` now treats missing native exit-code enforcement as a blocking project defect and verifies the local `biznisweb-vevo-content` registration without exposing credentials.
- No browser session was used for creation or publication.

## Batch 38 - Cleaning Tools and Overlooked Areas 2026-07-14

- Status: complete, published through the repo-local slug-safe MCP, and independently verified on the public site.
- Duplicate-safe topics:
  - `ako-vycistit-mop-a-vedro-spinava-voda-usadeniny-a-spravne-susenie`
  - `ako-vycistit-hubky-a-kefy-na-upratovanie-mastnota-opotrebovanie-a-vcasna-vymena`
  - `ako-vycistit-vetracie-mriezky-v-domacnosti-prach-mastnota-a-prudenie-vzduchu`
  - `ako-vycistit-strop-a-rohy-od-prachu-a-pavucin-bez-rozmazania`
  - `ako-vycistit-garnizu-a-kolajnice-zaclon-prach-mastnota-a-zasekavanie`
- Candidate selection rejected exact or intent-level overlaps for microfiber cloths, vacuum attachments, broom/dustpan, steam mop, window squeegee, lampshades, bins, picture frames, and artificial decorations. The final five all passed the merged live/local guard against `806` records without a manual override.
- Quality evidence:
  - visible word counts: `2081`, `1975`, `1865`, `1987`, `1901`;
  - every article: `20` H2 sections, `2` tables, `7` styled blocks, `1` product link, `2` category links, at least `5` FAQ questions, no fixed prices, no forbidden internal wording, and no one-character paragraphs;
  - link preflight: `29` checks total, including five free public slugs and `24` unique internal/product/category/source destinations, `failure_count=0`;
  - full pre-publication check: `27` unit/regression tests, duplicate/content/depth/HTML gates all passed.
- MCP publication result:
  - `2329`: `https://www.vevo.sk/n/ako-vycistit-mop-a-vedro-spinava-voda-usadeniny-a-spravne-susenie`
  - `2330`: `https://www.vevo.sk/n/ako-vycistit-hubky-a-kefy-na-upratovanie-mastnota-opotrebovanie-a-vcasna-vymena`
  - `2331`: `https://www.vevo.sk/n/ako-vycistit-vetracie-mriezky-v-domacnosti-prach-mastnota-a-prudenie-vzduchu`
  - `2332`: `https://www.vevo.sk/n/ako-vycistit-strop-a-rohy-od-prachu-a-pavucin-bez-rozmazania`
  - `2333`: `https://www.vevo.sk/n/ako-vycistit-garnizu-a-kolajnice-zaclon-prach-mastnota-a-zasekavanie`
- Persistent publication report: `exports/batch-38-2026-07-14-mcp-publication.json` has `record_count=5`, `public_ok_count=5`, `all_ok=true`.
- Independent public report: `exports/batch-38-2026-07-14-publication-verify.json` has `article_count=5`, `links_checked=24`, `all_ok=true`; all pages preserved the expected clean slug, rich HTML, links, and visible depth.
- No browser session was used for creation or publication.

## Cross-section Duplicate Audit 2026-07-14

- Status: read-only inventory and similarity audit complete for all three Slovak content blocks:
  - glossary/encyclopedia page `805`, block `1905`;
  - FAQ page `313`, block `774`;
  - Blog page `309`, block `765`.
- Durable report: `exports/vevo-cross-section-duplicate-audit-2026-07-14.json`.
- Audited `829` admin records: `808` active/public and `21` hidden.
- Findings before remediation:
  - `9` exact normalized-title groups in the complete admin inventory;
  - `7` groups contain more than one active/public post;
  - `4` exact normalized-content groups, one of them public: post `1682` and bad-slug post `1520` contain the same curtain-ironing article;
  - `88` near-title pairs were scored from full titles and bodies; `14` were promoted for manual high-priority review.
- Manual classification keeps legitimate fan-out topics separate, including 30 vs. 60 degree washing, robot vacuum vs. robot vacuum with mop, general vs. gender-specific fragrance guides, and general vs. material-specific sink cleaning.
- Confirmed remediation targets include the seven public exact-title groups plus strongly overlapping intent clusters for laundry-perfume definition/use, wash cost, streak-free windows, detergent plus laundry perfume, and best-laundry-perfume selection.
- The audit is reproducible through `tools/vevo_cross_section_duplicate_audit.py`; two regression tests were added and the full suite passes with `30` tests.
- Exact-duplicate remediation is complete through the repo-local content MCP:
  - `12` articles in the seven public title groups were expanded into distinct, useful scopes;
  - canonical clean URLs were preserved;
  - bad-slug curtain clone `1520` was hidden after canonical post `1682` was verified;
  - no public exact-title or exact-body duplicate remains in the post-remediation audit.
- Exact remediation evidence:
  - `exports/exact-duplicate-remediation-2026-07-14-results.json` reports `article_count=12` and `all_ok=true`;
  - `exports/vevo-cross-section-duplicate-audit-2026-07-14-post-remediation.json` reports `public_exact_title_group_count=0` and `public_exact_body_group_count=0`.
- All `88` near-title pairs were manually classified. Most are legitimate fan-out topics. A focused set of `23` existing articles needs additive differentiation across detergent variants, laundry perfumes, wash-temperature use cases, sports textiles, bathroom mats, black clothing, windows, wash cost, and related troubleshooting.
- Semantic-remediation preflight is complete:
  - every existing title and slug is locked and must remain unchanged;
  - the prepared expansion set contains `23` rich HTML articles with `1509-3238` visible words;
  - link preflight passed for all article destinations;
  - duplicate, content, depth, HTML, and unit-test gates passed with `36` tests.
- Semantic remediation implementation:
  - `imports/remediate_semantic_duplicates_2026_07_14.py` is read-only by default and requires `--execute-live` for updates;
  - `tools/vevo_rich_expansion_renderer.py` preserves the original content, removes malformed one-character paragraphs, and adds differentiated expert sections, tables, sources, product/category cards, and FAQ;
  - `content-plan/cross-section-duplicate-remediation-2026-07-14.md` records every remediated, retained, and rejected pair.
- First live semantic-remediation pass exposed a legacy admin encoding edge case: the same emoji can be returned as a UTF-16 surrogate pair. Content MCP `0.3.3` now normalizes both expected and actual readback values before comparison; a dedicated regression test covers title and body readback without weakening slug or truncation checks.
- The first complete live pass also exposed a quality issue in the shared expansion renderer: common generic decision/recovery prose increased cosine similarity for several otherwise distinct articles. That version is not accepted as the final remediation.
- Corrective semantic rebuild is prepared from the immutable pre-remediation backup:
  - generic decision/recovery prose was removed from the rendered article body;
  - `imports/semantic_duplicate_deep_dives_2026_07_14.py` supplies article-specific analysis for all `23` targets;
  - `--rebuild-from-backup` requires the current live marker, validates the original backup id set, and builds a replacement without stacking a second expansion onto the first;
  - prepared articles contain `1510-2977` visible words and preserve the original body after the new differentiated material;
  - all links, content/depth/HTML guards, project audit, and `37` unit/regression tests pass;
  - selected short historical overlap pairs now have five-word shingle Jaccard values around `0.012-0.028`; the six recent long-form hand/30/60-degree and black/white/color articles retain more of their original shared wording by design, but their prepared overlap is lower than both the original and first-pass versions.
- Final semantic remediation is complete through the repo-local MCP/API:
  - `23/23` existing articles were rebuilt from the original backup and passed admin readback with unchanged titles and slugs;
  - final visible depth is `1510-2977` words per target;
  - `exports/semantic-duplicate-remediation-2026-07-14-results.json` reports `all_ok=true`.
- Final full-inventory audit:
  - `829` records, `807` active and `22` hidden;
  - `0` public exact-title groups and `0` public exact-body groups;
  - `88` near-title pairs and `14` title-led high-priority pairs remain as review signals, all manually classified in `content-plan/cross-section-duplicate-remediation-2026-07-14.md`.
- Independent public verification:
  - `35/35` remediated public pages preserve the prepared visible body, exact title and exact slug;
  - every checked article retains rich HTML, product and category links, and has no one-character paragraph damage or forbidden internal wording;
  - hidden legacy post `1520` on repeated-`1` slug remains public `404`;
  - durable report: `exports/duplicate-remediation-public-verify-2026-07-14.json`, `all_ok=true`.

## Batch 39 Test Article - Gramáž látky a GSM 2026-07-14

- Status: complete, published through the repo-local slug-safe content MCP, and independently verified on the public site.
- Article:
  - title: `Gramáž látky: čo znamená GSM pri uterákoch, obliečkach a tričkách`;
  - slug: `gramaz-latky-co-znamena-gsm-pri-uterakoch-oblieckach-a-trickach`;
  - content-plan source: C09A material encyclopedia gap.
- Full three-block duplicate check found no standalone article about gramáž látky, GSM, or plošná hmotnosť. The merged live/local duplicate guard checked `810` records and returned `ok` with no cluster or review issue.
- Link preflight checked the still-free target slug plus `11` article destinations. The target returned exact `404`; all five related VEVO articles, the product, category, and three primary technical sources returned `200` without a broken destination.
- Prepared article quality:
  - `2230` visible words, `17` H2 headings, `2` tables, `9` styled blocks;
  - one product card and one category card without a fixed price;
  - `10` H3 sections including six reader-facing FAQ questions;
  - zero one-character paragraphs, zero malformed HTML findings, and zero forbidden internal wording.
- Full pre-publication suite passed: `37/37` unit/regression tests, project audit `block_count=0`, duplicate/content/depth/HTML guards all green.
- Repo-local content MCP dry run loaded `557` admin catalog rows and returned `preflight_passed=true`; no browser session is required.
- MCP publication result:
  - post ID `2334`;
  - public URL: `https://www.vevo.sk/n/gramaz-latky-co-znamena-gsm-pri-uterakoch-oblieckach-a-trickach`;
  - hidden create preserved exact title, slug, `270`-character short text, and `22891`-character rich HTML body while the public URL remained `404`;
  - explicit publication retained the same slug/body and changed the public response to `200`;
  - `exports/batch-39-test-gsm-2026-07-14-mcp-publication.json` reports `record_count=1`, `public_ok_count=1`, `all_ok=true`.
- Independent public verification:
  - exact clean URL and title present;
  - quick-answer marker, `2` tables, rich styled layout, product/category cards, all related VEVO links, and all three technical sources present;
  - zero escaped HTML, zero malformed links, and no one-character paragraph run;
  - all `11` unique outgoing destinations returned `200`;
  - `exports/batch-39-test-gsm-2026-07-14-publication-verify.json` reports `article_count=1`, `all_ok=true`.
- Read-only browser render check confirmed the H1, both `100%`-width tables, and both action buttons are visible and remain inside the article column at the default desktop viewport and a temporary `390x844` mobile viewport; neither table has internal horizontal overflow. The temporary viewport override was reset and the verification tab was closed.
- Final post-publication project check passed with `37/37` unit/regression tests, project audit `block_count=0`, live catalog `record_count=808`, zero duplicate-title groups, zero bad slugs, and all content/depth/HTML gates green.
- No browser session was used for creation, publication, or verification.
- Durable artifacts:
  - `content-plan/batch-39-test-gsm-2026-07-14.md`;
  - `imports/build_batch_39_test_gsm.py`;
  - `imports/batch-39-test-gsm-2026-07-14-articles.json`;
  - `imports/verify_batch_39_test_gsm_public.py`;
  - corresponding duplicate, link, content, depth, and HTML reports under `exports/`.

## Batch 40 Material Decision Guides - Published 2026-07-16

- Status: complete, published through the repo-local slug-safe content MCP, and independently verified on the public site.
- Complete duplicate scan covered Blog block `765`, FAQ block `774`, and Slovník/Encyklopédia block `1905`. All four final candidates returned `ok`; the live catalog had zero duplicate-title groups and zero bad slugs before creation.
- Topics and public records:
  - `2335`: `https://www.vevo.sk/n/priedusnost-savost-a-rychloschnutie-ako-citat-vlastnosti-textilu`
  - `2336`: `https://www.vevo.sk/n/bavlna-lan-saten-alebo-flanel-ake-obliecky-vybrat-podla-sezony-a-potenia`
  - `2337`: `https://www.vevo.sk/n/frote-bambus-alebo-mikrovlakno-ktory-uterak-vybrat-podla-savosti-a-schnutia`
  - `2338`: `https://www.vevo.sk/n/polyester-polyamid-merino-alebo-elastan-z-coho-ma-byt-sportove-oblecenie`
- Link preflight checked `41` destinations with zero failures. Each future target slug returned exact `404`; all existing VEVO articles, the product, the category, and technical sources returned the expected successful response.
- Prepared article quality:
  - visible word counts `2133`, `2034`, `2023`, and `2010`;
  - each article has `19` H2 headings, `2` tables, `9` styled blocks, a product card, a category card, and at least six reader-facing FAQ questions;
  - zero fixed prices, zero forbidden internal wording, zero malformed HTML findings, and zero one-character paragraphs.
- Full pre-publication suite passed: `37/37` unit/regression tests, project audit `block_count=0`, and duplicate/content/depth/HTML guards all green.
- MCP publication used hidden-first creation and explicit visibility confirmation. Every hidden record preserved its exact title, slug and rich HTML while the public URL returned `404`; every final record retained the same slug/body and returned `200`. `exports/batch-40-2026-07-16-mcp-publication.json` reports `record_count=4`, `public_ok_count=4`, and `all_ok=true`.
- Independent public verification checked exact titles and clean paths, at least `1700` visible words, `19` H2 sections, both tables, all styled blocks, product/category links, external sources, escaped HTML, malformed links, and short-paragraph damage. All `37` unique outgoing links returned `200`; `exports/batch-40-2026-07-16-publication-verify.json` reports `all_ok=true`.
- Read-only responsive QA on the first article confirmed two tables, nine styled blocks, zero article one-character paragraphs, and no document-level horizontal overflow at a temporary `390x844` viewport. The viewport override was reset afterward.
- Final post-publication project check passed with `37/37` unit/regression tests, project audit `block_count=0`, live catalog `record_count=812`, zero duplicate-title groups, zero bad slugs, and all content/depth/HTML gates green.
- Durable artifacts:
  - `content-plan/batch-40-material-decision-guides-2026-07-16.md`;
  - `imports/build_batch_40_material_decision_guides.py`;
  - `imports/batch-40-2026-07-16-articles.json`;
  - `imports/verify_batch_40_public.py`;
  - corresponding duplicate, link, content, depth, HTML, MCP publication, and public verification reports under `exports/`.

## Batch 41 Material Blends and Performance - Published 2026-07-21

- Status: complete, published through the repo-local slug-safe content MCP, independently verified on the public site, and checked at a mobile breakpoint.
- Complete duplicate selection covered Blog block `765`, FAQ block `774`, and Slovník/Encyklopédia block `1905`. The first proposal rejected polyester/elastane and viscose/elastane articles as too close to existing coverage; no override was used. All four replacement candidates returned `ok` against `815` merged known records before creation.
- Topics and public records:
  - `2340`: `https://www.vevo.sk/n/bavlna-a-elastan-starostlivost-o-tricka-rifle-a-spodnu-bielizen`
  - `2341`: `https://www.vevo.sk/n/vlna-a-polyamid-preco-sa-miesaju-vlakna-a-ako-to-ovplyvnuje-pranie`
  - `2342`: `https://www.vevo.sk/n/staticka-elektrina-v-obleceni-preco-latky-prilnu-a-ako-obmedzit-iskrenie`
  - `2343`: `https://www.vevo.sk/n/odolnost-textilu-proti-oderu-co-znamena-martindale-pri-obleceni-a-bytovych-latkach`
- Link preflight checked `33` destinations with zero failures. Each future target slug returned exact `404`; all existing VEVO articles, the product, category, and primary or authoritative technical sources returned `200`.
- Prepared article quality:
  - visible word counts `2856`, `2560`, `2496`, and `2427`;
  - each article has `25` H2 headings, `2` tables, `11` styled blocks, a concrete product card, a category card, and six reader-facing FAQ questions;
  - zero fixed prices, zero forbidden internal wording, zero malformed HTML findings, and zero one-character paragraphs.
- Full pre-publication suite passed: `37/37` unit/regression tests, project audit `block_count=0`, and duplicate/content/depth/HTML guards all green.
- Disposable hidden MCP smoke post `2339` preserved its exact slug and rich HTML, remained public `404`, and was deleted; the follow-up admin lookup found zero matches. `exports/batch-41-2026-07-21-mcp-smoke.json` reports `all_ok=true`.
- MCP publication used hidden-first creation and explicit visibility confirmation. Every hidden record preserved its exact title, slug, short text, and `25580-27957` character rich HTML while the public URL returned `404`; every final record retained the same slug/body and returned `200`. `exports/batch-41-2026-07-21-mcp-publication.json` reports `record_count=4`, `public_ok_count=4`, and `all_ok=true`.
- Independent public verification checked exact titles and clean paths, minimum depth, quick-answer markers, both tables, styled blocks, product/category cards, external sources, escaped HTML, malformed links, and short-paragraph damage. All `29` unique outgoing destinations returned `200`; `exports/batch-41-2026-07-21-publication-verify.json` reports `all_ok=true`.
- Read-only responsive DOM QA on the first article confirmed no uncontained horizontal overflow or text overflow at `390x844`. Both `680px` tables remain inside `351px` containers with `overflow-x:auto`; product and category buttons fit inside the article column. Browser screenshot capture timed out twice, so the render conclusion is based on measured layout geometry plus the independent public HTML verification. The temporary viewport override was reset and the verification tab was closed.
- Final post-publication project check passed with `37/37` unit/regression tests, project audit `block_count=0`, live catalog `record_count=816`, zero duplicate-title groups, zero bad slugs, and all content/depth/HTML gates green.
- Durable artifacts:
  - `content-plan/batch-41-material-blends-and-performance-2026-07-21.md`;
  - `batches/batch-41-candidates-2026-07-21.txt`;
  - `imports/build_batch_41_material_blends_and_performance.py`;
  - `imports/batch-41-2026-07-21-articles.json`;
  - `imports/verify_batch_41_public.py`;
  - corresponding duplicate, link, content, depth, HTML, MCP smoke, MCP publication, and public verification reports under `exports/`.

## Batch 42 Textile Construction and Durability - Published 2026-07-22

- Status: complete, published through the repo-local slug-safe content MCP, independently verified on the public site, and checked at desktop and mobile widths.
- Complete duplicate selection covered Blog block `765`, FAQ block `774`, and Slovník/Encyklopédia block `1905`. A broader set of eight candidates passed the first scan, but the final batch was deliberately narrowed to four distinct intents to avoid tight overlap with existing Martindale and shrinkage coverage. All four final candidates returned `ok` against `819` merged known records before creation.
- Topics and public records:
  - `2345`: `https://www.vevo.sk/n/stalofarebnost-textilu-preco-farby-blednu-pri-prani-svetle-a-treni`
  - `2346`: `https://www.vevo.sk/n/pevnost-sva-a-posun-niti-preco-oblecenie-praska-pri-svoch`
  - `2347`: `https://www.vevo.sk/n/zatrhavanie-textilu-preco-vznikaju-vytiahnute-ocka-a-ako-im-predchadzat`
  - `2348`: `https://www.vevo.sk/n/pocet-niti-pri-oblieckach-co-znamena-thread-count-a-co-o-kvalite-nehovori`
- Link preflight checked `42` target and outgoing destinations with zero failures. Each future target slug returned exact `404`; all existing VEVO articles, the product, category, and primary or authoritative technical sources returned the expected successful response. One older flannel URL was found to return `404` during research and was excluded rather than published as a broken internal link.
- Prepared article quality:
  - visible word counts `2555`, `2578`, `2460`, and `2484`;
  - each article has `23` H2 headings, `2` tables, `11` styled blocks, a concrete product card, a category card, and six reader-facing FAQ questions;
  - zero fixed prices, zero forbidden internal wording, zero malformed HTML findings, and zero one-character paragraphs.
- Full pre-publication suite passed: `37/37` unit/regression tests, project audit `block_count=0`, and duplicate/content/depth/HTML guards all green.
- Disposable hidden MCP smoke post `2344` preserved its exact slug and rich HTML, remained public `404`, and was deleted; the follow-up admin lookup found zero matches. `exports/batch-42-2026-07-22-mcp-smoke.json` reports `all_ok=true`.
- MCP publication used hidden-first creation and explicit visibility confirmation. Every hidden record preserved its exact title, slug, short text, and `25292-26128` character rich HTML while the public URL returned `404`; every final record retained the same slug/body and returned `200`. `exports/batch-42-2026-07-22-mcp-publication.json` reports `record_count=4`, `public_ok_count=4`, and `all_ok=true`.
- Independent public verification checked exact titles and clean paths, minimum depth, quick-answer markers, both tables, styled blocks, product/category cards, external sources, escaped HTML, malformed links, and short-paragraph damage. All `38` unique outgoing destinations returned `200`; `exports/batch-42-2026-07-22-publication-verify.json` reports `all_ok=true`.
- Read-only responsive DOM QA on the first article confirmed the desktop article column and both tables fit their `890px` container. At `390x844`, the article column is `351px`, all 11 styled blocks and both buttons remain contained, and both `680px` tables are safely scrollable inside `351px` wrappers with `overflow-x:auto`. Existing page-level overflow comes from the site's product carousel outside the article. Browser screenshot capture timed out twice, so the render conclusion is based on measured live DOM geometry plus independent public HTML verification. The temporary viewport override was reset and the verification tabs were closed.
- Final post-publication project check passed with `37/37` unit/regression tests, project audit `block_count=0`, live Blog catalog `record_count=820`, zero duplicate-title groups, zero bad slugs, and all content/depth/HTML gates green.
- Durable artifacts:
  - `content-plan/batch-42-textile-construction-and-durability-2026-07-22.md`;
  - `batches/batch-42-candidates-2026-07-22.txt`;
  - `imports/build_batch_42_textile_construction_and_durability.py`;
  - `imports/batch-42-2026-07-22-articles.json`;
  - `imports/verify_batch_42_public.py`;
  - corresponding duplicate, link, content, depth, HTML, MCP smoke, MCP publication, and public verification reports under `exports/`.

## Batch 43 - Published 2026-08-14

- Status: complete; all four articles were published through the repo-local slug-safe MCP/API workflow and independently verified on the public website.
- The eight-topic exploration was narrowed to four distinct C09A intents. The wrinkle-resistance candidate returned `review` and was excluded. The final four titles returned `ok` against `823` merged Blog, FAQ, glossary, and local records:
  - `Pevnosť textilu v ťahu a proti roztrhnutiu: čo skúšky hovoria o odolnosti`;
  - `Splývavosť textilu: prečo niektoré látky držia tvar a iné kopírujú postavu`;
  - `Tepelný odpor textilu: prečo niektoré vrstvy hrejú viac pri rovnakej hrúbke`;
  - `Ochrana textilu pred UV žiarením: čo znamená UPF a čo ju znižuje`.
- Prepared quality: visible word counts `2612`, `2621`, `2668`, and `2713`; every article has `26` H2 headings, `2` responsive tables, `12` styled blocks, a product card, a category card, six reader-facing FAQ questions, and zero one-character paragraphs.
- Link preflight checked `37` target and outgoing destinations with zero failures. Future target slugs return exact `404`; VEVO article, product, category, AATCC, ASTM, WHO, UK government, and GINETEX links return `200`. ISO standard pages return their documented automated-client `403`, accepted only for `www.iso.org` after the four exact pages were independently opened and verified as valid current standard pages.
- Public wording and HTML safety guards are clean: zero fixed prices, zero forbidden workflow terminology, zero escaped HTML, and zero malformed-link findings.
- Full pre-publication project check passed with `38/38` tests, audit `block_count=0`, and the live Blog catalog at `820` public RSS records with zero duplicate-title groups and zero invalid slugs.
- The read-only MCP preflight found all four exact titles and slugs free in the `570`-record admin Blog catalog. Disposable hidden smoke post `3165` preserved its explicit slug and HTML, returned public `404` while hidden, and was deleted with zero admin matches and public `404` afterward.
- Live posts and canonical public URLs:
  - post `3166`: `https://www.vevo.sk/n/pevnost-textilu-v-tahu-a-proti-roztrhnutiu-co-skusky-hovoria-o-odolnosti`;
  - post `3167`: `https://www.vevo.sk/n/splyvavost-textilu-preco-niektore-latky-drzia-tvar-a-ine-kopiruju-postavu`;
  - post `3168`: `https://www.vevo.sk/n/tepelny-odpor-textilu-preco-niektore-vrstvy-hreju-viac-pri-rovnakej-hrubke`;
  - post `3169`: `https://www.vevo.sk/n/ochrana-textilu-pred-uv-ziarenim-co-znamena-upf-a-co-ju-znizuje`.
- Independent public verification passed `4/4` exact URLs with HTTP `200`. Public article segments contain `2614`, `2623`, `2670`, and `2715` visible words; each has `26` H2 headings, `2/2` responsive table wrappers, `12` styled blocks, two styled product/category action buttons, and zero short paragraphs. All `33` unique outgoing destinations passed; four exact ISO pages returned the documented automated-client `403` accepted only for `www.iso.org`.
- Public verification found zero fixed prices, forbidden internal wording, escaped HTML, malformed links, redirecting article slugs, or missing product/category/source links.
- Durable artifacts:
  - `content-plan/batch-43-textile-performance-2026-08-14.md`;
  - `batches/batch-43-candidate-scan-2026-08-14.txt` and `batches/batch-43-candidates-2026-08-14.txt`;
  - `imports/build_batch_43_textile_performance.py` and `imports/batch-43-2026-08-14-articles.json`;
  - `imports/verify_batch_43_public.py`;
  - corresponding candidate-scan, duplicate, link, wording, depth, HTML, MCP smoke, MCP publication, and independent public verification reports under `exports/`.

## Batch 44 Fabric Constructions - Prepared 2026-08-20

- Status: four articles are prepared and all pre-publication gates pass; no live record has been created at this checkpoint.
- A twelve-topic exploratory scan was narrowed to four distinct construction-level intents. All final titles and exact slugs returned `ok` against `827` merged Blog, FAQ, glossary, and local records. The live catalog has zero duplicate-title groups and zero invalid slugs:
  - `Čo je jersey úplet: pružnosť, krútenie švov a správne pranie`;
  - `Čo je popelín: hladká košeľová tkanina, vlastnosti a starostlivosť`;
  - `Čo je perkál: hustá tkanina na obliečky, vlastnosti a pranie`;
  - `Čo je ripstop: mriežkovaná tkanina, odolnosť a pranie outdoorového oblečenia`.
- Prepared quality: visible word counts `2985`, `2831`, `3299`, and `3348`; H2 counts `27`, `27`, `30`, and `31`; every article has two responsive tables, twelve styled blocks, a concrete product card, a category card, at least six reader-facing FAQ questions, and zero short or one-character paragraphs.
- Link preflight checked all future targets and outgoing destinations with zero failures. Future article slugs return exact `404`; existing VEVO article, product, category, ASTM, AATCC, CottonWorks, DLA, and GINETEX links return `200`. Exact ISO pages return their documented automated-client `403`; the official EUR-Lex consolidated regulation returns an asynchronous `202` to the automated checker. Those responses are accepted only for the exact authoritative hosts after the pages were independently identified as valid sources.
- Public wording, depth, and HTML safety guards pass with zero fixed prices, forbidden workflow wording, escaped HTML, malformed links, or structural failures. The complete project check passed `38/38` tests with project audit `block_count=0`.
- Durable prepared artifacts:
  - `content-plan/batch-44-fabric-constructions-2026-08-20.md`;
  - `batches/batch-44-candidate-scan-2026-08-20.txt` and `batches/batch-44-candidates-2026-08-20.txt`;
  - `imports/build_batch_44_fabric_constructions.py` and `imports/batch-44-2026-08-20-articles.json`;
  - corresponding candidate-scan, duplicate, link, wording, depth, and HTML reports under `exports/`.
- Next exact step: run the repo-local VEVO content MCP/API read-only preflight, disposable hidden smoke, immediate duplicate recheck, hidden-first publication, explicit visibility confirmation, and independent public verification. Do not use the legacy remote add/update tools.

## Known Issues

- The initial VEVO snapshot migration and batch 43 work are isolated on `codex/vevo-content-batch-43`; they are not part of `main` until the branch is reviewed and merged through a pull request.
- Legacy remote VEVO MCP publishing was rechecked on `2026-07-08`: its `add_news_post`/`update_news_post` tools expose only `title`, `short`, `long`, `visible`, and `position`. They remain unsafe because hidden test post `2296` changed link to `111`, and batch 35 generated repeated-`1` slugs for posts `2298-2307`. Use only repo-local `biznisweb-vevo-content`, which preserves explicit `link` and passed the hidden/public verification on `2026-07-14`.
- Batch 34 is complete; no batch 34 publication blocker remains.
- Batch 35 is complete; its bad direct-MCP-created posts were deleted and the final clean public URLs are verified. Do not recreate any batch 35 slug.
- Batch 36 is complete; all 5 bedding-care posts are live with clean slugs and verified rich HTML. Do not recreate any batch 36 slug.
- Batch 37 is complete; post IDs `2324-2328` are live with exact clean slugs and verified rich HTML. Do not recreate any batch 37 slug.
- Batch 38 is complete; post IDs `2329-2333` are live with exact clean slugs and verified rich HTML. Do not recreate any batch 38 slug.
- Batch 40 is complete; post IDs `2335-2338` are live with exact clean slugs and verified rich HTML. Do not recreate any batch 40 title, intent, or slug.
- Batch 41 is complete; post IDs `2340-2343` are live with exact clean slugs and verified rich HTML. Do not recreate any batch 41 title, intent, or slug.
- Batch 42 is complete; post IDs `2345-2348` are live with exact clean slugs and verified rich HTML. Do not recreate any batch 42 title, intent, or slug.
- Batch 43 is complete; post IDs `3166-3169` are live with exact clean slugs and verified rich HTML. Do not recreate any batch 43 title, intent, or slug.
- VEVO admin browser automation can be unstable on long loops. Use short stepwise source-mode saves, verify public URLs after each small block, and avoid relying on generic hidden-field selectors when old ExtJS form instances remain in the DOM.
- The VEVO admin browser session was recovered on `2026-07-06` and used to finish batch 33 publication; no batch 33 publication blocker remains.
- Batch 32 robot-vacuum batch is now published and verified; no batch 32 publication blocker remains.
- One batch 25 duplicate mapping for laundry symbols remains HTTP 404 by design/cleanup; avoid publishing another standalone laundry-symbol article and use the existing canonical URL.

## Next Exact Step

For the next VEVO work:

1. Select a small batch 44 from an unfilled content-plan cluster and run the duplicate guard against Blog block `765`, FAQ block `774`, glossary block `1905`, and all local prepared batches before drafting.
2. Keep `codex/vevo-content-batch-43` rebased on `origin/main`, push every durable checkpoint, and merge only through a pull request. Never return VEVO content work to the unrelated local-only `opan-claw` history.
3. Resolve the older batch 31 hidden posts `2278` and `2279` separately, preserving their prepared titles and slugs and checking first that no public/canonical replacement already exists.
4. Never use legacy remote `biznisweb-add_news_post`/`biznisweb-update_news_post` as the final VEVO new-article path. The approved API path is repo-local `biznisweb-vevo-content` with hidden-first creation, explicit slug, readback, resumable report, and independent public verification.

## Handoff Template

Date:
Repo:
Branch:
Project: VEVO_CONTENT
What changed:
What is verified:
Known issues:
Next exact step:
