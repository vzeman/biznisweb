# Audit automatizácií objednávok ROY / VEVO

Date: 2026-09-09
Repo: `vzeman/biznisweb`
Branch: `codex/roy-order-automation-audit-20260909`
Audited source: `c66b0cab05629bd9a28825b1aee87c4c216c8992` (`origin/main` at audit start).

Privacy: tento repozitár je verejný. `CASE-A`, `CASE-B`, `INVOICE-A`, `INVOICE-B` a `ADMIN-A` sú zástupné označenia, nie skutočné identifikátory. Prvý prípad zodpovedá používateľom nahlásenej chýbajúcej faktúre; druhý podozrivým stavom. Presné čísla, sumy, časové značky udalostí a zákaznícke údaje sa do Git histórie neukladajú; pôvodné dôkazy zostávajú v prihlásenej administrácii. Relatívne časy zachovávajú technický mechanizmus incidentu. Pred súkromným odoslaním návrhu správy podpore používateľ doplní skutočné údaje.

## Stav a rozsah

Audit potvrdil chybu výberu fakturovaných objednávok a reálny návrat už odoslanej objednávky do platobných stavov. Oprava ani produkčný deploy neprebehli. Podľa používateľovho pravidla pri nájdení chyby zastavujeme produkčné zásahy a odovzdávame dôkazy a presný ďalší krok.

Overené bolo aktuálne ROY admin UI, história dvoch objednávok, spoločný zdrojový kód, konfigurácia ROY/VEVO a vybrané existujúce GitHub Actions behy. Statické zistenia o spoločnom kóde sa týkajú aj VEVO; samostatná história VEVO objednávok ani iné e-shopy neboli auditované.

AWS CLI v tejto relácii vracia `NoCredentials`. Aktuálne ECS tasky, ich IP/image digest, prirodzené behy, alarmy, DLQ a CloudWatch log konkrétnych incidentov preto nie sú priamo overené. Historická úspešná smoke skúška nepreukazuje dnešnú nepretržitú prevádzku. Nebola spustená žiadna produkčná automatizácia, vystavená faktúra, zmenený stav ani odoslaný email.

## Objednávka CASE-A: oneskorené odoslanie vypadlo z výberu

Zdroj: prihlásená administrácia `https://roy.flox.sk/erp/main/orders`, vyhľadanie presného čísla, detail / História. ROY.SK, slovenská objednávka, EUR, dobierka, suma s DPH [suma nezverejnená]. Relatívne časy vychádzajú z administrácie.

| Relatívny čas | Overená udalosť |
| --- | --- |
| Deň 0 | Objednávka vytvorená. |
| Deň 14 | `Čaká na vybavenie` → `Odoslaná`. |
| Deň 20, pred auditom | Účet `ADMIN-A` vytvoril faktúru `INVOICE-A`. |
| Krátko po vytvorení faktúry | Účet `ADMIN-A` odoslal fakturačný email. |

Pri kontrole už faktúra existovala. Audit ju nevytvoril; neopakovať vystavenie ani odoslanie.

Konfigurácia `projects/roy/settings.json:97` má `lookback_days=7`. `generate_invoices.py:913` triedi podľa `pur_date`, na `:947` ukončí stránkovanie pri starších nákupoch a na `:1083` filtruje opäť podľa dátumu nákupu. Na dátum odoslania ani načítané `last_change` sa tento výber neviaže. Pri odoslaní 14 dní po nákupe už nákup nemôže patriť do sedemdňového výberu. Izolovaná reprodukcia metódy s falošným API potvrdila vyradenie starého nákupu s novým `last_change`. Log pôvodného produkčného behu nebol dostupný; dátumy z UI a výberový algoritmus sú overené priamo.

Trvalá oprava musí sledovať nespracované oprávnené objednávky nezávisle od veku nákupu: evidovať čakajúce objednávky, spracúvať zmeny a pravidelne kontrolovať úplnosť. Samotné zväčšenie okna iba odsúva rovnaký problém. Pred spätným vystavením treba znovu overiť stav, platbu, existujúce faktúry a dobropisy a deduplikovať podľa e-shopu + objednávky.

## Objednávka CASE-B: stratený stav odoslania

Zdroj: rovnaká ROY administrácia a úplne zobrazená história presného čísla. Online platba, EUR, suma [suma nezverejnená]. Aktuálny stav pri kontrole: `Platba online - zaplatené`; faktúra `INVOICE-B`.

| Relatívny čas | Overená udalosť |
| --- | --- |
| Deň 0, vytvorenie | Objednávka vytvorená. |
| Približne minútu po vytvorení | `Čaká na vybavenie` → `Stripe - unpaid`. |
| Približne 24 minút po vytvorení | Pod účtom `ADMIN-A`: `Stripe - unpaid` → `Nezaplatená - zrušená objednávka`; odoslaný email o zrušení. |
| Krátko po zrušení | Stripe platba [suma nezverejnená] akceptovaná; úhrada predfaktúry; zrušená → zaplatené; vytvorená faktúra `INVOICE-B`. |
| Nasledujúci deň ráno | `Platba online - zaplatené` → `Odoslaná`. |
| Približne 24 hodín po vytvorení | `Odoslaná` → `Stripe - cancelled` → `Stripe - expired`. |
| Približne 7 minút po expirácii | `Stripe - expired` → `Platba online - zaplatené`; ďalší email zákazníkovi. |

Platba prišla niekoľko sekúnd po zrušení označenom účtom ADMIN-A. Záznam účtu sám osebe nedokazuje manuálne kliknutie ani konkrétny externý nástroj. Táto zmena po 24 minútach nezodpovedá aktuálnemu ROY nočnému pravidlu rušenia po 14 dňoch o 02:10.

Pokles z `Odoslaná` do Stripe stavov je potvrdený incident. V auditovanom repozitári nebol nájdený writer nastavujúci tieto Stripe cieľové stavy. Čas približne 24 hodín po vytvorení objednávky zodpovedá predvolenej expirácii Checkout Session. **Staršia nedokončená platobná relácia je hypotéza, nie identifikovaný event.** Potrebné sú presné Stripe event/session identifikátory a FLOX webhook log.

Návrat krátko po expirácii zodpovedá plánovanému invoice behu v príslušnom 15-minútovom intervale a jeho pravidlu „existuje konečná faktúra + chybný platobný stav → zaplatené“. Bez CloudWatch záznamu `Reconciled order CASE-B...` ho nemožno priradiť konkrétnemu tasku s istotou. Pravidlo na `generate_invoices.py:344-358` nečíta históriu odoslania ani dôkaz úhrady. Predbežná kontrola síce preskočí objednávku, ktorá je **práve** `Odoslaná`, ale neochráni pred stratou tohto stavu po prepísaní externým webhookom.

Trvalé riešenie musí zabrániť tomu, aby starý/neúspešný platobný pokus prepísal potvrdenú platbu alebo vybavenie iného úspešného pokusu. Obnova `Odoslaná` musí vychádzať z dôveryhodného dokladu odoslania/histórie, nie zo samotnej faktúry. Nejednoznačný prípad má zastaviť zmenu a vyvolať upozornenie. Korekčné zmeny nesmú automaticky rozosielať zákazníkovi opakované protichodné emaily.

## Ďalšie chyby a riziká spoločného kódu

Riadky nižšie patria auditovanému commitu; zistenie v zdroji nie je dôkazom výskytu pri každej produkčnej objednávke.

| Priorita | Zistenie a dôkaz | Potrebné riešenie |
| --- | --- | --- |
| P1 | Chyba načítania sa môže zmeniť na normálny návrat prázdneho/neúplného zoznamu (`generate_invoices.py:953-1089`). Izolovaný HTTP 429 scenár vrátil `[]` bez výnimky. | Obmedzené opakovanie čítania, explicitná úplnosť scanu, chybový výsledok pri neúplnosti. |
| P1 | `price_elements` chyba s existujúcim kurzorom opakuje rovnakú stránku cez `continue` (`:992-998`). Izolovaná skúška bola zámerne ukončená po troch opakovaniach. | Detekcia nepostupujúceho kurzora, časový/page limit, kontrolované zlyhanie. |
| P1 | Invoice aj cancellation runner publikujú `RunSucceeded` pred vyhodnotením počítadiel chýb; neskoršie výnimky obídu publikovanie `RunFailed` (`invoice_runner.py:143,171,187-199`; `unpaid_order_cancellation_runner.py:101-120`). | Jediný konečný výsledok po všetkých kontrolách; oddeliť dry-run metriky od produkcie. |
| P1 | Existencia faktúry sa používa ako podmienka presunu do zaplateného stavu bez overenia úhrady (`generate_invoices.py:344-358`; `unpaid_order_cancellation.py:432-443`). | Oddeliť existenciu dokladu, potvrdenie platby a stav vybavenia; spracovať refundácie/dobropisy osobitne. |
| P1 | Pred samotným vytvorením faktúry chýba čerstvé načítanie stavu a faktúr aj zámok proti súbehu (`generate_invoices.py:1561,1576-1577`). Produkčná duplicita týmto auditom nebola potvrdená. | Ochrana proti súbehu podľa e-shopu/objednávky, nový readback a overenie výsledku; neopakovať naslepo neistú mutáciu. |
| P2 | Neúspešný fakturačný email nemá trvalý retry: ďalší beh vylúči objednávku s existujúcou faktúrou (`:1134-1147`, `:1288-1294`). HTTP 200 s prihlasovacím HTML sa navyše pokladá za úspech (`:1346-1350`), izolovane reprodukované. | Samostatná evidencia odoslania podľa ID faktúry, jednoznačný výsledok, deduplikovaný retry a upozornenie pri neistom výsledku. |
| P2 | Cancellation scan má spoločný limit 50 strán po 30 a vie prehltnúť chybu neskoršej stránky, ďalej zmeniť čiastočných kandidátov a vrátiť úspech (`unpaid_order_cancellation.py:563-621,688-693`). | Dokázateľná úplnosť alebo trvalý checkpoint, upozornenie na nespracovaný zvyšok. |
| Review | Creditnote guard môže považovať aj čiastočný dobropis za dôvod plného storna; nerobí bezprostredný readback výsledku (`creditnote_storno_guard.py:280-325`). | Najprv potvrdiť pravidlo pre čiastočné dobropisy, potom úzka implementácia a readback. |

Creditnote guard navyše pred mutáciou neobnovuje aktuálnu spôsobilosť objednávky (`creditnote_storno_guard.py:280-325`). Cancellation/recovery môže prázdny alebo nesprávny návrat mutácie počítať ako úspech (`unpaid_order_cancellation.py:626-631,832-840`). Obe cesty potrebujú čerstvú kontrolu pred zmenou a overenie vráteného objektu aj cieľového stavu; ide o technické chyby nezávislé od pravidla čiastočných dobropisov.

Ďalšia kontrola zistila retry transport aj na klientoch vykonávajúcich mutácie (`generate_invoices.py:578-584`, `unpaid_order_cancellation.py:357-363`, `export_orders.py:795-811`). Lokálne dostupná verzia `gql 4.0.0` opakuje aj POST pri vybraných HTTP chybách. Produkčná verzia knižnice nebola overená. Neistý výsledok mutácie sa nesmie slepo opakovať: najprv readback, potom rozhodnutie. Creditnote guard navyše eviduje chyby načítania v `audit_errors`, ale nemusia zvýšiť chybový výsledok; aj túto neúplnosť musí monitoring vidieť.

Pozor pri oprave dôkazu platby: PROJECT_STATE už opisuje manuálne spárované prevody, pri ktorých API faktúry ukazovalo `paid=false`. Náhradou preto nemôže byť jednoduché pravidlo `paid=true`; treba zjednotiť overené dôkazy úhrady, sumu, refundácie a spôsob platby. Pri objednávke CASE-B je prijatie konkrétnej Stripe platby doložené históriou.

Aktuálne vytváranie faktúr nie je obmedzené výhradne na dobierku. Filtruje `Odoslaná`, kladnú sumu a neprítomnosť faktúry; platobnú metódu invoice query nenačítava. Treba výslovne zadefinovať pravidlá pre dobierku, prevod, online platbu, nulové objednávky a cudzie jazykové verzie.

## Prevádzka a overené hranice

Konfigurácia v Gite:

- ROY fakturácia: každých 15 minút od 06:05 do 23:50, doplnkový beh 23:59; rodina `roy-invoice-daily`, služba/plán `roy-daily-invoice-generation`.
- VEVO fakturácia: každých 15 minút od 06:00 do 23:45, doplnkový beh 23:58; rodina `vevo-invoice-daily`.
- ROY rušenie nezaplatených: 02:10, vek 14 dní, rodina/služba `roy-unpaid-order-cancellation`; recovery z `Stripe - expired` pri existujúcej faktúre smeruje na zaplatené.
- Creditnote guard je súčasťou denného reportovacieho runnera; treba zahrnúť tento ďalší writer do spoločného modelu stavov.

Standalone fakturácia teda nemá interval pokrývajúci všetkých 24 hodín. Iné nočné joby nemožno zamieňať za doloženú 24/7 fakturačnú službu.

Existujúce CI dôkazy:

- [Production Invoice Smoke 33866982720](https://github.com/vzeman/biznisweb/actions/runs/33866982720), 4. september: úspešný manuálny **dry-run** oboch e-shopov. PROJECT_STATE dokumentuje Fargate (`instance-id=N/A`), ROY private IP `172.31.13.190`, `roy-daily-invoice-generation`, `/app`, localhost marker a digest `sha256:de5c1f91cc8e95fb17dbf8a29fe85272df84499976b0541a242f9591b8f0b768`. Je to historický dôkaz, nie dnešný hard-gate.
- [Unpaid deploy 33885896205](https://github.com/vzeman/biznisweb/actions/runs/33885896205), 4. september: neúspech pri čakaní na presný ECR image. Súvisiaci [build 33885896196](https://github.com/vzeman/biznisweb/actions/runs/33885896196) zlyhal v teste dashboard autentifikácie (`200 != 409`); tento pokus neprešiel do nasadenia. Nejde o dôkaz, že už bežiaca staršia automatizácia prestala fungovať.

Aktuálny zoznam prirodzených behov, posledný úplný úspech, čas trvania, súbehy, fakturačný backlog a funkčnosť upozornení zostávajú neoverené. Bez toho nesľubovať stabilnú bezchybnú prevádzku.

Ďalšie prevádzkové nálezy:

- `.github/workflows/deploy-unpaid-order-cancellation.yml:294-313` zapína/mení plán ešte pred host smoke skúškou. Nasadenie musí najprv overiť kandidáta a až potom zmeniť aktívny plán, s rollbackom pri neúspechu.
- Staršia projektová dokumentácia uvádza invoice task definitions s pohyblivým tagom `vevo-reporting:latest`; build tento tag aktualizuje. To môže meniť budúci invoice beh spolu s nesúvisiacimi buildmi. Dnešné task definitions treba načítať; historický zápis nie je dôkazom, že tag stále používajú. Cieľom sú nemenné image digesty a samostatná propagácia každej služby po overení.
- V auditovaných definíciách nebol nájdený reprodukovateľný súbor invoice/cancellation alarmov a dedikovaných DLQ. Unpaid deploy preberá Target zo zdrojového plánu bez explicitného overenia týchto nastavení. Existenciu alebo absenciu alarmov v AWS nemožno z toho vyvodiť.
- Chýba trvalý zámok proti súbehu; ROY beh 23:50 a 23:59 sweep sa môžu prekrývať pri trvaní nad deväť minút. Historicky dokumentované súbežné smoke skúšky už narazili na HTTP 429. Opakovanie pokusov musí mať spoločné limity pre daný e-shop.
- YAML unpaid deploy workflow spúšťa sám seba po zmene, ale tento súbor nie je medzi path triggermi image buildu. Ide o samostatnú medzeru, **nie** o príčinu konkrétneho buildu zlyhaného na teste autentifikácie vyššie.

## Návrh správy podpore BiznisWebu — neodoslaný

Predmet: ROY — prepísanie zaplatenej a odoslanej objednávky Stripe expiráciou

Na ROY objednávke CASE-B bola krátko po vytvorení objednávky prijatá Stripe platba [suma nezverejnená] a vytvorená faktúra INVOICE-B. Nasledujúci deň ráno prešla do Odoslaná. V ten istý deň približne 24 hodín po vytvorení sa však zmenila z Odoslaná na Stripe - cancelled a následne Stripe - expired. Prosíme overiť presné event/session identifikátory a spracovanie webhooku: mohla expirácia staršieho nezaplateného pokusu prepísať zaplatenú a už odoslanú objednávku? Potrebujeme ochranu, ktorá viaže udalosť ku konkrétnemu platobnému pokusu a nedovolí takýto návrat stavu. Naša následná fakturačná reconciliation môže stav vrátiť na zaplatené, ale tým sa stráca informácia o odoslaní. Prosíme aj o potvrdenie bezpečného spôsobu čítania histórie/odoslania cez API na overenie nápravy. Správa nebola odoslaná.

## Next exact step

1. Získať cez existujúci spravovaný AWS/GitHub prístup sanitizovaný read-only výpis aktuálnych invoice/cancellation plánov a prirodzených behov vrátane časového intervalu incidentu podľa súkromnej administrácie. Potvrdiť account, instance-id (`N/A` pre Fargate), presný task/private IP, service, `/app`, image digest; nepoužiť staré IP ako dnešný dôkaz.
2. Dohľadať FLOX/Stripe udalosti pre CASE-B a dôkaz odoslania. Bez potvrdenej identity udalosti nemenovať konkrétny webhook ako preukázanú príčinu. Prípadnú správu podpore odošle používateľ.
3. Po vyriešení incidentného stopu implementovať cez úzke PR: úplný fakturačný backlog, bezpečné retry čítania, ochranu proti súbehu a neistým mutáciám, pravdivé metriky, zachovanie platby/odoslania a oddelené odosielanie emailov. Overiť scenáre starého nákupu, 429, opakovaného kurzora, timeoutu po úspešnej mutácii, paralelného behu, starej Stripe udalosti, čiastočnej platby/dobropisu a chyby emailu.
4. Najprv kompletný read-only audit chýbajúcich faktúr a stavov. Vystavovanie starých dokladov musí rešpektovať správne dátumy/číslovanie a aktuálny stav; CASE-A už má faktúru. Žiadne hromadné zmeny z neúplného zoznamu.
5. Deploy iba z Git/CI s rollbackom, potom priamo host `curl localhost` + marker, až potom UI. Zaviesť 24/7 interval, nezávislé sledovanie posledného úplného úspechu/backlogu a upozornenia na zlyhanie, nie iba na spustenie tasku. Overiť prirodzený beh vrátane nočného intervalu.

## Zdroje a cleanup

- [BiznisWeb: objednávky a statusy](https://www.biznisweb.sk/a/59/nastavenie-objednavok), [fakturácia](https://www.biznisweb.sk/a/90/sprava-faktur-fakturacia), [platobné brány a faktúry](https://www.biznisweb.sk/a/1026/platobne-brany-vystavovanie-faktur-a-predfaktur): aktuálne otvorené pri audite. Faktúra a záznam úhrady sú samostatné údaje.
- [Stripe: expirácia Checkout Session](https://docs.stripe.com/payments/checkout/managing-limited-inventory): predvolene 24 hodín a udalosť `checkout.session.expired`; vysvetľuje hypotézu, nepotvrdzuje konkrétnu ROY udalosť.
- Do Git zápisu nepatria zákaznícke mená, adresy, emaily, IP, presné order/invoice identifikátory, sumy ani secrets. Údaje z prvotného lokálneho návrhu boli pred prvým commitom anonymizované; surový UI export sa neukladal.
- Nebol spustený lokálny dev server, worker, watcher, tunel, Docker ani persistentný proces. Nie je čo ukončovať. Existujúce používateľské a produkčné procesy zostali nedotknuté.
