# Podklad pre opravu natívneho spracovania Stripe a GoPay vo FLOX-e

Stav: pripravený technický podklad, neodoslaný dodávateľovi. Neobsahuje údaje
zákazníkov, čísla objednávok ani tajné kľúče. Konkrétna časová os a doklady sú
uložené v privátnych auditných záznamoch.

Aktuálny smer podľa používateľa: ROY už používa Stripe; VEVO práve prechádza
z GoPay na Stripe. Pravidlá preto musia chrániť potvrdenú úhradu nezávisle od
brány. Historické GoPay pokusy sa môžu dobiehať aj počas migrácie. Pri tomto
audite sa nastavenia brán, mapovania stavov ani webhooky nemenili.

## Pozorovaný problém

Pri viacerých platobných pokusoch k jednej objednávke môže udalosť o zrušení
alebo vypršaní staršieho pokusu prepísať stav objednávky po úspešnej platbe
iným pokusom a po odoslaní zásielky. Samotná existencia faktúry nenahrádza
potvrdenie platby ani odoslania.

Rovnaký konflikt nastáva pri platbe prijatej mimo brány: zákazník pôvodne zvolí
online platbu, nakoniec pošle peniaze priamo bankovým prevodom a obsluha potvrdí
úhradu. Zmena spôsobu platby na bankový prevod ani ručný návrat do odoslaného
stavu podľa pozorovania používateľa nezastavia neskorý zápis pôvodnej brány.
Expirácia jej pokusu nevypovedá o prijatí samostatného bankového prevodu.

Administrácia pri overení 2026-09-13 ponúkala priradenie pevných stavov pre
jednotlivé výsledky platby. Pri zrušení a vypršaní nebola vo výbere viditeľná
možnosť bez zmeny stavu. Overenie nepreukázalo podporované podmienené potlačenie
starého pokusu; nevyvodzuje sa z neho absencia skrytej funkcie dodávateľa.

## Požadované správanie

1. Spojiť udalosť s konkrétnym platobným pokusom a objednávkou. Pred zmenou
   preveriť aktuálny výsledok všetkých relevantných pokusov, zaplatenú sumu
   a aktuálny stav objednávky.
2. Neaplikovať zrušenie alebo vypršanie neúspešného pokusu na objednávku s inou
   potvrdenou platbou, vrátane ručne spárovaného priameho bankového prevodu.
   Zachovať overené odoslanie zásielky. Zmena spôsobu úhrady sama osebe nie je
   dôkaz platby; potrebný je oddelený a auditovateľný záznam potvrdenia úhrady.
3. Evidovať spracované udalosti a zabrániť opakovanému zápisu aj opakovanému
   zákazníckemu e-mailu. Poradie prijatia alebo samotný čas udalosti nestačí.
4. Skutočné vrátenie a refundácie posudzovať samostatne. Predchádzajúca úhrada,
   odoslanie a faktúra nesmú blokovať preukázaný úplný storno dobropis.
   Dobropis musí patriť k tej istej objednávke a číslu faktúry, byť uzavretý
   a sám nesmie byť stornovaný; rozpracovaný alebo nejasný doklad nestačí.
   Čiastočná refundácia alebo vrátenie časti tovaru nie sú dôvodom automaticky
   zrušiť celú objednávku. Neznámy alebo rozporný rozsah patrí na kontrolu.
5. Overiť dva platobné pokusy v testovacom prostredí, neskoré doručenie starej
   negatívnej udalosti, opakované doručenie a súbežné spracovanie. Výsledná
   objednávka má zachovať potvrdenú platbu a odoslanie bez duplicitnej správy.

## Konkrétne akceptačné scenáre

| Situácia | Očakávaný výsledok |
| --- | --- |
| Online pokus vyprší po potvrdenej plnej úhrade priamym bankovým prevodom | Zaevidovať výsledok pokusu; objednávku nevrátiť do nezaplateného stavu. |
| Obsluha zároveň zmení spôsob platby na bankový prevod a označí objednávku za odoslanú | Neskorá udalosť starého pokusu neprepíše potvrdenú úhradu ani odoslanie. |
| Iný pokus k tej istej objednávke už uspel | Neúspech starého pokusu neprebije platnú úplnú úhradu. |
| Po úhrade a odoslaní vznikne overený úplný storno dobropis | Samostatné pravidlo dovolí storno celej objednávky. |
| Vznikne iba čiastočný dobropis alebo sa vráti časť položiek | Nezrušiť automaticky celú objednávku. |
| Dobropis je rozpracovaný, stornovaný alebo nejednoznačne priradený | Doklad nepoužiť ako dôkaz úplného storna objednávky; vyžiadať preverenie. |
| Príde opakovaná alebo súbežná udalosť | Žiadna duplicitná zmena ani zákaznícky e-mail. |
| O úhrade existuje iba názov spôsobu platby, faktúra alebo neoverený historický stav | Nepredstierať dôkaz úhrady; zachovať prípad na overenie. |

Dodávateľ má potvrdiť podporovaný spôsob potlačenia neplatného prechodu ešte
pred zápisom objednávky, prípadne opraviť spracovanie callbacku. Pevné globálne
mapovanie negatívnej udalosti na iný objednávkový stav túto podmienku nenahrádza.

## Rozsah lokálnej nápravy

AWS automatizácie kontrolujú čerstvé platby, zásielky a dobropisy, zapisujú
operácie pod spoločným zámkom a neopakujú neisté výsledky. Následná oprava stavu
v AWS však nezabraňuje pôvodnému zápisu natívneho webhooku. Je potrebná oprava
na strane jeho správcu alebo dodávateľom potvrdené podmienené nastavenie.
Zmena pevného cieľového stavu ani vypnutie udalostí bez overenia následkov
nebola nasadená ako náhrada tejto podmienky.

[Stripe dokumentuje nezaručené poradie a opakované doručenie udalostí](https://docs.stripe.com/webhooks#event-ordering).
[BiznisWeb dokumentuje vlastný webhook a jeho nastavenie](https://www.biznisweb.sk/a/1472/medzinarodna-platobna-brana-stripe).
[GoPay opisuje kontrolu konkrétneho platobného pokusu po notifikácii](https://help.gopay.com/cs/tema/integrace-platebni-brany/integrace-nova-platebni-brany/overeni-stavu-platby-v-systemu-platebni-brany-prostrednictvim-rest-api).
[GoPay rozlišuje konečné stavy pokusu a doručenie notifikácie](https://help.gopay.com/cs/tema/mam-platebni-branu/chci-pouzivat-gopay-obchodni-ucet/prehled-plateb-a-pohybu/kde-mohu-sledovat-prijate-platby).
[BiznisWeb opisuje samostatné evidovanie úhrady faktúry](https://www.biznisweb.sk/a/90/sprava-faktur-fakturacia).
