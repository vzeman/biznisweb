# Podklad pre opravu natívneho spracovania Stripe vo FLOX-e

Stav: pripravený technický podklad, neodoslaný dodávateľovi. Neobsahuje údaje
zákazníkov, čísla objednávok ani tajné kľúče. Konkrétna časová os a doklady sú
uložené v privátnych auditných záznamoch.

## Pozorovaný problém

Pri viacerých platobných pokusoch k jednej objednávke môže udalosť o zrušení
alebo vypršaní staršieho pokusu prepísať stav objednávky po úspešnej platbe
iným pokusom a po odoslaní zásielky. Samotná existencia faktúry nenahrádza
potvrdenie platby ani odoslania.

Administrácia pri overení 2026-09-13 ponúkala priradenie pevných stavov pre
jednotlivé výsledky platby. Pri zrušení a vypršaní nebola vo výbere viditeľná
možnosť bez zmeny stavu. Overenie nepreukázalo podporované podmienené potlačenie
starého pokusu; nevyvodzuje sa z neho absencia skrytej funkcie dodávateľa.

## Požadované správanie

1. Spojiť udalosť s konkrétnym platobným pokusom a objednávkou. Pred zmenou
   preveriť aktuálny výsledok všetkých relevantných pokusov, zaplatenú sumu
   a aktuálny stav objednávky.
2. Neaplikovať zrušenie alebo vypršanie staršieho neúspešného pokusu na
   objednávku s inou potvrdenou platbou. Zachovať overené odoslanie zásielky.
3. Evidovať spracované udalosti a zabrániť opakovanému zápisu aj opakovanému
   zákazníckemu e-mailu. Poradie prijatia alebo samotný čas udalosti nestačí.
4. Skutočné refundácie posudzovať samostatne; čiastočná refundácia nie je
   dôvodom automaticky zrušiť celú objednávku.
5. Overiť dva platobné pokusy v testovacom prostredí, neskoré doručenie starej
   negatívnej udalosti, opakované doručenie a súbežné spracovanie. Výsledná
   objednávka má zachovať potvrdenú platbu a odoslanie bez duplicitnej správy.

## Rozsah lokálnej nápravy

AWS automatizácie kontrolujú čerstvé platby, zásielky a dobropisy, zapisujú
operácie pod spoločným zámkom a neopakujú neisté výsledky. Následná oprava stavu
v AWS však nezabraňuje pôvodnému zápisu natívneho webhooku. Je potrebná oprava
na strane jeho správcu alebo dodávateľom potvrdené podmienené nastavenie.
Zmena pevného cieľového stavu ani vypnutie udalostí bez overenia následkov
nebola nasadená ako náhrada tejto podmienky.

[Stripe dokumentuje nezaručené poradie a opakované doručenie udalostí](https://docs.stripe.com/webhooks#event-ordering).
[BiznisWeb dokumentuje vlastný webhook a jeho nastavenie](https://www.biznisweb.sk/a/1472/medzinarodna-platobna-brana-stripe).
