import importlib.util
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests
import xlwt


BASE = "https://www.vevo.sk"
BATCH_DATE = "2025-09-27"
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-25-2026-06-16-articles.json")
OUT_XLS = Path.home() / "AppData/Local/Temp/vevo-batch-25-laundry-science-clean-urls.xls"
HELPERS_PATH = Path("content/VEVO_CONTENT/imports/build_batch_21_material_guides.py")


spec = importlib.util.spec_from_file_location("batch21_helpers", HELPERS_PATH)
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)


ARTICLES = [
    {
        "title": "Ako čítať štítok na oblečení: materiál, symboly prania a správny program",
        "short": "Štítok na oblečení čítajte v poradí: zloženie materiálu, symboly prania, sušenia a žehlenia, potom praktický stav oblečenia. Správny program nevyberajte len podľa farby, ale aj podľa najcitlivejšej časti výrobku.",
        "keywords": "ako čítať štítok na oblečení, symboly prania na štítku, zloženie materiálu na štítku, aký program zvoliť, čo znamená štítok na tričku, ako prať podľa štítku",
        "quick_title": "Rýchly postup pri každom novom kúsku",
        "quick": [
            "<strong>Najprv pozrite zloženie.</strong> Bavlna, polyester, vlna, elastan, viskóza alebo membrána menia program aj sušenie.",
            "<strong>Potom čítajte symboly starostlivosti.</strong> Vanička rieši pranie, trojuholník bielenie, štvorec sušenie, žehlička teplotu žehlenia a kruh profesionálne čistenie.",
            "<strong>Vyberajte podľa najcitlivejšej časti.</strong> Ak je tričko bavlna s elastanom a potlačou, nestačí myslieť iba na bavlnu.",
            "<strong>Štítok nie je marketing.</strong> Je to praktický limit, ako ďaleko môžete ísť bez zbytočného rizika poškodenia.",
        ],
        "intro": [
            "Štítok na oblečení je malý, ale pri praní rozhoduje o veľa veciach: teplote, žmýkaní, bielení, sušičke, žehlení aj tom, či textil vôbec patrí do domácej práčky. Mnoho problémov vzniká preto, že ľudia triedia iba podľa farby a ignorujú zloženie materiálu, elastan, potlač, membránu alebo povrchovú úpravu.",
            "Ak hľadáte ako čítať štítok na oblečení, čo znamenajú pracie symboly alebo aký program zvoliť, najlepšie je začať jednoduchým poradím: materiál, symboly, konštrukcia výrobku, miera znečistenia. Tenký úplet, rifľovina, softshell a obliečky nemajú rovnaké potreby ani vtedy, keď majú podobnú farbu.",
            "Dôležitá je aj praktická realita. Štítok určuje maximálne bezpečné zaobchádzanie, ale nemusíte vždy prať na najvyššej povolenej teplote. Menej zašpinené oblečenie často potrebuje skôr rozumné dávkovanie, dobrý oplach a rýchle sušenie než agresívny program.",
        ],
        "rows": [
            ("1. Zloženie", "určuje citlivosť materiálu", "hľadajte vlnu, elastan, viskózu, membránu alebo zmesi"),
            ("2. Symbol prania", "limituje teplotu a typ procesu", "nižšia teplota je často bezpečnejšia, ak textil nie je silno špinavý"),
            ("3. Sušenie", "rozhoduje o zrážaní a tvare", "sušička môže byť väčšie riziko než samotné pranie"),
            ("4. Žehlenie a čistenie", "rieši povrch, potlač a profesionálne postupy", "pri neistote začnite nižšou teplotou alebo nežehlite potlač"),
        ],
        "workflow": [
            ("Bavlnené tričko s potlačou", "prať naruby, podľa štítku, nižšie otáčky", "potlač a úplet sú citlivejšie než samotná bavlna"),
            ("Legíny s elastanom", "šetrný program, bez horúcej sušičky", "pružnosť sa kazí teplom a agresívnym žmýkaním"),
            ("Softshell alebo membrána", "bez aviváže, podľa štítku, dobrý oplach", "film z prípravkov môže zhoršiť funkčné vlastnosti"),
            ("Obliečky", "zapnúť zipsy, nepreplniť bubon, úplne vysušiť", "objem potrebuje priestor na oplach a sušenie"),
        ],
        "mistakes": [
            "Triediť iba podľa farby a ignorovať zloženie materiálu.",
            "Prať všetko, čo vyzerá ako tričko, na rovnakom programe.",
            "Použiť aviváž aj tam, kde je funkčný materiál alebo uterák.",
            "Prehliadnuť symbol sušenia a potom riešiť zrazenie v sušičke.",
            "Zamieňať povolenú maximálnu teplotu za odporúčanie prať vždy horúco.",
        ],
        "expert": "Oficiálne systémy starostlivosti o textil pracujú so symbolmi, ktoré majú spotrebiteľovi ukázať hranice bezpečného ošetrovania. Podstatné je, že symboly sa vzťahujú na celý hotový výrobok, nielen na hlavné vlákno. Zipsy, gombíky, potlače, farbivá, výstuže a aplikácie môžu byť dôvodom, prečo štítok odporúča šetrnejší postup, než by ste čakali podľa samotného materiálu.",
        "sources": [
            ("GINETEX: Care symbols", "https://www.ginetex.net/gb/labelling/care-symbols.asp"),
            ("GINETEX: Labelling", "https://www.ginetex.net/GB/labelling/labelling.asp"),
            ("FTC: Complying with the Care Labeling Rule", "https://www.ftc.gov/business-guidance/resources/clothes-captioning-complying-care-labeling-rule"),
        ],
        "related": [
            ("Ako vybrať prací gél podľa typu bielizne", "/n/ako-vybrat-praci-gel-podla-typu-bielizne"),
            ("Čo je zmesový materiál a prečo sa správa inak", "/n/co-je-zmesovy-material-preco-sa-oblecenie-zraza-alebo-sprava-inak-nez-cakate"),
            ("Kedy nepoužívať aviváž", "/n/kedy-nepouzivat-avivaz-uteraky-sportove-oblecenie-softshell-aj-detska-bielizen"),
        ],
        "faq": [
            ("Čo je najdôležitejšie na štítku?", "Najprv zloženie a potom symboly prania, sušenia a žehlenia. Program vyberajte podľa najcitlivejšej časti výrobku."),
            ("Môžem prať na nižšej teplote, než povoľuje štítok?", "Áno, ak je textil bežne nosený a nie silno znečistený. Štítok často ukazuje maximálny bezpečný limit, nie povinnosť prať horúco."),
            ("Čo ak je štítok odstrihnutý?", "Postupujte opatrne podľa materiálu, farby a konštrukcie. Pri drahých alebo citlivých kusoch zvoľte ručné či profesionálne čistenie."),
        ],
    },
    {
        "title": "Symboly prania na štítku: čo znamená vanička, trojuholník, kruh, štvorec a žehlička",
        "short": "Symboly prania sú skratka pre bezpečnú starostlivosť. Vanička znamená pranie, trojuholník bielenie, štvorec sušenie, žehlička žehlenie a kruh profesionálne čistenie.",
        "keywords": "symboly prania, čo znamená vanička na štítku, čo znamená trojuholník na oblečení, symbol sušičky, symbol žehlenia, pracie značky na oblečení",
        "quick_title": "Rýchla mapa symbolov",
        "quick": [
            "<strong>Vanička:</strong> domáce pranie, teplota a šetrnosť programu.",
            "<strong>Trojuholník:</strong> bielenie alebo zákaz bielenia.",
            "<strong>Štvorec:</strong> sušenie, často aj informácia o sušičke.",
            "<strong>Žehlička:</strong> žehlenie a najvyššia vhodná teplota.",
            "<strong>Kruh:</strong> profesionálne čistenie, nie bežný prací program.",
        ],
        "intro": [
            "Pracie symboly na štítku vyzerajú ako malá technická reč, ale dajú sa naučiť veľmi rýchlo. Nemusíte poznať všetky varianty naspamäť. Stačí chápať päť základných rodín symbolov a vedieť, že čiary, bodky, čísla a preškrtnutie upravujú intenzitu alebo zákaz.",
            "Najčastejšie otázky sú praktické: čo znamená vanička s číslom, čo znamená preškrtnutý trojuholník, či môže ísť oblečenie do sušičky a ako horúco žehliť. Pri každej z týchto otázok platí, že symbol sa týka hotového výrobku. Ak má oblečenie potlač, elastan alebo lepené časti, symbol môže byť prísnejší než samotný materiál.",
            "Dobrá správa je, že symboly vám neberú kontrolu. Naopak, chránia textil pred zbytočným poškodením. Ak je na štítku povolená nižšia teplota, neznamená to slabé pranie. Znamená to, že treba pracovať s programom, dávkovaním, predpraním a sušením premyslene.",
        ],
        "rows": [
            ("Vanička", "pranie vo vode", "číslo je maximálna teplota, čiary často znamenajú šetrnejší proces"),
            ("Ruka vo vaničke", "ručné pranie", "jemne, bez krútenia a agresívneho žmýkania"),
            ("Trojuholník", "bielenie", "preškrtnutie znamená nebieliť"),
            ("Štvorec", "sušenie", "kruh v štvorci sa týka sušičky"),
            ("Žehlička", "žehlenie", "bodky naznačujú teplotu"),
            ("Kruh", "profesionálne čistenie", "písmená sú informácia pre čistiareň"),
        ],
        "workflow": [
            ("Symbol 30 alebo 40", "neprekračujte teplotu, použite vhodný program", "vyššia teplota môže fixovať problém alebo zmeniť tvar"),
            ("Preškrtnutá sušička", "sušiť na vzduchu", "najmä elastan, potlače a jemné úplety"),
            ("Preškrtnutý trojuholník", "nepoužívať bielidlá", "farba a vlákna môžu utrpieť"),
            ("Preškrtnutá žehlička", "nežehliť", "riziko poškodenia povrchu, potlače alebo úpravy"),
        ],
        "mistakes": [
            "Ignorovať symbol sušičky a riešiť iba praciu teplotu.",
            "Brať ruku vo vaničke ako bežný jemný program bez kontroly štítku.",
            "Bieliť bielizeň len preto, že je biela, aj keď trojuholník bielenie zakazuje.",
            "Žehliť potlač priamo, hoci štítok alebo materiál vyžaduje opatrnosť.",
            "Myslieť si, že symbol profesionálneho čistenia znamená domáce pranie.",
        ],
        "expert": "GINETEX uvádza starostlivosť podľa skupín symbolov pre pranie, bielenie, sušenie, žehlenie a profesionálne čistenie. Pri praktickom rozhodovaní je dôležitý princíp najvyššej bezpečnej záťaže. Ak symbol povoľuje určitý postup, jemnejší postup je často možný, ale agresívnejší už môže byť rizikový.",
        "sources": [
            ("GINETEX: Care symbols", "https://www.ginetex.net/gb/labelling/care-symbols.asp"),
            ("FTC: Care Labeling Rule text", "https://www.ftc.gov/legal-library/browse/rules/care-labeling-textile-wearing-apparel-certain-piece-goods-text"),
        ],
        "related": [
            ("Ako prať softshell bundu a nohavice bez poškodenia membrány", "/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany"),
            ("Ako obnoviť impregnáciu softshellu po praní", "/n/ako-obnovit-impregnaciu-softshellu-po-prani-a-kedy-ju-neriesit"),
            ("Ako správne prať obliečky", "/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou"),
        ],
        "faq": [
            ("Čo znamená vanička s číslom?", "Ide o symbol prania vo vode a číslo ukazuje maximálnu teplotu. Ak sú pod symbolom čiary, zvyčajne ide o šetrnejší proces."),
            ("Čo znamená preškrtnutý trojuholník?", "Nepoužívať bielidlá. Platí to aj vtedy, keď je textil biely."),
            ("Čo znamená kruh na štítku?", "Kruh sa týka profesionálneho čistenia. Písmená a podčiarknutia sú hlavne informácia pre čistiareň."),
        ],
    },
    {
        "title": "Prečo sa oblečenie zrazí po praní: teplota, vlákna, sušička a prevencia",
        "short": "Oblečenie sa zrazí najčastejšie pre kombináciu vlákna, konštrukcie látky, tepla, mechaniky a sušenia. Najviac riskujú vlna, bavlnené úplety, viskóza, zmesi a kúsky bez kontroly štítku.",
        "keywords": "prečo sa oblečenie zrazí, zrazené tričko po praní, ako zabrániť zrážaniu oblečenia, zrazilo sa oblečenie v sušičke, zrážanie bavlny, zrážanie vlny",
        "quick_title": "Rýchla odpoveď k zrážaniu",
        "quick": [
            "<strong>Zrážanie nie je iba chyba práčky.</strong> Často ide o kombináciu materiálu, úpletu, tepla a sušičky.",
            "<strong>Najväčší rizikový faktor je teplo.</strong> Horúce pranie a horúca sušička vedia zmeniť tvar viac než jemný program.",
            "<strong>Nové kúsky perte opatrnejšie.</strong> Najmä bavlnené tričká, viskózu, vlnu a zmesi.",
            "<strong>Prevencia je jednoduchšia než záchrana.</strong> Po zrazení už textil často nevrátite do pôvodného stavu úplne.",
        ],
        "intro": [
            "Zrazené tričko, krátke rukávy, menšie pyžamo alebo sveter po praní patria medzi najčastejšie domáce pracie sklamania. Ľudia často vinia práčku, no skutočná príčina býva širšia: materiál, spôsob výroby látky, predúprava, teplota, žmýkanie, sušička a to, či ste rešpektovali štítok.",
            "Ak riešite prečo sa oblečenie zrazí po praní, začnite otázkou, z čoho je vyrobené. Vlna sa správa inak než bavlnený úplet, viskóza inak než polyester a zmes s elastanom inak než pevná tkanina. Dva kúsky s rovnakou farbou môžu potrebovať úplne inú starostlivosť.",
            "Dôležité je aj sušenie. Mnoho textílií zvládne šetrné pranie, ale nezvládne horúcu sušičku alebo radiátor. Pri zrážaní sa preto nepýtajte iba na praciu teplotu, ale aj na otáčky, dĺžku programu a spôsob sušenia.",
        ],
        "rows": [
            ("Vlna", "môže plstnatieť a meniť tvar", "špeciálny program alebo ručné pranie podľa štítku"),
            ("Bavlnený úplet", "môže sa skrátiť pri teple a sušičke", "prať naruby, neprehrievať, sušiť opatrne"),
            ("Viskóza a modal", "mokré vlákno a úplet môžu meniť tvar", "nižšie otáčky a sušenie upravené do tvaru"),
            ("Elastanové zmesi", "strata pružnosti sa tvári ako zmena veľkosti", "chrániť pred teplom a avivážou pri funkčných kusoch"),
        ],
        "workflow": [
            ("Nové oblečenie", "prvé pranie podľa štítku, nižšia teplota", "nový textil môže mať zvyškové napätie z výroby"),
            ("Tričko s potlačou", "naruby, bez horúcej sušičky", "potlač a úplet sú riziková kombinácia"),
            ("Sveter", "program vlna alebo ručné pranie, sušiť naležato", "tvar drží hlavne správne sušenie"),
            ("Obliečky", "nepreplniť bubon, sušiť dôkladne", "zrazenie je menší problém než zatuchnutie, no teplo stále rozhoduje"),
        ],
        "mistakes": [
            "Prať nové tričko hneď s uterákmi na silnom programe.",
            "Dať vlnený sveter do bežnej sušičky.",
            "Ignorovať symbol sušenia a riešiť iba teplotu prania.",
            "Preplniť bubon, takže sa textil trie a horšie oplachuje.",
            "Vešať ťažký mokrý úplet tak, že sa vytiahne a deformuje.",
        ],
        "expert": "Zrážanie je výsledok správania vlákna aj konštrukcie látky. Pri pleteninách a úpletoch zohráva rolu uvoľnenie napätia, pri vlne aj plstnatenie a pri zmesiach najcitlivejšia zložka. Preto je praktické myslieť na celý výrobok a nie iba na dominantný materiál na štítku.",
        "sources": [
            ("GINETEX: Labelling", "https://www.ginetex.net/GB/labelling/labelling.asp"),
            ("FTC: Complying with the Care Labeling Rule", "https://www.ftc.gov/business-guidance/resources/clothes-captioning-complying-care-labeling-rule"),
        ],
        "related": [
            ("Čo je zmesový materiál a prečo sa správa inak", "/n/co-je-zmesovy-material-preco-sa-oblecenie-zraza-alebo-sprava-inak-nez-cakate"),
            ("Ako prať kašmírový sveter doma bez zrazenia a žmolkov", "/n/ako-prat-kasmirovy-sveter-doma-bez-zrazenia-a-zmolkov"),
            ("Čo je bavlna: vlastnosti a starostlivosť", "/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost"),
        ],
        "faq": [
            ("Dá sa zrazené oblečenie zachrániť?", "Niekedy sa dá mierne uvoľniť tvar, ale pôvodný stav nie je zaručený. Pri vlne a teplom poškodenej zmesi môže byť zmena trvalá."),
            ("Zráža viac práčka alebo sušička?", "Veľmi často sušička, najmä horúci režim. Riziko však vzniká kombináciou prania, žmýkania a sušenia."),
            ("Ako zabrániť zrážaniu nového oblečenia?", "Prvé pranie urobte podľa štítku, radšej šetrne, bez horúcej sušičky a bez preplneného bubna."),
        ],
    },
    {
        "title": "Ako prať nové oblečenie prvýkrát: farby, chemický pach, zrážanie a štítok",
        "short": "Nové oblečenie pred prvým nosením alebo po kúpe perte podľa štítku, oddelene od citlivej bielizne a s dôrazom na farbu. Prvé pranie má odstrániť zvyšky z výroby a obchodu bez poškodenia tvaru.",
        "keywords": "ako prať nové oblečenie prvýkrát, treba prať nové oblečenie, nové tričko púšťa farbu, chemický pach z nového oblečenia, prvé pranie džínsov, prvé pranie detského oblečenia",
        "quick_title": "Rýchly postup pri prvom praní",
        "quick": [
            "<strong>Nové oblečenie perte samostatnejšie.</strong> Najmä tmavé, červené, rifľové a výrazne farbené kúsky.",
            "<strong>Začnite štítkom.</strong> Prvé pranie nie je dôvod skúšať silnejší program.",
            "<strong>Pri chemickom pachu nepoužívajte iba vôňu.</strong> Najprv vetrať, vyprať a úplne vysušiť.",
            "<strong>Detské oblečenie opláchnite dôkladne.</strong> Pri citlivej pokožke je dôležitý jemný prostriedok a dobrý oplach.",
        ],
        "intro": [
            "Nové oblečenie prešlo výrobou, balením, skladom, dopravou a predajňou. Preto dáva zmysel riešiť prvé pranie samostatne, najmä pri spodnej bielizni, tričkách, detskom oblečení, posteľnej bielizni a kúskoch, ktoré idú priamo na pokožku. Cieľom nie je oblečenie zničiť silným praním, ale šetrne odstrániť zvyšky z výroby a manipulácie.",
            "Najviac otázok vzniká pri farbách: nové rifle púšťajú farbu, čierne tričko farbí, červené ponožky ohrozia bielu bielizeň. Pri prvom praní preto netriedite len na biele a farebné. Rizikové farby perte oddelene alebo s podobnými odtieňmi a podľa štítku.",
            "Chemický pach z nového oblečenia tiež netreba prekryť parfumom do prania. Najprv textil vyvetrajte, vyperte primeranou dávkou pracieho gélu a dôkladne vysušte. Vôňa má dopĺňať čistotu, nie maskovať zvyšky výroby, skladovania alebo vlhkosti.",
        ],
        "rows": [
            ("Tmavé rifle", "môžu púšťať farbu", "prať naruby a s podobnými tmavými kusmi"),
            ("Červené a sýte farby", "vyššie riziko zafarbenia", "prvé pranie oddeliť od svetlej bielizne"),
            ("Detské oblečenie", "kontakt s citlivou pokožkou", "jemný prostriedok, dôkladný oplach, úplné sušenie"),
            ("Textil s pachom", "zvyšky výroby, balenia alebo skladu", "vetrať, prať, sušiť, až potom voliť vôňu"),
        ],
        "workflow": [
            ("Pred praním", "prečítať štítok, zapnúť zipsy, otočiť naruby", "chráni farbu, potlač a tvar"),
            ("Prvá dávka", "oddeliť výrazné farby a rizikové materiály", "zníži riziko zafarbenia ostatnej bielizne"),
            ("Dávkovanie", "primeraná dávka gélu, nie dvojnásobok", "zvyšky prostriedku zhoršujú pocit na pokožke"),
            ("Sušenie", "úplne vysušiť pred uložením", "vlhký nový textil rýchlo zatuchne"),
        ],
        "mistakes": [
            "Pridať nové tmavé tričko k bielej alebo svetlej bielizni.",
            "Prekryť chemický pach silnou vôňou bez prania a vetrania.",
            "Prať nové oblečenie horúco iba pre pocit hygieny.",
            "Ignorovať zipsy, háčiky a potlač pri prvom praní.",
            "Uložiť nové oblečenie do skrine ešte mierne vlhké.",
        ],
        "expert": "Prvé pranie je kombinácia hygieny, ochrany farby a stabilizácie starostlivosti. Oficiálne pravidlá označovania zdôrazňujú, že štítok má poskytnúť postup pre pravidelnú starostlivosť. Pri novom oblečení však navyše riešite zvyšky z výroby, skladovania a možnú nestálosť farby, preto je opatrné triedenie ešte dôležitejšie.",
        "sources": [
            ("FTC: Complying with the Care Labeling Rule", "https://www.ftc.gov/business-guidance/resources/clothes-captioning-complying-care-labeling-rule"),
            ("GINETEX: Care symbols", "https://www.ginetex.net/gb/labelling/care-symbols.asp"),
        ],
        "related": [
            ("Ako zabrániť púšťaniu farby pri praní nového oblečenia", "/n/ako-zabranit-pustaniu-farby-pri-prani-noveho-oblecenia"),
            ("Pustila farba v práčke: čo urobiť s bielym tričkom", "/n/pustila-farba-v-pracke-co-urobit-s-bielym-trickom-a-ruzovou-bieliznou"),
            ("Ako prať rifľovú bundu a tmavé džínsy", "/n/ako-prat-riflovu-bundu-a-tmave-dzinsy-aby-nepustali-farbu"),
        ],
        "faq": [
            ("Treba prať nové oblečenie pred nosením?", "Pri spodnej bielizni, detskom oblečení, tričkách a veciach priamo na pokožku je to rozumné. Perte podľa štítku a šetrne."),
            ("Čo robiť, keď nové oblečenie páchne chemicky?", "Najprv vetrať, potom vyprať podľa štítku a úplne vysušiť. Vôňu pridajte až po vyčistení."),
            ("Ako prať nové rifle prvýkrát?", "Naruby, oddelene alebo s tmavými farbami, podľa štítku a bez zbytočne horúceho programu."),
        ],
    },
    {
        "title": "Certifikáty na textile: OEKO-TEX, GOTS, recyklované vlákna a čo znamenajú pri praní",
        "short": "Textilné certifikáty hovoria najmä o bezpečnosti, pôvode alebo spracovaní materiálu. Pri domácom praní však stále rozhoduje štítok, farba, zmes, potlač a konštrukcia výrobku.",
        "keywords": "textilné certifikáty, čo znamená OEKO-TEX, čo znamená GOTS, certifikovaná bavlna pranie, recyklovaný polyester pranie, certifikát na oblečení",
        "quick_title": "Rýchla odpoveď bez marketingu",
        "quick": [
            "<strong>Certifikát nenahrádza prací štítok.</strong> Hovorí o bezpečnosti, pôvode alebo procese, nie o tom, že môžete ignorovať symboly prania.",
            "<strong>OEKO-TEX STANDARD 100</strong> sa týka testovania na škodlivé látky v textile.",
            "<strong>GOTS a organická bavlna</strong> riešia organický pôvod a spracovanie, ale bavlna sa stále perie podľa hotového výrobku.",
            "<strong>Recyklované vlákno</strong> nemení automaticky domácu starostlivosť. Recyklovaný polyester sa stále správa ako polyester.",
        ],
        "intro": [
            "Na oblečení, uterákoch, obliečkach a detskom textile nájdete množstvo označení: OEKO-TEX, GOTS, organic cotton, recycled polyester, rPET, Made in Green a ďalšie. Pre zákazníka sú užitočné, ale ľahko vznikne nesprávny záver, že certifikát automaticky znamená špeciálne pranie alebo naopak úplnú odolnosť.",
            "Pri domácom praní je dôležité oddeliť dve veci: čo certifikát hovorí o materiáli alebo výrobe a čo hovorí štítok o starostlivosti. Certifikát môže byť dôležitý pri výbere produktu, najmä pri citlivej pokožke alebo preferencii pôvodu materiálu. Prací program však stále vyberáte podľa vlákna, farby, potlače, zmesi, gumičiek, zipsov a sušenia.",
            "Ak hľadáte čo znamená OEKO-TEX, GOTS alebo recyklovaný polyester pri praní, praktická odpoveď znie: berte ich ako informáciu pri nákupe, nie ako náhradu symbolov starostlivosti. Aj certifikované tričko sa môže zraziť, púšťať farbu alebo stratiť tvar, ak ho vyperiete nevhodne.",
        ],
        "rows": [
            ("OEKO-TEX STANDARD 100", "testovanie textilu na škodlivé látky", "neznamená automaticky vyššiu teplotu prania"),
            ("GOTS / organická bavlna", "organický pôvod a spracovanie v reťazci", "bavlnený výrobok stále riešte podľa štítku"),
            ("Recyklovaný polyester", "pôvod vlákna alebo materiálu", "perie sa ako polyester podľa konkrétneho výrobku"),
            ("Made in Green a podobné štítky", "širší kontext výroby a sledovateľnosti", "domáce pranie stále určuje care label"),
        ],
        "workflow": [
            ("Detské body s certifikátom", "jemný gél, dôkladný oplach, úplné sušenie", "citlivú pokožku dráždia aj zvyšky pracieho prostriedku"),
            ("Organické bavlnené tričko", "naruby, s podobnými farbami, podľa štítku", "farba a potlač sú praktické riziko"),
            ("Recyklovaná polyesterová mikina", "riešiť pot, pach a rýchle sušenie", "vlákno sa správa podobne ako bežná syntetika"),
            ("Certifikované obliečky", "nepreplniť bubon a dosušiť do sucha", "komfort ničí aj vlhkosť a zvyšky prostriedku"),
        ],
        "mistakes": [
            "Myslieť si, že OEKO-TEX znamená, že textil môžete prať akokoľvek.",
            "Prať organickú bavlnu agresívne len preto, že je kvalitná.",
            "Ignorovať farbu a potlač pri certifikovanom tričku.",
            "Prekryť zápach parfumom bez vyriešenia potu, vlhkosti alebo zlého oplachu.",
            "Zamieňať recyklovaný pôvod vlákna s odolnosťou voči teplu.",
        ],
        "expert": "Textilné certifikácie majú význam pri transparentnosti, bezpečnosti alebo pôvode materiálu. Z pohľadu domácej starostlivosti však pracujete s hotovým výrobkom. Preto vždy kombinujte informáciu z certifikátu s pracím štítkom a praktickým použitím textilu. Iné nároky má detské body, iné športová syntetika a iné posteľná bielizeň.",
        "sources": [
            ("OEKO-TEX STANDARD 100", "https://www.oeko-tex.com/en/our-standards/oeko-tex-standard-100/"),
            ("Textile Exchange: Organic Cotton Certification", "https://textileexchange.org/organic-cotton-certification/"),
            ("Textile Exchange: Materials Market Report", "https://textileexchange.org/knowledge-center/reports/materials-market-report-2024/"),
        ],
        "related": [
            ("Organická bavlna: čo znamená a či sa perie inak", "/n/organicka-bavlna-co-znamena-a-ci-sa-perie-inak-ako-bezna-bavlna"),
            ("Recyklovaný polyester: čo znamená a ako sa oň starať", "/n/recyklovany-polyester-co-znamena-ake-ma-vyhody-a-ako-sa-on-starat"),
            ("Čo je polyester a ako ho prať, aby nezapáchal", "/n/co-je-polyester-a-ako-ho-prat-aby-nezapachal"),
        ],
        "faq": [
            ("Znamená OEKO-TEX, že textil je vhodný pre citlivú pokožku?", "Je to užitočný signál testovania na škodlivé látky, ale pri citlivej pokožke rozhoduje aj prací prostriedok, oplach a konkrétna reakcia človeka."),
            ("Perie sa GOTS bavlna inak?", "Nie automaticky. GOTS súvisí s organickým textilným štandardom, ale domáci postup určuje štítok hotového výrobku."),
            ("Je recyklovaný polyester jemnejší na pranie?", "Nie nevyhnutne. Recyklovaný pôvod nemení fakt, že ide o polyesterový materiál alebo zmes."),
        ],
    },
]


def build_long(article):
    html = [
        f"<p><strong>Rýchla odpoveď:</strong> {article['short']}</p>",
        f"<p>Okrem hlavnej odpovede rozoberáme aj praktické situácie z domácnosti: <strong>{article['keywords']}</strong>. Praktická pointa je jednoduchá: štítok, materiál, farba, konštrukcia a sušenie treba čítať spolu, nie oddelene.</p>",
        helpers.quick_box(article["quick_title"], article["quick"]),
    ]
    html.extend(f"<p>{paragraph}</p>" for paragraph in article["intro"])
    html.append("<h2>Prehľad v praxi</h2>")
    html.append(helpers.table(["Situácia", "Čo znamená", "Ako postupovať"], article["rows"]))
    html.append("<h2>Praktický postup podľa typu textilu</h2>")
    html.append("<p>Ak si nie ste istí, zvoľte najšetrnejší postup, ktorý ešte dáva zmysel pre mieru znečistenia. Pri pote, biologických škvrnách alebo zatuchnutí najprv riešte príčinu, nie silnejšiu vôňu.</p>")
    html.append(helpers.table(["Textil alebo situácia", "Postup", "Prečo"], article["workflow"]))
    html.append("<h2>Najčastejšie chyby</h2>")
    html.append("<ul>" + "".join(f"<li>{item}</li>" for item in article["mistakes"]) + "</ul>")
    html.append("<h2>Odbornejší pohľad</h2>")
    html.append(f"<p>{article['expert']}</p>")
    html.append(helpers.sources(article["sources"]))
    html.append(helpers.recommendation())
    html.append(helpers.related(article["related"]))
    html.append("<h2>FAQ</h2>")
    for question, answer in article["faq"]:
        html.append(f"<h3>{question}</h3><p>{answer}</p>")
    return "\n".join(html)


def main():
    articles = []
    times = ["08:00:00", "08:12:00", "08:24:00", "08:36:00", "08:48:00"]
    for index, article in enumerate(ARTICLES):
        long_html = build_long(article)
        if re.search(r"\bCTA\b", long_html):
            raise SystemExit(f"Forbidden customer-facing CTA wording in {article['title']}")
        if "Cena:" in long_html or re.search(r"\d+(?:[,.]\d{2})?\s*\u20ac", long_html):
            raise SystemExit(f"Fixed price wording in {article['title']}")
        if len(long_html) > 32700:
            raise SystemExit(f"XLS cell too long for {article['title']}: {len(long_html)}")
        articles.append(
            {
                "title": article["title"],
                "short": article["short"],
                "long": long_html,
                "date_posted": BATCH_DATE,
                "time_posted": times[index],
                "active": 1,
                "link": helpers.slugify(article["title"]),
                "commenting": "none",
            }
        )

    hrefs = sorted({href for row in articles for href in re.findall(r'href="([^"]+)"', row["long"])})
    checks = []
    for href in hrefs:
        if href.startswith("/"):
            url = urljoin(BASE, href)
        elif href.startswith("http"):
            url = href
        else:
            continue
        response = requests.get(url, timeout=30, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
        checks.append((href, response.status_code, response.url))
        if response.status_code != 200:
            raise SystemExit(f"Link preflight failed: {href} -> {response.status_code} {response.url}")

    OUT_JSON.write_text(json.dumps(articles, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    book = xlwt.Workbook(encoding="utf-8")
    sheet = book.add_sheet("news")
    headers = ["title", "short", "long", "date_posted", "time_posted", "active", "link", "commenting"]
    for col, header in enumerate(headers):
        sheet.write(0, col, header)
    for row_index, article in enumerate(articles, start=1):
        for col, header in enumerate(headers):
            sheet.write(row_index, col, article[header])
    OUT_XLS.parent.mkdir(parents=True, exist_ok=True)
    book.save(str(OUT_XLS))

    print(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "article_count": len(articles),
                "json": str(OUT_JSON),
                "xls": str(OUT_XLS),
                "links_checked": len(checks),
                "slugs": [article["link"] for article in articles],
                "lengths": [len(article["long"]) for article in articles],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
